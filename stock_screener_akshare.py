#!/usr/bin/env python3
"""Download all A-share daily data with AKShare and screen by selectable strategies.

Strategies:
1) ma120: current_price < ma120 * 0.88 and 0 < pe_dynamic < max_pe
2) weekly_chip_breakout: weekly chip concentration with 2-3 tests, then breakout above zone high
"""

from __future__ import annotations

import argparse
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import akshare as ak
import pandas as pd

STRATEGY_MA120 = "ma120"
STRATEGY_WEEKLY_CHIP_BREAKOUT = "weekly_chip_breakout"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A-share downloader + multi-strategy screener")
    parser.add_argument("--start-date", default="20100101", help="history start date, e.g. 20100101")
    parser.add_argument("--end-date", default="", help="history end date, e.g. 20260217; empty means latest")
    parser.add_argument("--adjust", default="qfq", choices=["", "qfq", "hfq"], help="price adjustment")
    parser.add_argument("--workers", type=int, default=8, help="thread count for downloading histories")
    parser.add_argument("--retry", type=int, default=2, help="retries per stock when request fails")
    parser.add_argument("--sleep", type=float, default=0.15, help="sleep seconds between retries")
    parser.add_argument("--data-dir", default="data/daily", help="folder to save daily CSV files")
    parser.add_argument(
        "--result-file",
        default="output/screen_result.csv",
        help="output CSV for filtered stocks",
    )
    parser.add_argument(
        "--strategy",
        default="auto",
        choices=["auto", STRATEGY_MA120, STRATEGY_WEEKLY_CHIP_BREAKOUT],
        help="screening strategy",
    )
    parser.add_argument(
        "--instruction",
        default="",
        help="natural-language instruction, e.g. '请使用MA120策略筛选股票'",
    )
    parser.add_argument("--max-pe", type=float, default=20.0, help="PE upper bound for MA120 strategy")
    parser.add_argument(
        "--use-local-cache",
        dest="use_local_cache",
        action="store_true",
        default=True,
        help="prefer reading existing CSV files in --data-dir before requesting from AKShare",
    )
    parser.add_argument(
        "--no-local-cache",
        dest="use_local_cache",
        action="store_false",
        help="always request latest data from AKShare and overwrite local CSV files",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def resolve_strategy(strategy: str, instruction: str) -> str:
    if strategy != "auto":
        return strategy

    text = (instruction or "").strip().lower()
    if "ma120" in text or "均线" in text:
        return STRATEGY_MA120
    if "筹码" in text or "周线" in text or "突破" in text:
        return STRATEGY_WEEKLY_CHIP_BREAKOUT
    return STRATEGY_MA120


def get_a_share_list() -> pd.DataFrame:
    df = ak.stock_info_a_code_name()
    df = df.rename(columns={"code": "symbol", "name": "name"})[["symbol", "name"]]
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    return df


def get_spot_pe_map() -> Dict[str, float]:
    spot = ak.stock_zh_a_spot_em()
    spot = spot.rename(columns={"代码": "symbol", "市盈率-动态": "pe_dynamic"})
    spot = spot[["symbol", "pe_dynamic"]].copy()
    spot["symbol"] = spot["symbol"].astype(str).str.zfill(6)
    spot["pe_dynamic"] = pd.to_numeric(spot["pe_dynamic"], errors="coerce")
    return dict(zip(spot["symbol"], spot["pe_dynamic"]))


def weighted_quantile(values: pd.Series, weights: pd.Series, q: float) -> float:
    temp = pd.DataFrame({"v": values, "w": weights}).dropna()
    temp = temp[temp["w"] > 0].sort_values("v")
    if temp.empty:
        return float("nan")
    cdf = temp["w"].cumsum() / temp["w"].sum()
    idx = cdf.searchsorted(q, side="left")
    idx = min(int(idx), len(temp) - 1)
    return float(temp.iloc[idx]["v"])


def normalize_daily_df(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    # Compatible with both new format (open/high/low/close/volume) and old mixed format.
    work = df.rename(
        columns={
            "日期": "date",
            "收盘": "close",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
        }
    ).copy()
    required = {"date", "close", "high", "low", "volume"}
    if not required.issubset(work.columns):
        return None
    if "open" not in work.columns:
        work["open"] = work["close"]

    keep_cols = ["date", "open", "high", "low", "close", "volume"]
    work = work[keep_cols].copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    for c in ["open", "high", "low", "close", "volume"]:
        work[c] = pd.to_numeric(work[c], errors="coerce")
    work = work.dropna(subset=keep_cols).sort_values("date")
    if work.empty:
        return None
    return work


def load_local_daily_df(csv_path: Path) -> Optional[pd.DataFrame]:
    try:
        raw = pd.read_csv(csv_path)
    except Exception:
        return None
    return normalize_daily_df(raw)


def fetch_daily_df(
    symbol: str,
    start_date: str,
    end_date: str,
    adjust: str,
    retry: int,
    sleep_sec: float,
) -> Optional[pd.DataFrame]:
    for attempt in range(1, retry + 2):
        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )
            if df is None or df.empty:
                return None

            df = normalize_daily_df(df)
            if df is None:
                return None
            return df
        except Exception:
            if attempt <= retry:
                time.sleep(sleep_sec)
                continue
            return None
    return None


def evaluate_ma120(df: pd.DataFrame, pe_dynamic: float, max_pe: float) -> Optional[Dict[str, float]]:
    work = df.copy()
    work["ma120"] = work["close"].rolling(120).mean()
    latest = work.iloc[-1]
    ma120 = latest["ma120"]
    latest_price = latest["close"]
    if pd.isna(ma120) or pd.isna(latest_price):
        return None

    if not (pd.notna(pe_dynamic) and pe_dynamic > 0 and pe_dynamic < max_pe):
        return None
    if not (latest_price < ma120 * 0.88):
        return None

    return {
        "latest_date": latest["date"].date().isoformat(),
        "latest_price": float(latest_price),
        "ma120": float(ma120),
        "price_ma120_ratio": float(latest_price / ma120),
        "pe_dynamic": float(pe_dynamic),
    }


def evaluate_weekly_chip_breakout(df: pd.DataFrame) -> Optional[Dict[str, float]]:
    if "volume" not in df.columns:
        return None

    weekly = (
        df.set_index("date")
        .resample("W-FRI")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["close", "high", "low", "volume"])
    )
    if len(weekly) < 60:
        return None

    # Build chip concentration zone from historical weekly close-volume profile.
    base = weekly.iloc[-52:-4].copy()
    if len(base) < 30:
        return None

    zone_low = weighted_quantile(base["close"], base["volume"], 0.35)
    zone_high = weighted_quantile(base["close"], base["volume"], 0.65)
    if pd.isna(zone_low) or pd.isna(zone_high) or zone_high <= zone_low:
        return None

    mean_price = float(base["close"].mean())
    zone_width_ratio = (zone_high - zone_low) / mean_price if mean_price > 0 else 999
    in_zone_volume = base.loc[(base["close"] >= zone_low) & (base["close"] <= zone_high), "volume"].sum()
    volume_concentration = in_zone_volume / base["volume"].sum() if base["volume"].sum() > 0 else 0

    # Heuristic for "筹码集中": narrow enough + enough volume concentrated in zone.
    if not (zone_width_ratio <= 0.18 and volume_concentration >= 0.40):
        return None

    pre_break = weekly.iloc[-14:-1].copy()
    if len(pre_break) < 8:
        return None

    touch_tol = 0.02
    attempt_mask = (pre_break["high"] >= zone_high * (1 - touch_tol)) & (pre_break["close"] <= zone_high * 1.01)
    attempts = int(attempt_mask.sum())
    if not (2 <= attempts <= 3):
        return None

    latest = weekly.iloc[-1]
    prev = weekly.iloc[-2]
    breakout = latest["close"] > zone_high * 1.01 and latest["high"] > zone_high and prev["close"] <= zone_high * 1.01
    if not breakout:
        return None

    return {
        "latest_week": weekly.index[-1].date().isoformat(),
        "latest_close": float(latest["close"]),
        "chip_zone_low": float(zone_low),
        "chip_zone_high": float(zone_high),
        "zone_width_ratio": float(zone_width_ratio),
        "volume_concentration": float(volume_concentration),
        "attempts": attempts,
        "breakout_strength": float(latest["close"] / zone_high),
    }


def process_one(
    symbol: str,
    name: str,
    strategy: str,
    start_date: str,
    end_date: str,
    adjust: str,
    data_dir: Path,
    retry: int,
    sleep_sec: float,
    pe_dynamic: float,
    max_pe: float,
    use_local_cache: bool,
) -> Optional[Dict[str, float]]:
    out_file = data_dir / f"{symbol}.csv"
    df: Optional[pd.DataFrame] = None

    if use_local_cache and out_file.exists():
        df = load_local_daily_df(out_file)

    if df is None or df.empty:
        df = fetch_daily_df(symbol, start_date, end_date, adjust, retry, sleep_sec)

    if df is None or df.empty:
        return None

    # Always persist normalized data format for future compatibility.
    df.to_csv(out_file, index=False, encoding="utf-8-sig")

    if strategy == STRATEGY_MA120:
        details = evaluate_ma120(df, pe_dynamic=pe_dynamic, max_pe=max_pe)
    else:
        details = evaluate_weekly_chip_breakout(df)

    if not details:
        return None

    result = {"symbol": symbol, "name": name, "strategy": strategy}
    result.update(details)
    if pd.notna(pe_dynamic):
        result["pe_dynamic"] = float(pe_dynamic)
    return result


def main() -> None:
    socket.setdefaulttimeout(15)
    args = parse_args()

    strategy = resolve_strategy(args.strategy, args.instruction)
    end_date = args.end_date.strip() or datetime.now().strftime("%Y%m%d")

    data_dir = Path(args.data_dir)
    result_file = Path(args.result_file)
    ensure_dir(data_dir)
    ensure_dir(result_file.parent)

    print("[1/4] 拉取 A 股列表...")
    stock_list = get_a_share_list()
    print(f"A 股数量: {len(stock_list)}")
    print(f"使用策略: {strategy}")

    pe_map: Dict[str, float] = {}
    if strategy == STRATEGY_MA120:
        print("[2/4] 拉取实时 PE(动态)...")
        pe_map = get_spot_pe_map()
    else:
        print("[2/4] 周线策略不依赖 PE，跳过 PE 拉取。")

    print("[3/4] 下载日线并执行策略计算...")
    rows: List[Dict[str, float]] = []
    total = len(stock_list)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                process_one,
                row.symbol,
                row.name,
                strategy,
                args.start_date,
                end_date,
                args.adjust,
                data_dir,
                args.retry,
                args.sleep,
                pe_map.get(row.symbol, float("nan")),
                args.max_pe,
                args.use_local_cache,
            )
            for row in stock_list.itertuples(index=False)
        ]

        done = 0
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                rows.append(result)

            if done % 100 == 0 or done == total:
                print(f"  进度: {done}/{total}")

    if not rows:
        print("没有股票满足当前策略，请放宽参数或检查数据状态。")
        return

    screened = pd.DataFrame(rows)

    if strategy == STRATEGY_MA120:
        screened = screened.sort_values(["price_ma120_ratio", "pe_dynamic"], ascending=[True, True])
    else:
        screened = screened.sort_values(["breakout_strength", "zone_width_ratio"], ascending=[False, True])

    screened.to_csv(result_file, index=False, encoding="utf-8-sig")

    print(f"[4/4] 筛选完成: {len(screened)} 只")
    print(f"筛选文件: {result_file.resolve()}")
    print(f"日线目录: {data_dir.resolve()}")


if __name__ == "__main__":
    main()
