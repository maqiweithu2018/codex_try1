#!/usr/bin/env python3
"""Download all A-share daily data with AKShare and screen by MA120/PE.

Screening rules:
1) current_price < ma120 * 0.88
2) pe_dynamic < 20
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A-share daily downloader + MA120/PE screener")
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
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def get_a_share_list() -> pd.DataFrame:
    df = ak.stock_info_a_code_name()
    df = df.rename(columns={"code": "symbol", "name": "name"})[["symbol", "name"]]
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    return df


def get_spot_pe_map() -> Dict[str, float]:
    """Get current PE(dynamic) for all A-shares from spot data."""
    spot = ak.stock_zh_a_spot_em()
    spot = spot.rename(columns={"代码": "symbol", "市盈率-动态": "pe_dynamic"})
    spot = spot[["symbol", "pe_dynamic"]].copy()
    spot["symbol"] = spot["symbol"].astype(str).str.zfill(6)
    spot["pe_dynamic"] = pd.to_numeric(spot["pe_dynamic"], errors="coerce")
    return dict(zip(spot["symbol"], spot["pe_dynamic"]))


def download_one(
    symbol: str,
    name: str,
    start_date: str,
    end_date: str,
    adjust: str,
    data_dir: Path,
    retry: int,
    sleep_sec: float,
) -> Optional[Dict[str, float]]:
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

            # Standardize and compute MA120.
            df = df.rename(columns={"日期": "date", "收盘": "close"})
            df["date"] = pd.to_datetime(df["date"])
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df = df.dropna(subset=["close"]).sort_values("date")
            if df.empty:
                return None

            df["ma120"] = df["close"].rolling(120).mean()
            latest = df.iloc[-1]
            ma120 = latest["ma120"]
            latest_price = latest["close"]
            if pd.isna(ma120):
                return None

            # Save per-symbol daily data.
            out_file = data_dir / f"{symbol}.csv"
            df.to_csv(out_file, index=False, encoding="utf-8-sig")

            return {
                "symbol": symbol,
                "name": name,
                "latest_price": float(latest_price),
                "ma120": float(ma120),
                "price_ma120_ratio": float(latest_price / ma120),
            }
        except Exception:
            if attempt <= retry:
                time.sleep(sleep_sec)
                continue
            return None
    return None


def main() -> None:
    # Prevent hanging forever on unstable network calls inside AKShare.
    socket.setdefaulttimeout(15)
    args = parse_args()
    end_date = args.end_date.strip() or datetime.now().strftime("%Y%m%d")

    data_dir = Path(args.data_dir)
    result_file = Path(args.result_file)
    ensure_dir(data_dir)
    ensure_dir(result_file.parent)

    print("[1/4] 拉取 A 股列表...")
    stock_list = get_a_share_list()
    print(f"A 股数量: {len(stock_list)}")

    print("[2/4] 拉取实时 PE(动态)...")
    pe_map = get_spot_pe_map()

    print("[3/4] 下载日线并计算 MA120...")
    rows: List[Dict[str, float]] = []
    total = len(stock_list)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                download_one,
                row.symbol,
                row.name,
                args.start_date,
                end_date,
                args.adjust,
                data_dir,
                args.retry,
                args.sleep,
            )
            for row in stock_list.itertuples(index=False)
        ]

        done = 0
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                pe = pe_map.get(result["symbol"])
                result["pe_dynamic"] = float(pe) if pd.notna(pe) else float("nan")
                rows.append(result)

            if done % 100 == 0 or done == total:
                print(f"  进度: {done}/{total}")

    if not rows:
        print("没有成功下载到有效数据，请检查网络或 AKShare 接口状态。")
        return

    df_all = pd.DataFrame(rows)
    df_all["pe_dynamic"] = pd.to_numeric(df_all["pe_dynamic"], errors="coerce")

    print("[4/4] 按条件筛选...")
    screened = df_all[
        (df_all["latest_price"] < df_all["ma120"] * 0.88)
        & (df_all["pe_dynamic"] > 0)
        & (df_all["pe_dynamic"] < 20)
    ].copy()
    screened = screened.sort_values(["price_ma120_ratio", "pe_dynamic"], ascending=[True, True])

    screened.to_csv(result_file, index=False, encoding="utf-8-sig")

    print(f"下载完成，成功处理: {len(df_all)} 只")
    print(f"筛选结果数量: {len(screened)} 只")
    print(f"筛选文件: {result_file.resolve()}")
    print(f"日线目录: {data_dir.resolve()}")


if __name__ == "__main__":
    main()
