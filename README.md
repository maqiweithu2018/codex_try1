# A 股多策略筛选脚本（AKShare）

本项目提供一个可直接运行的 Python 脚本，用于批量下载/复用 A 股历史数据，并按可切换策略筛选股票，为后续研究或评价做准备。

脚本文件：`stock_screener_akshare.py`

## 功能概览

- 拉取全部 A 股代码列表
- 下载个股日线数据（支持前复权/后复权）
- 支持本地缓存优先读取，减少重复联网请求
- 兼容旧版本地数据结构（中文列名与中英混合列名）
- 支持两种可切换策略：
  - `ma120`：价格相对 MA120 折价 + PE 过滤
  - `weekly_chip_breakout`：周线筹码集中 + 2~3 次试探后突破
- 输出筛选结果 CSV，方便后续回测或主观复盘

## 目录结构

```text
.
├── stock_screener_akshare.py   # 主脚本
├── data/
│   └── daily/                  # 个股日线缓存（每个股票一个 CSV）
├── output/
│   └── screen_result.csv       # 默认筛选结果
└── README.md
```

## 环境要求

- Python 3.9+
- 可联网访问 AKShare 数据源

安装依赖：

```bash
pip install akshare pandas
```

## 快速开始

在项目目录运行：

```bash
python3 stock_screener_akshare.py
```

默认行为：

- 策略：`auto`（若无自然语言指令，默认回落到 `ma120`）
- 起始日期：`20100101`
- 结束日期：当天
- 复权方式：`qfq`
- 结果文件：`output/screen_result.csv`

## 策略说明

### 1) MA120 策略（`ma120`）

筛选逻辑：

- `latest_price < ma120 * 0.88`
- `0 < pe_dynamic < max_pe`（默认 `max_pe=20`）

输出核心字段：

- `symbol`, `name`
- `latest_date`, `latest_price`
- `ma120`, `price_ma120_ratio`
- `pe_dynamic`

### 2) 周线筹码突破策略（`weekly_chip_breakout`）

策略核心（启发式实现）：

- 由近一年周线（排除最近几周）构建“筹码集中区”（按成交量加权分位）
- 要求集中区“足够窄”且区间内成交量占比达到阈值
- 最近若干周出现 `2~3` 次对集中区上沿的试探
- 最新周确认向上突破集中区上沿

输出核心字段：

- `symbol`, `name`
- `latest_week`, `latest_close`
- `chip_zone_low`, `chip_zone_high`
- `zone_width_ratio`, `volume_concentration`
- `attempts`, `breakout_strength`

## 策略切换方式

### 方式 A：显式指定策略

```bash
# 使用 MA120 策略
python3 stock_screener_akshare.py --strategy ma120

# 使用周线筹码突破策略
python3 stock_screener_akshare.py --strategy weekly_chip_breakout
```

### 方式 B：自然语言自动识别（`--strategy auto`）

```bash
# 自动切换到 MA120
python3 stock_screener_akshare.py --strategy auto --instruction "请使用MA120策略筛选股票"

# 自动切换到周线筹码突破
python3 stock_screener_akshare.py --strategy auto --instruction "请使用周线筹码突破策略筛选股票"
```

识别规则简述：

- 包含 `ma120` / `均线` -> `ma120`
- 包含 `筹码` / `周线` / `突破` -> `weekly_chip_breakout`
- 其余情况默认 `ma120`

## 常用参数

```bash
python3 stock_screener_akshare.py \
  --start-date 20200101 \
  --end-date 20260218 \
  --adjust qfq \
  --workers 12 \
  --retry 2 \
  --sleep 0.15 \
  --max-pe 20 \
  --result-file output/screen_result.csv
```

参数说明：

- `--start-date`：历史起始日期（`YYYYMMDD`）
- `--end-date`：历史结束日期，留空表示当天
- `--adjust`：复权方式，`"" | qfq | hfq`
- `--workers`：并发下载线程数
- `--retry` / `--sleep`：失败重试次数与重试间隔
- `--strategy`：`auto | ma120 | weekly_chip_breakout`
- `--instruction`：自动识别策略用自然语言文本
- `--max-pe`：仅 MA120 策略生效
- `--data-dir`：日线缓存目录
- `--result-file`：筛选结果文件路径

## 本地缓存与旧数据兼容

默认开启本地缓存优先：

- 若 `data/daily/<symbol>.csv` 已存在，优先读取本地
- 本地数据不可用时才向 AKShare 请求

旧数据兼容点：

- 支持旧列名：`日期/开盘/最高/最低/收盘/成交量`
- 支持中英混合列名
- 脚本会统一保存为标准列：`date/open/high/low/close/volume`

缓存控制：

```bash
# 默认：优先使用本地缓存
python3 stock_screener_akshare.py --use-local-cache

# 强制全量联网刷新（覆盖本地）
python3 stock_screener_akshare.py --no-local-cache
```

## 输出说明

- 个股历史数据：`data/daily/*.csv`
- 筛选结果：`output/screen_result.csv`（或你指定的 `--result-file`）

脚本执行完成后会打印：

- 处理进度（按股票计数）
- 命中数量
- 结果文件绝对路径

## 常见问题（FAQ）

### 1) 结果为空怎么办？

- 周线筹码突破策略较严格，空结果是常见情况
- 可尝试：
  - 扩大样本区间（更早的 `--start-date`）
  - 使用 `--no-local-cache` 刷新数据
  - 适度调整策略阈值（需要改代码参数）

### 2) 运行很慢怎么办？

- 提高 `--workers`（例如 12~20）
- 首次全量下载后，后续使用本地缓存会快很多

### 3) 网络抖动导致失败怎么办？

- 增加 `--retry`
- 适当增大 `--sleep`

## 风险提示

本项目仅用于量化研究与学习，不构成任何投资建议。策略命中不代表未来收益，请结合基本面、风险控制和回测结果独立判断。

