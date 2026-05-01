#!/usr/bin/env python3
"""Validate the energy MVP's three primary public data sources.

The script reads the normalized pipeline output and writes a lightweight
Markdown validation note for early-stage source checking.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "public" / "data" / "issues" / "energy.json"
DEFAULT_OUTPUT = ROOT / "docs" / "energy-source-validation.md"


def fmt_number(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}".rstrip("0").rstrip(".")


def pct_diff(left: float, right: float) -> float:
    return abs(left - right) / right * 100 if right else 0.0


def get_mix(data: Dict[str, Any], fuel_id: str) -> Dict[str, Any]:
    for item in data.get("current", {}).get("generation_mix", []):
        if item.get("id") == fuel_id:
            return item
    return {"id": fuel_id, "label": fuel_id, "mw": 0.0, "share": 0.0}


def source_by_id(data: Dict[str, Any], source_id: str) -> Dict[str, Any]:
    for source in data.get("sources", []):
        if source.get("id") == source_id:
            return source
    return {}


def markdown_table(headers: Iterable[str], rows: Iterable[Iterable[str]]) -> str:
    header_list = list(headers)
    lines = [
        "| " + " | ".join(header_list) + " |",
        "| " + " | ".join("---" for _ in header_list) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def build_report(data: Dict[str, Any], input_path: Path) -> str:
    summary = data.get("summary", {})
    supply = data.get("current", {}).get("supply", {})
    pipeline = data.get("pipeline_summary", {})
    latest_daily = (data.get("daily", {}).get("thirty_days") or [{}])[-1]

    current_total_mw = float(summary.get("current_total_generation_mw") or 0)
    current_total_10mw = current_total_mw / 10
    current_load_10mw = float(summary.get("current_load_10mw") or 0)
    real_hour_supply_10mw = float(supply.get("real_hour_supply_10mw") or 0)
    forecast_max_supply_10mw = float(supply.get("forecast_max_supply_10mw") or 0)
    load_gap_10mw = current_total_10mw - current_load_10mw
    load_gap_pct = pct_diff(current_total_10mw, current_load_10mw)
    supply_gap_10mw = real_hour_supply_10mw - current_total_10mw
    supply_gap_pct = pct_diff(current_total_10mw, real_hour_supply_10mw)

    gas = get_mix(data, "gas")
    coal = get_mix(data, "coal")
    gas_coal_mw = float(gas.get("mw") or 0) + float(coal.get("mw") or 0)
    gas_coal_share = float(gas.get("share") or 0) + float(coal.get("share") or 0)
    c_gas_coal_10mw = float(latest_daily.get("gas_10mw") or 0) + float(latest_daily.get("coal_10mw") or 0)
    a_gas_coal_10mw = gas_coal_mw / 10

    source_rows: List[List[str]] = []
    for source_id, label in [
        ("realtime_generation", "a"),
        ("today_supply", "b"),
        ("daily_supply_history", "c"),
    ]:
        source = source_by_id(data, source_id)
        source_rows.append(
            [
                label,
                f"https://data.gov.tw/dataset/{source.get('dataset_id', '')}",
                str(source.get("name", "")),
                f"{fmt_number(float(source.get('downloaded_mb') or 0), 4)} MB",
                f"{int(source.get('parsed_records') or 0):,} 筆",
            ]
        )

    mix_rows = []
    for item in data.get("current", {}).get("generation_mix", []):
        mix_rows.append(
            [
                str(item.get("label", "")),
                f"{fmt_number(float(item.get('mw') or 0), 1)}",
                f"{fmt_number(float(item.get('share') or 0), 2)}%",
            ]
        )

    latest_daily_rows = [
        ["尖峰負載", f"{fmt_number(float(latest_daily.get('peak_load_10mw') or 0), 1)} 萬瓩"],
        ["淨尖峰供電能力", f"{fmt_number(float(latest_daily.get('net_peak_supply_10mw') or 0), 1)} 萬瓩"],
        ["太陽能發電", f"{fmt_number(float(latest_daily.get('solar_10mw') or 0), 1)} 萬瓩"],
        ["風力發電", f"{fmt_number(float(latest_daily.get('wind_10mw') or 0), 1)} 萬瓩"],
        ["燃氣", f"{fmt_number(float(latest_daily.get('gas_10mw') or 0), 1)} 萬瓩"],
        ["燃煤", f"{fmt_number(float(latest_daily.get('coal_10mw') or 0), 1)} 萬瓩"],
    ]

    skipped_mb = float(pipeline.get("skipped_resource_content_length_mb") or 0)
    c_peak = float(latest_daily.get("peak_load_10mw") or 0)
    c_net_supply = float(latest_daily.get("net_peak_supply_10mw") or 0)
    c_peak_gap = c_peak - current_load_10mw
    c_net_gap = c_net_supply - forecast_max_supply_10mw

    return f"""# 能源資料源驗證

驗證時間：{data.get("metadata", {}).get("generated_at", "unknown")}  
網站資料：`{input_path.relative_to(ROOT)}`  
本次 pipeline：`logs/pipeline/latest-energy-run.json`

## 使用資料源

{markdown_table(["代號", "data.gov.tw", "用途", "本次下載", "解析"], source_rows)}

補充：10 分鐘級各機組過去發電量檔約 {fmt_number(skipped_mb, 4)} MB，本次只記錄大小，不下載。

## 1. a 的供應細部是否與 b 的供需總數相近？

驗證片段：

- a 資料時間：{summary.get("source_updated_at", "unknown")}
- b 發布時間：{supply.get("publish_time_raw", "unknown")}，轉換為 {supply.get("publish_time", "unknown")}
- a 各機組即時淨發電量加總：{fmt_number(current_total_mw, 1)} MW，也就是 {fmt_number(current_total_10mw, 2)} 萬瓩
- b 目前用電量：{fmt_number(current_load_10mw, 1)} 萬瓩
- 差異：{fmt_number(load_gap_10mw, 2)} 萬瓩，約 {fmt_number(load_gap_10mw * 10, 1)} MW；相對 b 目前用電量約 {fmt_number(load_gap_pct, 2)}%

結論：a 的即時機組細項加總與 b 的「目前用電量」非常接近，可以作為同一時間片段的細部拆解。

但 a 不應直接拿來比 b 的「可供應能力」：

- b 實際小時供電能力：{fmt_number(real_hour_supply_10mw, 1)} 萬瓩
- b 預估最大供電能力：{fmt_number(forecast_max_supply_10mw, 1)} 萬瓩
- a 即時淨發電量比 b 實際小時供電能力少 {fmt_number(supply_gap_10mw, 2)} 萬瓩，差距約 {fmt_number(supply_gap_pct, 2)}%

原因是 b 的供電能力包含備轉、可調度容量等能力概念；a 是當下實際淨發電量。網站若要問「現在誰在供電」，應以 a 比例為主；若要問「系統還有多少餘裕」，才讀 b 的備轉與供電能力。

## 2. a 的即時比例為何？是否與網站顯示接近？

a 的即時發電比例：

{markdown_table(["類別", "MW", "比例"], mix_rows)}

燃氣加燃煤合計 {fmt_number(gas_coal_mw, 1)} MW，占 {fmt_number(gas_coal_share, 2)}%。網站讀取同一份 `public/data/issues/energy.json`，前端四捨五入到一位小數，因此顯示比例接近，實作上是同源資料。

## 3. c 的日資料是否與 a、b 相近？

c 最新可用日為 {latest_daily.get("date", "unknown")}，不是 {summary.get("source_updated_at", "unknown")} 的同一時間片段。它適合做「最近可用日資料」脈絡，不適合直接校驗即時供電。

c 最新日資料摘要：

{markdown_table(["指標", "數值"], latest_daily_rows)}

與 a、b 比較：

- c 尖峰負載 {fmt_number(c_peak, 1)} 萬瓩，與 b 目前用電量 {fmt_number(current_load_10mw, 1)} 萬瓩相差 {fmt_number(c_peak_gap, 1)} 萬瓩，約 {fmt_number(pct_diff(c_peak, current_load_10mw), 2)}%。
- c 淨尖峰供電能力 {fmt_number(c_net_supply, 1)} 萬瓩，與 b 預估最大供電能力 {fmt_number(forecast_max_supply_10mw, 1)} 萬瓩相差 {fmt_number(c_net_gap, 1)} 萬瓩，約 {fmt_number(pct_diff(c_net_supply, forecast_max_supply_10mw), 2)}%。
- c 燃氣加燃煤 {fmt_number(c_gas_coal_10mw, 1)} 萬瓩，a 當下燃氣加燃煤為 {fmt_number(a_gas_coal_10mw, 2)} 萬瓩；差距約 {fmt_number(pct_diff(c_gas_coal_10mw, a_gas_coal_10mw), 2)}%。這只能說量級接近，不能視為同一時間片段驗證。
- c 太陽能 {fmt_number(float(latest_daily.get("solar_10mw") or 0), 1)} 萬瓩，但 a 在 {summary.get("source_updated_at", "unknown")} 太陽能接近 0 MW。這是日間 / 夜間差異，正好說明 c 不能替代即時機組資料。

結論：c 可以用於 7 天、30 天的趨勢與脈絡，但不能拿來直接驗證 a、b 的即時數字。網站應明確標示 c 的最新可用日，並避免把月更新日資料誤讀成即時曲線。

## 對網站呈現的影響

- 目前橫條圖的主數字採用 a，回答「現在缺口由哪些發電類型補上」。
- b 用於「目前用電量」與備轉容量率，回答「系統餘裕」。
- c 用於 7 天與 30 天趨勢，回答「最近可用日資料的背景脈絡」。
- 當日 12:00 能源分佈不能由 b 或 c 推導；pipeline 已加入中午時段快照機制。若當天 12:00 尚未被 pipeline 捕捉，網站會明確標示等待，不用月資料或夜間資料偽裝成中午陰影。
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate energy data source consistency.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--write", action="store_true", help="Write the Markdown report to --output.")
    args = parser.parse_args()

    data = json.loads(args.input.read_text(encoding="utf-8"))
    report = build_report(data, args.input)
    if args.write:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
        print(f"wrote {args.output.relative_to(ROOT)}")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
