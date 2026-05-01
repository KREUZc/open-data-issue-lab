#!/usr/bin/env python3
"""Fetch bounded issue data for Open Data Issue Lab.

The script deliberately uses only the Python standard library. The issue YAML
parser supports the small subset documented in config/issues/README.md.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import os
import re
import sys
import traceback
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config" / "issues"
PUBLIC_DATA_DIR = ROOT / "public" / "data"
ISSUE_DATA_DIR = PUBLIC_DATA_DIR / "issues"
PIPELINE_DATA_DIR = PUBLIC_DATA_DIR / "pipeline"
LOG_DIR = ROOT / "logs" / "pipeline"
NOON_REFERENCE_PATH = ISSUE_DATA_DIR / "energy-noon-reference.json"

ENERGY_COLORS = {
    "gas": "#ff8c00",
    "coal": "#696969",
    "solar": "#ffd700",
    "wind": "#2e8b57",
    "hydro": "#1e90ff",
    "storage": "#00c2ff",
    "nuclear": "#9370db",
    "cogen": "#c17c4a",
    "oil": "#b45a3c",
    "other": "#b9b9b9",
}

ENERGY_LABELS = {
    "gas": "燃氣",
    "coal": "燃煤",
    "solar": "太陽能",
    "wind": "風力",
    "hydro": "水力",
    "storage": "儲能/抽蓄",
    "nuclear": "核能",
    "cogen": "汽電共生",
    "oil": "燃油",
    "other": "其他",
}


def now_taipei() -> dt.datetime:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=8)))


def iso_now() -> str:
    return now_taipei().isoformat(timespec="seconds")


def parse_scalar(raw: str) -> Any:
    raw = raw.strip()
    if raw == "":
        return ""
    if raw in {"true", "false"}:
        return raw == "true"
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        return raw[1:-1]
    if re.fullmatch(r"-?\d+", raw):
        try:
            return int(raw)
        except ValueError:
            return raw
    if re.fullmatch(r"-?\d+\.\d+", raw):
        try:
            return float(raw)
        except ValueError:
            return raw
    return raw


def split_key_value(raw: str) -> Tuple[str, Any]:
    if ":" not in raw:
        return raw.strip(), ""
    key, value = raw.split(":", 1)
    return key.strip(), parse_scalar(value)


def load_issue_yaml(path: Path) -> Dict[str, Any]:
    issue: Dict[str, Any] = {}
    current_section = None
    current_item = None

    for original in path.read_text(encoding="utf-8").splitlines():
        if not original.strip() or original.lstrip().startswith("#"):
            continue
        indent = len(original) - len(original.lstrip(" "))
        line = original.strip()

        if indent == 0:
            key, value = split_key_value(line)
            current_section = key
            current_item = None
            if value == "":
                if key in {"sources", "metrics", "charts", "limits"}:
                    issue[key] = []
                else:
                    issue[key] = {}
            else:
                issue[key] = value
            continue

        if current_section is None:
            continue

        if isinstance(issue.get(current_section), dict) and indent == 2:
            key, value = split_key_value(line)
            issue[current_section][key] = value
            continue

        if isinstance(issue.get(current_section), list) and indent == 2 and line.startswith("- "):
            payload = line[2:].strip()
            if current_section == "limits" and ":" not in payload:
                issue[current_section].append(parse_scalar(payload))
                current_item = None
            else:
                key, value = split_key_value(payload)
                current_item = {key: value}
                issue[current_section].append(current_item)
            continue

        if isinstance(issue.get(current_section), list) and indent == 4 and current_item is not None:
            key, value = split_key_value(line)
            current_item[key] = value

    return issue


def request_head(url: str, timeout: int = 30) -> Dict[str, Any]:
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "OpenDataIssueLab/0.1"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return {
            "status": response.status,
            "content_length": int(response.headers.get("Content-Length") or 0),
            "content_type": response.headers.get("Content-Type"),
            "last_modified": response.headers.get("Last-Modified"),
            "etag": response.headers.get("ETag"),
        }


def download_bytes(url: str, timeout: int = 45) -> Tuple[bytes, Dict[str, Any]]:
    req = urllib.request.Request(url, headers={"User-Agent": "OpenDataIssueLab/0.1"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        data = response.read()
        meta = {
            "status": response.status,
            "content_length": int(response.headers.get("Content-Length") or len(data)),
            "content_type": response.headers.get("Content-Type"),
            "last_modified": response.headers.get("Last-Modified"),
            "etag": response.headers.get("ETag"),
        }
        return data, meta


def as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    text = str(value).strip()
    if text in {"", "-", "N/A"}:
        return default
    text = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return default
    return float(match.group(0))


def parse_json_payload(raw: bytes) -> Any:
    return json.loads(raw.decode("utf-8-sig"))


def normalize_realtime_fuel(fuel_type: str) -> str:
    text = re.sub(r"<[^>]+>", "", fuel_type or "")
    if "燃氣" in text:
        return "gas"
    if "燃煤" in text:
        return "coal"
    if "太陽" in text:
        return "solar"
    if "風力" in text:
        return "wind"
    if "水力" in text:
        return "hydro"
    if "抽蓄" in text or "儲能" in text:
        return "storage"
    if "核" in text:
        return "nuclear"
    if "汽電" in text:
        return "cogen"
    if "燃料油" in text or "輕油" in text or "燃油" in text:
        return "oil"
    return "other"


def parse_realtime_generation(raw: bytes, source: Dict[str, Any]) -> Dict[str, Any]:
    payload = parse_json_payload(raw)
    rows = payload.get("aaData", [])
    subtotals: Dict[str, float] = defaultdict(float)
    fallback_totals: Dict[str, float] = defaultdict(float)

    for row in rows:
        fuel_type = row.get("機組類型", "")
        unit_name = row.get("機組名稱", "")
        group = normalize_realtime_fuel(fuel_type)
        net_mw = as_float(row.get("淨發電量(MW)"))
        if unit_name.startswith("小計"):
            subtotals[group] += net_mw
        else:
            fallback_totals[group] += net_mw

    totals = dict(fallback_totals)
    for group, value in subtotals.items():
        totals[group] = value

    ordered_keys = ["gas", "coal", "solar", "wind", "hydro", "storage", "nuclear", "cogen", "oil", "other"]
    total_mw = sum(max(totals.get(key, 0.0), 0.0) for key in ordered_keys)
    mix = []
    for key in ordered_keys:
        mw = round(totals.get(key, 0.0), 1)
        if abs(mw) < 0.05:
            continue
        mix.append(
            {
                "id": key,
                "label": ENERGY_LABELS[key],
                "mw": mw,
                "share": round((mw / total_mw) * 100, 2) if total_mw else 0,
                "color": ENERGY_COLORS[key],
            }
        )

    solar_mw = totals.get("solar", 0.0)
    gas_coal_mw = totals.get("gas", 0.0) + totals.get("coal", 0.0)
    return {
        "source_id": source["id"],
        "dataset_id": source["dataset_id"],
        "source_updated_at": payload.get("DateTime"),
        "parsed_records": len(rows),
        "total_mw": round(total_mw, 1),
        "solar_mw": round(solar_mw, 1),
        "gas_coal_mw": round(gas_coal_mw, 1),
        "mix": mix,
    }


def parse_minguo_datetime(value: str) -> str:
    if not value:
        return ""
    match = re.search(r"(\d{2,3})\.(\d{1,2})\.(\d{1,2}).*?(\d{1,2}):(\d{2})", value)
    if not match:
        return value
    year = int(match.group(1)) + 1911
    month = int(match.group(2))
    day = int(match.group(3))
    hour = int(match.group(4))
    minute = int(match.group(5))
    return dt.datetime(year, month, day, hour, minute).isoformat(timespec="minutes")


def parse_today_supply(raw: bytes, source: Dict[str, Any]) -> Dict[str, Any]:
    payload = parse_json_payload(raw)
    merged: Dict[str, str] = {}
    for record in payload.get("records", []):
        merged.update(record)

    return {
        "source_id": source["id"],
        "dataset_id": source["dataset_id"],
        "parsed_records": len(payload.get("records", [])),
        "current_load_10mw": as_float(merged.get("curr_load")),
        "current_util_rate": as_float(merged.get("curr_util_rate")),
        "forecast_max_supply_10mw": as_float(merged.get("fore_maxi_sply_capacity")),
        "forecast_peak_load_10mw": as_float(merged.get("fore_peak_dema_load")),
        "forecast_reserve_10mw": as_float(merged.get("fore_peak_resv_capacity")),
        "forecast_reserve_rate": as_float(merged.get("fore_peak_resv_rate")),
        "forecast_indicator": merged.get("fore_peak_resv_indicator", ""),
        "forecast_peak_hour_range": merged.get("fore_peak_hour_range", ""),
        "publish_time_raw": merged.get("publish_time", ""),
        "publish_time": parse_minguo_datetime(merged.get("publish_time", "")),
        "yesterday_date": merged.get("yday_date", ""),
        "real_hour_supply_10mw": as_float(merged.get("real_hr_maxi_sply_capacity")),
        "real_hour_peak_time": merged.get("real_hr_peak_time", ""),
    }


def row_date(value: str) -> str:
    text = str(value).strip()
    if re.fullmatch(r"\d{8}", text):
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    return text


def sum_columns(row: Dict[str, str], needles: Iterable[str]) -> float:
    total = 0.0
    for name in needles:
        if name in row:
            total += as_float(row.get(name))
    return total


def parse_daily_supply_history(raw: bytes, source: Dict[str, Any], trend_days: int = 30, recent_days: int = 7) -> Dict[str, Any]:
    text = raw.decode("utf-8-sig")
    reader = csv.DictReader(text.splitlines())
    rows = [row for row in reader if row.get("日期")]

    coal_cols = [
        "林口#1(萬瓩)", "林口#2(萬瓩)", "林口#3(萬瓩)",
        "台中#1(萬瓩)", "台中#2(萬瓩)", "台中#3(萬瓩)", "台中#4(萬瓩)", "台中#5(萬瓩)",
        "台中#6(萬瓩)", "台中#7(萬瓩)", "台中#8(萬瓩)", "台中#9(萬瓩)", "台中#10(萬瓩)",
        "興達#1(萬瓩)", "興達#2(萬瓩)", "興達#3(萬瓩)", "興達#4(萬瓩)",
        "大林#1(萬瓩)", "大林#2(萬瓩)",
        "和平#1(萬瓩)", "和平#2(萬瓩)",
        "麥寮#1(萬瓩)", "麥寮#2(萬瓩)", "麥寮#3(萬瓩)",
    ]
    gas_cols = [
        "大潭 (#1-#6)(萬瓩)", "通霄 (#1-#6)(萬瓩)", "通霄 (#1-#6、GT#9)(萬瓩)",
        "興達 (#1-#5)(萬瓩)", "南部 (#1-#4)(萬瓩)", "大林(#5-#6)(萬瓩)",
        "海湖 (#1-#2)(萬瓩)", "國光 #1(萬瓩)", "新桃#1(萬瓩)", "星彰#1(萬瓩)",
        "星元#1(萬瓩)", "嘉惠#1(萬瓩)", "豐德(#1-#2)(萬瓩)",
    ]
    hydro_cols = [
        "德基(萬瓩)", "青山(萬瓩)", "谷關(萬瓩)", "天輪(萬瓩)", "馬鞍(萬瓩)",
        "萬大(萬瓩)", "大觀(萬瓩)", "鉅工(萬瓩)", "大觀二(萬瓩)", "明潭(萬瓩)",
        "碧海(萬瓩)", "立霧(萬瓩)", "龍澗(萬瓩)", "卓蘭(萬瓩)", "水里(萬瓩)",
        "其他小水力(萬瓩)",
    ]
    nuclear_cols = [
        "核一#1(萬瓩)", "核一#2(萬瓩)", "核二#1(萬瓩)", "核二#2(萬瓩)",
        "核三#1(萬瓩)", "核三#2(萬瓩)",
    ]

    normalized = []
    for row in rows:
        item = {
            "date": row_date(row.get("日期", "")),
            "peak_load_10mw": as_float(row.get("尖峰負載(萬瓩)")),
            "net_peak_supply_10mw": as_float(row.get("淨尖峰供電能力(萬瓩)")),
            "reserve_10mw": as_float(row.get("備轉容量(萬瓩)")),
            "reserve_rate": as_float(row.get("備轉容量率(%)")),
            "solar_10mw": as_float(row.get("太陽能發電(萬瓩)")),
            "wind_10mw": as_float(row.get("風力發電(萬瓩)")),
            "gas_10mw": round(sum_columns(row, gas_cols), 3),
            "coal_10mw": round(sum_columns(row, coal_cols), 3),
            "hydro_10mw": round(sum_columns(row, hydro_cols), 3),
            "nuclear_10mw": round(sum_columns(row, nuclear_cols), 3),
            "cogen_10mw": as_float(row.get("汽電共生(萬瓩)")),
        }
        normalized.append(item)

    normalized = sorted(normalized, key=lambda item: item["date"])
    latest_30 = normalized[-trend_days:]
    latest_7 = normalized[-recent_days:]

    avg = lambda key, items: round(sum(item[key] for item in items) / len(items), 2) if items else 0
    return {
        "source_id": source["id"],
        "dataset_id": source["dataset_id"],
        "parsed_records": len(rows),
        "latest_available_date": normalized[-1]["date"] if normalized else "",
        "seven_days": latest_7,
        "thirty_days": latest_30,
        "thirty_day_averages": {
            "peak_load_10mw": avg("peak_load_10mw", latest_30),
            "solar_10mw": avg("solar_10mw", latest_30),
            "wind_10mw": avg("wind_10mw", latest_30),
            "gas_10mw": avg("gas_10mw", latest_30),
            "coal_10mw": avg("coal_10mw", latest_30),
            "reserve_rate": avg("reserve_rate", latest_30),
        },
    }


def mb(bytes_count: int) -> float:
    return round(bytes_count / (1024 * 1024), 4)


def parse_iso_timestamp(value: str) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        return None


def minutes_from_noon(value: str) -> Optional[int]:
    timestamp = parse_iso_timestamp(value)
    if timestamp is None:
        return None
    noon = timestamp.replace(hour=12, minute=0, second=0, microsecond=0)
    return abs(int((timestamp - noon).total_seconds() // 60))


def build_noon_reference(generation: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = parse_iso_timestamp(str(generation.get("source_updated_at") or ""))
    if timestamp is None:
        return {
            "status": "unavailable",
            "reason": "current_source_timestamp_unavailable",
        }

    target_date = timestamp.date().isoformat()
    if NOON_REFERENCE_PATH.exists():
        previous = json.loads(NOON_REFERENCE_PATH.read_text(encoding="utf-8"))
    else:
        previous = {}

    current_distance = minutes_from_noon(generation.get("source_updated_at", ""))
    previous_distance = minutes_from_noon(previous.get("source_updated_at", ""))
    should_capture = (
        timestamp.hour == 12
        and (
            previous.get("date") != target_date
            or previous_distance is None
            or (current_distance is not None and current_distance < previous_distance)
        )
    )

    if should_capture:
        previous = {
            "status": "available",
            "date": target_date,
            "source_updated_at": generation.get("source_updated_at"),
            "captured_at": iso_now(),
            "total_mw": generation.get("total_mw", 0),
            "mix": generation.get("mix", []),
        }
        write_json(NOON_REFERENCE_PATH, previous)

    if previous.get("date") == target_date and previous.get("mix"):
        return previous

    return {
        "status": "pending",
        "target_date": target_date,
        "reason": "no_current_day_noon_snapshot_yet",
        "message": "當日12點機組分佈需由中午時段 pipeline 捕捉；不以月資料或當下資料替代。",
    }


def source_max_bytes(source: Dict[str, Any]) -> int:
    return int(float(source.get("max_download_mb", 1)) * 1024 * 1024)


def fetch_source(source: Dict[str, Any]) -> Tuple[Dict[str, Any], Any]:
    source_log: Dict[str, Any] = {
        "id": source["id"],
        "dataset_id": source.get("dataset_id"),
        "name": source.get("name"),
        "url": source.get("url"),
        "official_page": source.get("official_page"),
        "parser": source.get("parser"),
        "max_download_mb": float(source.get("max_download_mb", 1)),
        "downloaded_bytes": 0,
        "downloaded_mb": 0,
        "parsed_records": 0,
        "status": "pending",
    }

    try:
        head = request_head(source["url"])
        source_log.update(
            {
                "head_status": head.get("status"),
                "content_length": head.get("content_length", 0),
                "content_length_mb": mb(head.get("content_length", 0)),
                "content_type": head.get("content_type"),
                "last_modified": head.get("last_modified"),
                "etag": head.get("etag"),
            }
        )
    except Exception as exc:
        source_log["head_error"] = str(exc)
        head = {"content_length": 0}

    parser_name = str(source.get("parser"))
    if parser_name == "skip_large_resource":
        source_log["status"] = "skipped"
        source_log["reason"] = "download_guardrail"
        source_log["parsed_records"] = 0
        return source_log, None

    content_length = int(head.get("content_length") or 0)
    if content_length and content_length > source_max_bytes(source):
        source_log["status"] = "skipped"
        source_log["reason"] = "content_length_exceeds_max_download_mb"
        return source_log, None

    raw, response_meta = download_bytes(source["url"])
    source_log.update(
        {
            "status": "downloaded",
            "http_status": response_meta.get("status"),
            "downloaded_bytes": len(raw),
            "downloaded_mb": mb(len(raw)),
            "content_type": response_meta.get("content_type"),
            "last_modified": response_meta.get("last_modified") or source_log.get("last_modified"),
            "etag": response_meta.get("etag") or source_log.get("etag"),
        }
    )

    if len(raw) > source_max_bytes(source):
        source_log["status"] = "failed"
        source_log["error"] = "downloaded_bytes_exceeds_max_download_mb"
        raise RuntimeError(f"{source['id']} exceeded max_download_mb after download")

    if parser_name == "parse_realtime_generation":
        parsed = parse_realtime_generation(raw, source)
    elif parser_name == "parse_today_supply":
        parsed = parse_today_supply(raw, source)
    elif parser_name == "parse_daily_supply_history":
        parsed = parse_daily_supply_history(raw, source)
    else:
        raise RuntimeError(f"unknown parser: {parser_name}")

    source_log["status"] = "parsed"
    source_log["parsed_records"] = parsed.get("parsed_records", 0)
    return source_log, parsed


def compose_energy_output(issue: Dict[str, Any], parsed_by_source: Dict[str, Any], source_logs: List[Dict[str, Any]]) -> Dict[str, Any]:
    generation = parsed_by_source.get("realtime_generation", {})
    supply = parsed_by_source.get("today_supply", {})
    daily = parsed_by_source.get("daily_supply_history", {})
    noon_reference = build_noon_reference(generation)

    total_downloaded = sum(int(item.get("downloaded_bytes", 0)) for item in source_logs)
    total_parsed = sum(int(item.get("parsed_records", 0)) for item in source_logs)
    total_skipped_length = sum(int(item.get("content_length", 0)) for item in source_logs if item.get("status") == "skipped")

    solar_mw = generation.get("solar_mw", 0)
    gas_coal_mw = generation.get("gas_coal_mw", 0)
    total_mw = generation.get("total_mw", 0)
    gas_coal_share = round((gas_coal_mw / total_mw) * 100, 1) if total_mw else 0

    if solar_mw <= 1 and total_mw:
        answer = f"目前太陽能接近 0 MW，燃氣與燃煤合計約 {gas_coal_mw:,.0f} MW，承擔約 {gas_coal_share}% 的即時發電。"
    elif total_mw:
        answer = f"目前太陽能仍有約 {solar_mw:,.0f} MW，燃氣與燃煤合計約 {gas_coal_mw:,.0f} MW，是主要調度支撐。"
    else:
        answer = "目前沒有可用的即時發電資料。"

    return {
        "metadata": {
            "schema_version": "0.1.0",
            "issue_slug": issue.get("slug"),
            "issue_title": issue.get("title"),
            "question": issue.get("question"),
            "generated_at": iso_now(),
            "timezone": issue.get("data_window", {}).get("timezone", "Asia/Taipei"),
            "status": "fresh",
            "stale": False,
        },
        "summary": {
            "answer": answer,
            "source_updated_at": generation.get("source_updated_at") or supply.get("publish_time"),
            "latest_daily_available_date": daily.get("latest_available_date"),
            "current_total_generation_mw": total_mw,
            "current_load_10mw": supply.get("current_load_10mw", 0),
            "forecast_reserve_rate": supply.get("forecast_reserve_rate", 0),
            "forecast_peak_hour_range": supply.get("forecast_peak_hour_range", ""),
            "thirty_day_averages": daily.get("thirty_day_averages", {}),
        },
        "current": {
            "generation_mix": generation.get("mix", []),
            "noon_reference": noon_reference,
            "supply": supply,
        },
        "daily": {
            "seven_days": daily.get("seven_days", []),
            "thirty_days": daily.get("thirty_days", []),
        },
        "pipeline_summary": {
            "total_downloaded_bytes": total_downloaded,
            "total_downloaded_mb": mb(total_downloaded),
            "total_parsed_records": total_parsed,
            "skipped_resource_content_length_bytes": total_skipped_length,
            "skipped_resource_content_length_mb": mb(total_skipped_length),
            "source_count": len(source_logs),
        },
        "sources": source_logs,
        "limits": issue.get("limits", []),
    }


def mark_stale(previous: Dict[str, Any], error: str, run_started_at: str) -> Dict[str, Any]:
    output = dict(previous)
    output["metadata"] = dict(previous.get("metadata", {}))
    output["metadata"]["status"] = "stale"
    output["metadata"]["stale"] = True
    output["metadata"]["last_attempt_at"] = iso_now()
    output["metadata"]["last_attempt_error"] = error
    output["metadata"]["stale_reason"] = "latest_pipeline_failed_using_last_success"
    output["pipeline_summary"] = dict(previous.get("pipeline_summary", {}))
    output["pipeline_summary"]["last_failed_run_started_at"] = run_started_at
    return output


def write_json(path: Path, payload: Dict[str, Any]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    path.write_text(text + "\n", encoding="utf-8")
    return path.stat().st_size


def run(issue_slug: str) -> int:
    run_started_at = iso_now()
    issue_path = CONFIG_DIR / f"issue-{issue_slug}.yaml"
    output_path = ISSUE_DATA_DIR / f"{issue_slug}.json"
    latest_log_path = PIPELINE_DATA_DIR / "latest-run.json"
    latest_full_log_path = LOG_DIR / f"latest-{issue_slug}-run.json"
    timestamp_log_path = LOG_DIR / f"{issue_slug}-{now_taipei().strftime('%Y%m%dT%H%M%S%z')}.json"

    source_logs: List[Dict[str, Any]] = []
    run_log: Dict[str, Any] = {
        "issue_slug": issue_slug,
        "started_at": run_started_at,
        "finished_at": None,
        "status": "running",
        "sources": source_logs,
        "output_path": str(output_path.relative_to(ROOT)),
    }

    try:
        issue = load_issue_yaml(issue_path)
        parsed_by_source: Dict[str, Any] = {}

        for source in issue.get("sources", []):
            source_log, parsed = fetch_source(source)
            source_logs.append(source_log)
            if parsed is not None:
                parsed_by_source[source["id"]] = parsed

        required = {"realtime_generation", "today_supply", "daily_supply_history"}
        missing = required - set(parsed_by_source)
        if missing:
            raise RuntimeError(f"missing required parsed sources: {', '.join(sorted(missing))}")

        output = compose_energy_output(issue, parsed_by_source, source_logs)
        output_bytes = write_json(output_path, output)
        output["pipeline_summary"]["output_bytes"] = output_bytes
        output["pipeline_summary"]["output_mb"] = mb(output_bytes)
        output_bytes = write_json(output_path, output)

        run_log.update(
            {
                "status": "success",
                "finished_at": iso_now(),
                "total_downloaded_bytes": output["pipeline_summary"]["total_downloaded_bytes"],
                "total_downloaded_mb": output["pipeline_summary"]["total_downloaded_mb"],
                "total_parsed_records": output["pipeline_summary"]["total_parsed_records"],
                "output_bytes": output_bytes,
                "output_mb": mb(output_bytes),
            }
        )
        write_json(latest_log_path, run_log)
        write_json(latest_full_log_path, run_log)
        write_json(timestamp_log_path, run_log)
        print(json.dumps(run_log, ensure_ascii=False, indent=2))
        return 0

    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        run_log.update(
            {
                "status": "failed",
                "finished_at": iso_now(),
                "error": error,
                "traceback": traceback.format_exc(),
            }
        )

        if output_path.exists():
            previous = json.loads(output_path.read_text(encoding="utf-8"))
            stale = mark_stale(previous, error, run_started_at)
            stale_bytes = write_json(output_path, stale)
            run_log.update(
                {
                    "status": "stale_success",
                    "stale_output_bytes": stale_bytes,
                    "stale_output_mb": mb(stale_bytes),
                    "stale_reason": "latest_pipeline_failed_using_last_success",
                }
            )
            write_json(latest_log_path, run_log)
            write_json(latest_full_log_path, run_log)
            write_json(timestamp_log_path, run_log)
            print(json.dumps(run_log, ensure_ascii=False, indent=2), file=sys.stderr)
            return 0

        write_json(latest_log_path, run_log)
        write_json(latest_full_log_path, run_log)
        write_json(timestamp_log_path, run_log)
        print(json.dumps(run_log, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    slug = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("ISSUE_SLUG", "energy")
    raise SystemExit(run(slug))
