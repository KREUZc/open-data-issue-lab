# Open Data Issue Lab 開放資料議題實驗室

## Goal

Build a high-availability civic data MVP that helps people understand a public energy question:

> 太陽下山後，電力缺口如何被補上？

The product should feel like a public-interest data lab: readable at the entrance, rigorous in the method layer, transparent about data freshness and limitations.

## Scope

- Static website.
- No heavy build system.
- Daily data pipeline.
- Energy issue first.
- Only download bounded datasets needed for 7-day and 30-day energy numbers.
- Log downloaded bytes, parsed rows, output bytes, and skipped large resources.
- Show stale state when the latest pipeline run fails and the previous successful data is reused.

## Open Design Direction

Use the Open Design `sanity` design system as the base:

- Near-black lab canvas.
- Dense but readable data panels.
- Mono labels for source and pipeline metadata.
- Coral / red accent for active insight.
- Electric blue only as interaction signal.
- Avoid a pure dashboard look; the homepage still needs an editorial first read.

Adaptation for this civic data site:

- More generous Chinese body text line height.
- More contrast for public accessibility.
- Cards kept sharp and compact.
- Source and method metadata always visible near claims.

## Required Homepage Themes

1. 能源
2. 空氣品質
3. 交通安全
4. 托育人口
5. 居住負擔

Only energy is fully implemented in the MVP. The other four are topic blocks with possible data directions.

## Data Philosophy

- Data serves people, not the other way around.
- The first screen should answer one human question.
- Every chart should carry its source, freshness, and limitation.
- The pipeline should be portable across issue YAML files, different file types, and different parser shapes.
- Avoid downloading large historical resources unless a bounded parsing strategy exists.
