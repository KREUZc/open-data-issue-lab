# Open Data Issue Lab 開放資料議題實驗室

MVP civic data site for issue-first public open data storytelling.

Primary question:

> 太陽下山後，電力缺口如何被補上？

## Run Locally

Fetch bounded energy data:

```bash
python3 scripts/fetch_issue_data.py energy
```

Refresh the source validation note:

```bash
python3 scripts/validate_energy_sources.py --write
```

Serve the static site:

```bash
python3 -m http.server 4173 --directory public
```

Open:

```text
http://localhost:4173
```

## Data Contract

Issue definitions live in:

```text
config/issues/issue-{issue-slug}.yaml
```

The energy MVP outputs:

```text
public/data/issues/energy.json
public/data/pipeline/latest-run.json
logs/pipeline/latest-energy-run.json
```

The pipeline records downloaded MB, parsed rows, output size, skipped resource size, and stale fallback state.

GitHub Actions refreshes data every 6 hours and deploys the `public/` directory to GitHub Pages.
