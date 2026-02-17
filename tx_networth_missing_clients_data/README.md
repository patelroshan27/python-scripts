# Missing Weekly Data Report

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configure
Edit `config/qa.json` or `config/prod.json` with MySQL credentials.

## Run
```bash
python3 scripts/check_missing_weeks.py --start-year 2026 --start-week 1 --end-year 2026 --end-week 5 --batch-size 5000 --limit 200
```

### Notes
- The script **paginates** through accounts using `a.id > last_id` and `--batch-size`.
- Output is written to `results/missing_weeks_<timestamp>.json`.