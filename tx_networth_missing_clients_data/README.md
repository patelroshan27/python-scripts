# Missing Weekly Data Report

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configure
Edit `config/qa.json` or `config/prod.json` with MySQL credentials.

## Run Weekly Job
```bash
python3 scripts/check_missing_weeks.py --start-year 2025 --start-week 1 --end-year 2025 --end-week 52 --workers 32 --semaphore 50 --batch-size 2000 --env prod --max-clients 50000
```

## Run Monthly Job
```bash
python3 scripts/check_missing_months.py --start-year 2025 --start-month 1 --end-year 2025 --end-month 12 --workers 32 --semaphore 50 --batch-size 2000 --env prod --max-clients 50000
```


### Notes
- The script **paginates** through accounts using `a.id > last_id` and `--batch-size`.
- Output is written to `results/missing_weeks_<timestamp>.json`.