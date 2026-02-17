import argparse
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

import mysql.connector


CLIENT_ACCOUNT_QUERY = """
select distinct cim.public_id as client_public_id, a.id as account_id
from tx_data_service.client_id_mapping cim
join tx_data_service.account a on cim.id = a.client_id
where a.id > %s
order by a.id
limit %s
"""

WEEKS_QUERY = """
select year, week
from tx_data_service.account_summary_weekly
where client_public_id = %s
  and account_id = %s
  and (
        (year = %s and week >= %s)
     or (year > %s and year < %s)
     or (year = %s and week <= %s)
  )
"""


def iso_weeks_in_year(year: int) -> int:
    return date(year, 12, 28).isocalendar().week


def load_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_connection(cfg: dict):
    return mysql.connector.connect(
        host=cfg["host"],
        port=cfg.get("port", 3306),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg.get("database", "tx_data_service"),
    )


def fetch_client_accounts(conn, batch_size: int):
    last_id = 0
    while True:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(CLIENT_ACCOUNT_QUERY, (last_id, batch_size))
            rows = cur.fetchall()
        if not rows:
            break
        for row in rows:
            yield row
            last_id = row["account_id"]


def fetch_weeks(conn, client_public_id: str, account_id: int,
                start_year: int, start_week: int,
                end_year: int, end_week: int):
    with conn.cursor(dictionary=True) as cur:
        cur.execute(
            WEEKS_QUERY,
            (client_public_id, account_id,
             start_year, start_week,
             start_year, end_year,
             end_year, end_week),
        )
        return {(row["year"], row["week"]) for row in cur.fetchall()}


def expected_weeks(start_year: int, start_week: int,
                   end_year: int, end_week: int):
    expected = set()
    for y in range(start_year, end_year + 1):
        y_start = start_week if y == start_year else 1
        y_end = end_week if y == end_year else iso_weeks_in_year(y)
        for w in range(y_start, y_end + 1):
            expected.add((y, w))
    return expected


def worker_task(cfg: dict, semaphore: threading.Semaphore, exp: set,
                start_year: int, start_week: int, end_year: int, end_week: int,
                row: dict):
    with semaphore:
        conn = get_connection(cfg)
        try:
            client_public_id = row["client_public_id"]
            account_id = row["account_id"]
            actual = fetch_weeks(
                conn,
                client_public_id,
                account_id,
                start_year,
                start_week,
                end_year,
                end_week,
            )
            missing = sorted(list(exp - actual))
            if missing:
                return {
                    "client_public_id": client_public_id,
                    "account_id": account_id,
                    "missing_weeks": [{"year": y, "week": w} for (y, w) in missing],
                }
            return None
        finally:
            conn.close()


def main():
    start_time = time.time()
    today_iso = date.today().isocalendar()

    parser = argparse.ArgumentParser(description="Find missing weekly account summary data")
    parser.add_argument("--config", default="config/qa.json")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--start-week", type=int, default=1)
    parser.add_argument("--end-year", type=int, default=today_iso.year)
    parser.add_argument("--end-week", type=int, default=today_iso.week)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--semaphore", type=int, default=8)
    parser.add_argument("--max-clients", type=int, default=None)
    parser.add_argument("--output", default=None)

    args = parser.parse_args()

    cfg = load_config(Path(args.config))
    output_path = args.output
    if not output_path:
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        output_path = f"results/missing_weeks_{ts}.json"

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "start_year": args.start_year,
        "start_week": args.start_week,
        "end_year": args.end_year,
        "end_week": args.end_week,
        "total_pairs": 0,
        "missing": [],
    }

    exp = expected_weeks(args.start_year, args.start_week, args.end_year, args.end_week)
    semaphore = threading.Semaphore(args.semaphore)

    main_conn = get_connection(cfg)
    try:
        total = 0
        futures = []

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            for row in fetch_client_accounts(main_conn, args.batch_size):
                total += 1
                report["total_pairs"] = total
                futures.append(
                    executor.submit(
                        worker_task,
                        cfg,
                        semaphore,
                        exp,
                        args.start_year,
                        args.start_week,
                        args.end_year,
                        args.end_week,
                        row,
                    )
                )

                if total % 1000 == 0:
                    print(f"Processed clients: {total}")

                if args.max_clients and total >= args.max_clients:
                    break

            for f in as_completed(futures):
                result = f.result()
                if result:
                    report["missing"].append(result)

        print(f"Processed clients: {total}")
    finally:
        main_conn.close()

    elapsed = time.time() - start_time
    print(f"Time taken: {elapsed:.2f} seconds for {report['total_pairs']} clients")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Report saved to {output_path}")


if __name__ == "__main__":
    main()