import argparse
import json
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

import mysql.connector
from mysql.connector import pooling


CLIENT_ACCOUNT_QUERY = """
select distinct cim.public_id as client_public_id, a.id as account_id
from tx_data_service.client_id_mapping cim
join tx_data_service.account a on cim.id = a.client_id
where a.id > %s
order by a.id
limit %s
"""

WEEKS_QUERY_BATCH = """
select account_id, year, week
from tx_data_service.account_summary_weekly
where account_id IN ({})
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


def get_connection_pool(cfg: dict, pool_size: int):
    return pooling.MySQLConnectionPool(
        pool_name="tx_pool",
        pool_size=pool_size,
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


def fetch_weeks_batch(conn, account_ids: list,
                      start_year: int, start_week: int,
                      end_year: int, end_week: int):
    if not account_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(account_ids))
    query = WEEKS_QUERY_BATCH.format(placeholders)
    params = tuple(account_ids) + (
        start_year,
        start_week,
        start_year,
        end_year,
        end_year,
        end_week,
    )

    with conn.cursor(dictionary=True) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    results = {}
    for row in rows:
        acc_id = row["account_id"]
        if acc_id not in results:
            results[acc_id] = set()
        results[acc_id].add((row["year"], row["week"]))
    return results


def expected_weeks(start_year: int, start_week: int,
                   end_year: int, end_week: int):
    expected = set()
    for y in range(start_year, end_year + 1):
        y_start = start_week if y == start_year else 1
        y_end = end_week if y == end_year else iso_weeks_in_year(y)
        for w in range(y_start, y_end + 1):
            expected.add((y, w))
    return expected


def worker_task(pool: pooling.MySQLConnectionPool, semaphore: threading.Semaphore, exp: set,
                start_year: int, start_week: int, end_year: int, end_week: int,
                rows: list):
    with semaphore:
        try:
            conn = pool.get_connection()
        except (mysql.connector.Error, socket.gaierror) as err:
            print(f"Database connection error for batch of {len(rows)} accounts: {err}", flush=True)
            return []

        try:
            account_ids = [row["account_id"] for row in rows]
            actual_map = fetch_weeks_batch(
                conn,
                account_ids,
                start_year,
                start_week,
                end_year,
                end_week,
            )

            results = []
            for row in rows:
                client_public_id = row["client_public_id"]
                account_id = row["account_id"]
                actual = actual_map.get(account_id, set())
                missing = sorted(list(exp - actual))
                if missing:
                    results.append({
                        "client_public_id": client_public_id,
                        "account_id": account_id,
                        "missing_weeks": [{"year": y, "week": w} for (y, w) in missing],
                    })
            return results
        except mysql.connector.Error as err:
            print(f"Database error during query execution for batch of {len(rows)} accounts: {err}", flush=True)
            return []
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

    exp = expected_weeks(args.start_year, args.start_week, args.end_year, args.end_week)
    semaphore = threading.Semaphore(args.semaphore)

    pool = get_connection_pool(cfg, args.workers)
    main_conn = get_connection(cfg)

    try:
        total = 0
        futures = []

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            batch = []
            for row in fetch_client_accounts(main_conn, args.batch_size):
                total += 1
                batch.append(row)

                if len(batch) >= 100:
                    futures.append(
                        executor.submit(
                            worker_task,
                            pool,
                            semaphore,
                            exp,
                            args.start_year,
                            args.start_week,
                            args.end_year,
                            args.end_week,
                            batch,
                        )
                    )
                    batch = []

                if total % 10000 == 0:
                    print(f"Fetched accounts: {total}", flush=True)

                if args.max_clients and total >= args.max_clients:
                    break

            if batch:
                futures.append(
                    executor.submit(
                        worker_task,
                        pool,
                        semaphore,
                        exp,
                        args.start_year,
                        args.start_week,
                        args.end_year,
                        args.end_week,
                        batch,
                    )
                )

            print(f"All tasks submitted. Total accounts: {total}. Waiting for results...", flush=True)

            with open(output_path, "w", encoding="utf-8") as f_out:
                f_out.write("{\n")
                f_out.write(f'  "generated_at": {json.dumps(datetime.now().isoformat(timespec="seconds"))},\n')
                f_out.write(f'  "start_year": {args.start_year},\n')
                f_out.write(f'  "start_week": {args.start_week},\n')
                f_out.write(f'  "end_year": {args.end_year},\n')
                f_out.write(f'  "end_week": {args.end_week},\n')
                f_out.write('  "missing": [\n')

                first = True
                completed_batches = 0
                total_batches = len(futures)

                # Report every batch if few, otherwise every 50
                report_interval = 1 if total_batches <= 50 else 50

                for f in as_completed(futures):
                    results = f.result()
                    completed_batches += 1
                    if completed_batches % report_interval == 0 or completed_batches == total_batches:
                        print(f"Collected results for {completed_batches}/{total_batches} batches...", flush=True)

                    if results:
                        for res in results:
                            if not first:
                                f_out.write(",\n")
                            f_out.write("    " + json.dumps(res))
                            first = False

                f_out.write(f'\n  ],\n  "total_pairs": {total}\n}}')

        print(f"Processed total accounts: {total}", flush=True)
    finally:
        main_conn.close()

    elapsed = time.time() - start_time
    print(f"Time taken: {elapsed:.2f} seconds for {total} accounts", flush=True)

    print(f"Report saved to {output_path}", flush=True)


if __name__ == "__main__":
    main()