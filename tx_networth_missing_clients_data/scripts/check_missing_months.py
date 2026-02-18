import argparse
import json
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

import mysql.connector
from mysql.connector import pooling


CLIENT_ACCOUNT_QUERY = """
select distinct cim.public_id as client_public_id, a.id as account_id, a.public_id as yodlee_id
from tx_data_service.client_id_mapping cim
join tx_data_service.account a on cim.id = a.client_id
where a.id > %s
order by a.id
limit %s
"""

MONTHS_QUERY_BATCH = """
select client_public_id, account_id, year, month
from tx_data_service.account_summary_monthly
where (client_public_id, account_id) IN ({})
  and (year * 100 + month) >= %s
  and (year * 100 + month) <= %s
"""


def _load_env_file(path: Path) -> dict:
    env = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def load_config(env_name: str) -> dict:
    env_file = Path(".env.prod") if env_name == "prod" else Path(".env.qa")
    print(f"Loading environment variables from '{env_file}'", flush=True)

    env_vars = _load_env_file(env_file)

    cfg = {
        "host": env_vars.get("host") or os.getenv("host"),
        "port": int(env_vars.get("port") or os.getenv("port") or 3306),
        "user": env_vars.get("user") or os.getenv("user"),
        "password": env_vars.get("password") or os.getenv("password"),
        "database": env_vars.get("database") or os.getenv("database") or "tx_data_service",
    }

    print(f"Configuration loaded for environment '{cfg}'", flush=True)
    return cfg


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
            print(f"Executing CLIENT_ACCOUNT_QUERY with last_id={last_id}, batch_size={batch_size}", flush=True)
            cur.execute(CLIENT_ACCOUNT_QUERY, (last_id, batch_size))
            rows = cur.fetchall()
            print(f"Query returned {len(rows)} rows", flush=True)
        if not rows:
            break
        for row in rows:
            yield row
            last_id = row["account_id"]


def fetch_months_batch(conn, client_account_pairs: list,
                       start_year: int, start_month: int,
                       end_year: int, end_month: int):
    if not client_account_pairs:
        return {}

    placeholders = ", ".join(["(%s, %s)"] * len(client_account_pairs))
    query = MONTHS_QUERY_BATCH.format(placeholders)

    flattened_pairs = [val for pair in client_account_pairs for val in pair]
    params = tuple(flattened_pairs) + (
        start_year * 100 + start_month,
        end_year * 100 + end_month,
    )

    with conn.cursor(dictionary=True) as cur:
        print(f"Executing MONTHS_QUERY_BATCH with {len(client_account_pairs)} pairs", flush=True)
        cur.execute(query, params)
        rows = cur.fetchall()
        print(f"MONTHS_QUERY_BATCH returned {len(rows)} rows", flush=True)

    if rows:
        first = rows[0]
        print(f"DEBUG: Row types - client_public_id: {type(first['client_public_id'])}, account_id: {type(first['account_id'])}, year: {type(first['year'])}, month: {type(first['month'])}", flush=True)

    results = {}
    for row in rows:
        key = (str(row["client_public_id"]), str(row["account_id"]))
        if key not in results:
            results[key] = set()
        results[key].add((int(row["year"]), int(row["month"])))
    return results


def expected_months(start_year: int, start_month: int,
                    end_year: int, end_month: int):
    expected = set()
    for y in range(start_year, end_year + 1):
        y_start = start_month if y == start_year else 1
        y_end = end_month if y == end_year else 12
        for m in range(y_start, y_end + 1):
            expected.add((y, m))
    return expected


def worker_task(pool: pooling.MySQLConnectionPool, semaphore: threading.Semaphore, exp: set,
                start_year: int, start_month: int, end_year: int, end_month: int,
                rows: list):
    batch_id = f"{rows[0]['account_id']}-{rows[-1]['account_id']}"
    print(f"[{batch_id}] Task started", flush=True)
    with semaphore:
        print(f"[{batch_id}] Acquired semaphore, getting connection from pool...", flush=True)
        try:
            conn = pool.get_connection()
            print(f"[{batch_id}] Connection acquired from pool.", flush=True)
        except (mysql.connector.Error, socket.gaierror) as err:
            print(f"[{batch_id}] Database connection error: {err}", flush=True)
            return []

        try:
            print(f"[{batch_id}] Fetching months for {len(rows)} accounts", flush=True)
            client_account_pairs = [(row["client_public_id"], row["account_id"]) for row in rows]
            actual_map = fetch_months_batch(
                conn,
                client_account_pairs,
                start_year,
                start_month,
                end_year,
                end_month,
            )
            print(f"[{batch_id}] Fetched months, processing...", flush=True)

            results = []
            for row in rows:
                client_public_id = str(row["client_public_id"])
                account_id = str(row["account_id"])
                yodlee_id = row.get("yodlee_id")
                actual = actual_map.get((client_public_id, account_id), set())
                missing = sorted(list(exp - actual))
                if missing:
                    results.append({
                        "client_public_id": client_public_id,
                        "account_id": account_id,
                        "yodlee_id": yodlee_id,
                        "missing_months": [{"year": y, "month": m} for (y, m) in missing],
                    })
            if not results and rows:
                sample_acc = str(rows[0]["account_id"])
                sample_client = str(rows[0]["client_public_id"])
                sample_actual = actual_map.get((sample_client, sample_acc), set())
                print(f"[{batch_id}] No missing records. Sample account {sample_acc} had {len(sample_actual)} unique months.", flush=True)
            else:
                print(f"[{batch_id}] Batch completed with {len(results)} missing records", flush=True)
            return results
        except mysql.connector.Error as err:
            print(f"Database error during query execution for batch of {len(rows)} accounts: {err}", flush=True)
            return []
        finally:
            conn.close()


def main():
    start_time = time.time()
    today = date.today()

    parser = argparse.ArgumentParser(description="Find missing monthly account summary data")
    parser.add_argument("--env", choices=["qa", "prod"], default="qa")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-year", type=int, default=today.year)
    parser.add_argument("--end-month", type=int, default=today.month)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--worker-batch-size", type=int, default=100)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--semaphore", type=int, default=8)
    parser.add_argument("--max-clients", type=int, default=None)
    parser.add_argument("--output", default=None)

    args = parser.parse_args()

    cfg = load_config(args.env)
    output_path = args.output
    if not output_path:
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        output_path = f"results/missing_months_{args.env}_{ts}.json"

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    exp = expected_months(args.start_year, args.start_month, args.end_year, args.end_month)
    print(f"Expected months count: {len(exp)}", flush=True)
    sample_exp = sorted(list(exp))[:5]
    print(f"Sample expected months: {sample_exp}", flush=True)

    semaphore = threading.Semaphore(args.semaphore)

    print(f"Initializing connection pool with size {args.workers}...", flush=True)
    pool = get_connection_pool(cfg, args.workers)
    print("Establishing main connection...", flush=True)
    main_conn = get_connection(cfg)
    print("Main connection established.", flush=True)

    try:
        total = 0
        futures = []

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            batch = []
            for row in fetch_client_accounts(main_conn, args.batch_size):
                total += 1
                if total <= 10 or total % 1000 == 0:
                    print(f"Fetched account {total}: ID {row['account_id']}", flush=True)
                batch.append(row)

                if len(batch) >= args.worker_batch_size:
                    futures.append(
                        executor.submit(
                            worker_task,
                            pool,
                            semaphore,
                            exp,
                            args.start_year,
                            args.start_month,
                            args.end_year,
                            args.end_month,
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
                        args.start_month,
                        args.end_year,
                        args.end_month,
                        batch,
                    )
                )

            print(f"All tasks submitted. Total accounts: {total}. Waiting for results...", flush=True)

            with open(output_path, "w", encoding="utf-8") as f_out:
                f_out.write("{\n")
                f_out.write(f'  "generated_at": {json.dumps(datetime.now().isoformat(timespec="seconds"))},\n')
                f_out.write(f'  "start_year": {args.start_year},\n')
                f_out.write(f'  "start_month": {args.start_month},\n')
                f_out.write(f'  "end_year": {args.end_year},\n')
                f_out.write(f'  "end_month": {args.end_month},\n')
                f_out.write('  "missing_months": [\n')

                first = True
                completed_batches = 0
                total_batches = len(futures)

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