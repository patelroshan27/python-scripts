import argparse
import asyncio
import json
import os
from datetime import datetime, timedelta

import aiohttp


def parse_arguments():
    parser = argparse.ArgumentParser(description="Yodlee Data Loader")
    parser.add_argument("--env", required=True, choices=["qa", "prod"], help="Environment to run the script against")
    return parser.parse_args()


def load_config(env):
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', f'{env}.json')
    try:
        with open(config_path) as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_path}")
        exit(1)
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from the configuration file at {config_path}")
        exit(1)


def load_accounts():
    accounts_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'accounts.json')
    try:
        with open(accounts_path) as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Accounts file not found at {accounts_path}")
        exit(1)
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from the accounts file at {accounts_path}")
        exit(1)


async def fetch_balance(session, account, config, semaphore):
    """Asynchronously fetches historical balance for a single account."""
    login_name = account.get("loginName")
    yodlee_account_id = account.get("yodleeAccountId")

    if not login_name or not yodlee_account_id:
        return {"loginName": login_name, "error": "Missing loginName or yodleeAccountId"}

    to_date = datetime.now()
    from_date = to_date - timedelta(days=30)
    params = {
        "loginName": login_name,
        "accountId": yodlee_account_id,
        "fromDate": from_date.strftime("%Y-%m-%d"),
        "toDate": to_date.strftime("%Y-%m-%d"),
        "interval": "D",
    }
    headers = {
        "apikey": config.get("api_key"),
        "Content-Type": "application/json",
    }

    url = config.get("api_url")
    retries = config.get("retries", 2)

    async with semaphore:
        for attempt in range(retries + 1):
            try:
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {"loginName": login_name, "data": data}
                    elif response.status == 404:
                        return {"loginName": login_name, "data": {}}  # No data found
                    else:
                        error_message = f"Attempt {attempt + 1}: Failed with status {response.status}"
                        if attempt == retries:
                            return {"loginName": login_name, "error": error_message}
                        await asyncio.sleep(1)  # wait 1s before retrying
            except aiohttp.ClientError as e:
                error_message = f"Attempt {attempt + 1}: ClientError: {e}"
                if attempt == retries:
                    return {"loginName": login_name, "error": error_message}
                await asyncio.sleep(1)  # wait 1s before retrying
    return {"loginName": login_name, "error": "Exhausted retries"}


async def main():
    args = parse_arguments()
    config = load_config(args.env)
    accounts_data = load_accounts()
    accounts = accounts_data.get("accounts", [])

    concurrency = config.get("concurrency", 5)
    semaphore = asyncio.Semaphore(concurrency)

    results_dir = os.path.join(os.path.dirname(__file__), '..', 'results')
    os.makedirs(results_dir, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_balance(session, account, config, semaphore) for account in accounts]
        results = await asyncio.gather(*tasks)

        for result in results:
            login_name = result.get("loginName")
            if not login_name:
                print(f"Skipping result with no loginName: {result}")
                continue

            output_path = os.path.join(results_dir, f'{login_name}.json')
            if "error" in result:
                print(f"Error fetching data for {login_name}: {result['error']}")
                error_data = {"error": result['error']}
                with open(output_path, 'w') as f:
                    json.dump(error_data, f, indent=2)
            else:
                with open(output_path, 'w') as f:
                    json.dump(result.get('data', {}), f, indent=2)
                print(f"Successfully fetched and stored data for {login_name}")


if __name__ == "__main__":
    asyncio.run(main())
