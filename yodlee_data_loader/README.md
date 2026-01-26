# Yodlee Data Loader

This project is a Python script to fetch historical balance data from the Yodlee API for a list of accounts, and store the results in JSON files. It supports different environments (QA, Prod) through configuration files.

## Setup

1.  **Install Dependencies:**
    ```bash
    pip install -r yodlee_data_loader/requirements.txt
    ```

2.  **Configure API Keys:**
    Open the configuration files in `yodlee_data_loader/config/` (`qa.json` and `prod.json`) and replace the placeholder `"your-qa-api-key"` and `"your-prod-api-key"` with your actual API keys.

## Usage

To run the script, you need to specify the environment using the `--env` flag.

### QA Environment

```bash
python3 yodlee_data_loader/scripts/loader.py --env qa
```

### Production Environment

```bash
python3 yodlee_data_loader/scripts/loader.py --env prod
```
