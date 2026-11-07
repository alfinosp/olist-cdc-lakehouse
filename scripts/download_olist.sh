#!/usr/bin/env bash
# Download the Olist Brazilian E-Commerce dataset (CC BY-NC-SA 4.0) into data/raw.
# Needs the Kaggle CLI and an API token: ~/.kaggle/access_token, ~/.kaggle/kaggle.json,
# or the KAGGLE_API_TOKEN environment variable.
set -euo pipefail

RAW_DIR="${RAW_DIR:-data/raw}"
mkdir -p "$RAW_DIR"

if [[ -f "$RAW_DIR/olist_orders_dataset.csv" ]]; then
    echo "Olist CSV files already in $RAW_DIR, nothing to do."
    exit 0
fi

if ! command -v kaggle >/dev/null 2>&1; then
    echo "Kaggle CLI not found. Run 'make venv' first (it installs kaggle into .venv)." >&2
    exit 1
fi

kaggle datasets download -d olistbr/brazilian-ecommerce -p "$RAW_DIR" --unzip
ls -lh "$RAW_DIR"
