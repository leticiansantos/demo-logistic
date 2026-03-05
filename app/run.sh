#!/usr/bin/env bash
# Roda o dashboard (Streamlit). Execute a partir da raiz do repo: ./app/run.sh
cd "$(dirname "$0")"
pip install -q -r requirements.txt
streamlit run dashboard.py --server.port 8501 --server.headless true
