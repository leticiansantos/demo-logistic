#!/usr/bin/env bash
# Motz Demo – inicia backend FastAPI + frontend React (dev mode)
# Execute a partir da raiz do repo: ./app/run.sh
set -e
cd "$(dirname "$0")"

echo "Instalando dependencias Python..."
pip install -q -r requirements.txt

echo "Instalando dependencias Node.js..."
(cd frontend && npm install --silent)

echo ""
echo "Iniciando servidores:"
echo "  Backend  -> http://localhost:8000"
echo "  Frontend -> http://localhost:5173"
echo ""
echo "Pressione Ctrl+C para encerrar."
echo ""

# Inicia FastAPI em background
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload &
BACKEND_PID=$!

# Inicia Vite (foreground)
(cd frontend && npm run dev)

# Encerra backend ao sair
kill "$BACKEND_PID" 2>/dev/null || true
