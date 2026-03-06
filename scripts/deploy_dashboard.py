#!/usr/bin/env python3
"""
Faz o deploy do dashboard Motz Demo no Databricks (AI/BI Lakeview).

Requisitos:
  - databricks-sdk: pip install databricks-sdk
  - Autenticação configurada: databricks configure ou variáveis DATABRICKS_HOST + DATABRICKS_TOKEN

Uso:
  python scripts/deploy_dashboard.py
  python scripts/deploy_dashboard.py --parent-path "/Users/meu_email@empresa.com"
"""
from __future__ import annotations

import argparse
import json
import os
import sys

def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy do dashboard Motz Demo no Databricks")
    parser.add_argument(
        "--parent-path",
        default=os.environ.get("DASHBOARD_PARENT_PATH"),
        help="Pasta do workspace onde o dashboard será criado (default: sua pasta de usuário /Users/<você>)",
    )
    parser.add_argument(
        "--warehouse-id",
        default=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
        help="ID do SQL warehouse (opcional; se omitido, o SDK pode usar um padrão)",
    )
    parser.add_argument("--no-publish", action="store_true", help="Não publicar o dashboard após criar")
    args = parser.parse_args()

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dashboard_path = os.path.join(repo_root, "dashboards", "motz_demo_dashboard.json")
    if not os.path.isfile(dashboard_path):
        print(f"Arquivo não encontrado: {dashboard_path}", file=sys.stderr)
        sys.exit(1)

    with open(dashboard_path, encoding="utf-8") as f:
        payload = json.load(f)
    serialized = json.dumps(payload)

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.dashboards import Dashboard
    except ImportError:
        print("Instale o Databricks SDK: pip install databricks-sdk", file=sys.stderr)
        sys.exit(1)

    try:
        w = WorkspaceClient()
    except Exception as e:
        print("Erro ao conectar ao Databricks:", e, file=sys.stderr)
        print("\nConfigure a autenticação:", file=sys.stderr)
        print("  1. No workspace: User Settings → Developer → Access tokens → Generate new token", file=sys.stderr)
        print("  2. No terminal: databricks configure --host https://<seu-workspace>.cloud.databricks.com --token <token>", file=sys.stderr)
        print("  Ou defina: export DATABRICKS_HOST=... e export DATABRICKS_TOKEN=...", file=sys.stderr)
        sys.exit(1)

    display_name = "Dashboard de Negócios – Motz Demo"

    # Pasta de destino: padrão = pasta do usuário atual (sempre existe)
    parent_path = args.parent_path
    if not parent_path:
        try:
            me = w.current_user.me()
            parent_path = f"/Users/{me.user_name}"
            print(f"Usando pasta do usuário: {parent_path}")
        except Exception:
            parent_path = "/Workspace"
    parent_path = parent_path.rstrip("/")

    # Obter warehouse se não informado
    warehouse_id = args.warehouse_id
    if not warehouse_id:
        try:
            whs = list(w.warehouses.list())
        except Exception as e:
            if "Invalid Authorization" in str(e) or "PermissionDenied" in type(e).__name__:
                print("Token inválido ou expirado.", file=sys.stderr)
                print("\nPara corrigir:", file=sys.stderr)
                print("  1. Acesse o workspace no navegador e gere um novo token:", file=sys.stderr)
                print("     User Settings (ícone de usuário) → Developer → Access tokens → Generate new token", file=sys.stderr)
                print("  2. Configure de novo: databricks configure --host <url-do-workspace> --token <novo-token>", file=sys.stderr)
                print("  3. Ou use: export DATABRICKS_TOKEN=<seu-novo-token>", file=sys.stderr)
                print("\nSe já tiver o ID do SQL warehouse, use: --warehouse-id <id>", file=sys.stderr)
                sys.exit(1)
            raise
        if not whs:
            print("Nenhum SQL warehouse encontrado. Crie um ou passe --warehouse-id.", file=sys.stderr)
            sys.exit(1)
        # Preferir warehouse em execução
        running = [wh for wh in whs if getattr(wh, "status", None) == "RUNNING"]
        warehouse_id = (running[0] if running else whs[0]).id
        print(f"Usando warehouse: {warehouse_id}")

    dashboard = Dashboard(
        display_name=display_name,
        parent_path=parent_path,
        serialized_dashboard=serialized,
        warehouse_id=warehouse_id,
    )

    # Verificar se já existe (mesmo nome) → atualizar em vez de criar
    existing_id = None
    try:
        for d in w.lakeview.list():
            if getattr(d, "display_name", "") == display_name:
                existing_id = d.dashboard_id
                break
    except Exception as e:
        if "Invalid Authorization" in str(e) or "PermissionDenied" in type(e).__name__:
            print("Token inválido ou expirado.", file=sys.stderr)
            sys.exit(1)
        raise

    try:
        if existing_id:
            created = w.lakeview.update(existing_id, dashboard)
            print(f"Dashboard atualizado: {created.dashboard_id}")
        else:
            created = w.lakeview.create(dashboard)
            print(f"Dashboard criado: {created.dashboard_id}")
    except Exception as e:
        err_name = type(e).__name__
        if "Invalid Authorization" in str(e) or "PermissionDenied" in err_name:
            print("Token inválido ou expirado.", file=sys.stderr)
            print("Gere um novo token: User Settings → Developer → Access tokens.", file=sys.stderr)
            sys.exit(1)
        if "ResourceAlreadyExists" in err_name or "already exists" in str(e):
            # Criar falhou por já existir; buscar e atualizar
            for d in w.lakeview.list():
                if getattr(d, "display_name", "") == display_name:
                    created = w.lakeview.update(d.dashboard_id, dashboard)
                    print(f"Dashboard já existia; atualizado: {created.dashboard_id}")
                    break
            else:
                print("Dashboard com esse nome já existe em outro caminho. Use outro --parent-path ou renomeie no workspace.", file=sys.stderr)
                sys.exit(1)
        elif "ResourceDoesNotExist" in err_name or "doesn't exist" in str(e):
            print(f"Pasta não existe: {parent_path}", file=sys.stderr)
            print("Use: --parent-path \"/Users/<seu-user>\"", file=sys.stderr)
            sys.exit(1)
        else:
            raise

    print(f"Caminho: {getattr(created, 'path', created.parent_path)}")

    if not args.no_publish:
        w.lakeview.publish(dashboard_id=created.dashboard_id, warehouse_id=warehouse_id)
        print("Dashboard publicado.")
    host = os.environ.get("DATABRICKS_HOST", getattr(w.config, "host", ""))
    if host:
        print(f"URL: https://{host}/sql/dashboards/{created.dashboard_id}")

if __name__ == "__main__":
    main()
