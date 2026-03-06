# Workspace Databricks – Demo Logística

| Campo        | Valor |
|-------------|--------|
| **Host**    | `https://fevm-leticia-santos-classic-stable.cloud.databricks.com` |
| **Workspace ID** | `7474658265676932` |

## Configurar autenticação (token)

```bash
databricks configure --host https://fevm-leticia-santos-classic-stable.cloud.databricks.com --token
```

Ou com variáveis de ambiente:

```bash
export DATABRICKS_HOST="https://fevm-leticia-santos-classic-stable.cloud.databricks.com"
export DATABRICKS_TOKEN="<seu-token>"
```

O token é gerado em: **User Settings → Developer → Access tokens → Generate new token**.

## Deploy do dashboard

Com o token configurado:

```bash
source .venv/bin/activate
python scripts/deploy_dashboard.py
```
