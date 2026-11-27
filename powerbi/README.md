# Dashboard de vendas (Power BI)

Relatório do Power BI Desktop sobre a camada **Gold** do DataFlow Pipeline, salvo no formato
**Power BI Project (.pbip)**: modelo em TMDL e relatório em PBIR, tudo em texto e versionável no Git.
Os dados não ficam no repositório. Eles são importados do Postgres quando você atualiza o relatório.

## Como abrir

1. Suba o warehouse e rode a DAG `sales_pipeline` pelo menos uma vez (a Gold precisa ter dados):
   `docker compose up -d`
2. Abra `powerbi/DataFlowSales.pbip` no Power BI Desktop.
3. Clique em **Atualizar** (Página Inicial). Na primeira vez ele pede credenciais do PostgreSQL:
   aba **Banco de dados**, usuário `DW_USER` e senha `DW_PASSWORD` do seu `.env`.
   Se perguntar sobre criptografia, aceite conectar sem ela (o Postgres local não tem SSL).
4. Se você mudou `DW_HOST_PORT` no `.env`, ajuste o parâmetro **Servidor**
   em Transformar dados > Gerenciar parâmetros (padrão `localhost:5433`).

## Modelo de dados

Star schema com relacionamentos um-para-muitos, filtro em direção única, das dimensões para a fato:

| Tabela | Papel | Chave |
|---|---|---|
| `fact_sales` | Fato, uma linha por item de nota fiscal | `date_key`, `product_key`, `customer_key`, `country_key` |
| `dim_date` | Calendário, marcado como tabela de datas | `date_key` |
| `dim_product` | Produto (`Código`, `Produto`) | `product_key` |
| `dim_customer` | Cliente (SCD tipo 1), `-1` = não identificado | `customer_key` |
| `dim_country` | País, `-1` = Unknown | `country_key` |

Decisões:
- **Importação** em vez de DirectQuery: cerca de 1 milhão de linhas cabem com folga na memória e o relatório fica rápido.
- `line_key` e `sales_key` não são importados: não servem para análise e só aumentariam o modelo.
- Valores em `Currency.Type` (decimal fixo) para somar dinheiro sem erro de ponto flutuante.
- Colunas de chave ficam ocultas; as dimensões usam nomes amigáveis em português.
- `dim_date` ganha a coluna `Mês` (primeiro dia do mês) no Power Query, usada como eixo contínuo.

## Medidas

Todas em `fact_sales`, organizadas em pastas.

| Medida | Regra | Confere com |
|---|---|---|
| Receita Bruta | soma de `line_total` sem cancelamentos | `gross_revenue` em `01_monthly_revenue.sql` |
| Receita Cancelada | valor das notas canceladas, como número positivo | `cancelled_revenue` (com sinal invertido) |
| Receita Líquida | soma de `line_total` (bruta menos cancelada) | `net_revenue` |
| % Receita Cancelada | cancelada / bruta | |
| Crescimento MoM % | líquida contra o mês anterior | `net_growth_pct` |
| % da Receita Líquida | participação ignorando o filtro de produto | `revenue_share_pct` em `02_top_products.sql` |
| Pedidos | notas distintas sem cancelamento | `orders` |
| % Notas Canceladas | notas canceladas / notas | `03_cancellation_rate_by_country.sql` |
| % Notas Canceladas (100+ notas) | a mesma taxa, só com 100 notas ou mais | o `HAVING` da mesma consulta |
| Ticket Médio | receita bruta / pedidos | |
| Unidades Líquidas | soma de `quantity` | `units_sold` |
| Clientes Ativos | clientes identificados com compra no período | |
| Clientes Novos / Recorrentes | primeira compra dentro ou antes do período | `05_new_vs_returning_customers.sql` |
| Receita por Cliente | receita bruta de identificados / clientes ativos | |

## Páginas

1. **Visão Geral**: KPIs, receita bruta e líquida por mês, top 5 países, cancelamentos por mês.
2. **Produtos**: top 10 por receita e por unidades, tabela com todos os produtos.
3. **Países**: top 10 por receita, taxa de cancelamento (países com 100+ notas), resumo por país.
4. **Clientes**: clientes ativos, novos e recorrentes por mês, % de recorrência e ranking de clientes.

Dezembro de 2011 tem só 9 dias na base: a queda do último mês é corte dos dados, não queda de vendas.
