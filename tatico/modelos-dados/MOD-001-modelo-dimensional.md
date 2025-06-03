# MOD-001 - Modelo Dimensional

---
versão: 1.0.0
última_atualização: 2025-06-03
responsável: equipe.dados@m7investimentos.com.br
tags: [modelo-dados, dimensional, star-schema, datawarehouse]
---

## Objetivo

Documentar o modelo dimensional do Data Warehouse da M7 Investimentos, incluindo dimensões e fatos principais.

## Diagrama Conceitual

```
         ┌─────────────┐
         │ dim_pessoas │
         └──────┬──────┘
                │
┌───────────┐   │   ┌──────────────┐
│dim_tempo  ├───┼───┤ fact_captacao│
└───────────┘   │   └──────────────┘
                │
         ┌──────┴───────┐
         │dim_estruturas│
         └──────────────┘
```

## Dimensões

### dim_tempo (Calendário)
Dimensão temporal com hierarquia completa de datas.

| Campo | Tipo | Descrição | Chave |
|-------|------|-----------|--------|
| data_sk | INT64 | Surrogate key | PK |
| data | DATE | Data completa | AK |
| ano | INT64 | Ano (YYYY) | |
| mes | INT64 | Mês (1-12) | |
| dia | INT64 | Dia (1-31) | |
| trimestre | INT64 | Trimestre (1-4) | |
| dia_semana | STRING | Nome do dia | |
| dia_util | BOOLEAN | Indica se é dia útil | |

### dim_pessoas
Dimensão de pessoas (assessores, clientes, etc).

| Campo | Tipo | Descrição | Chave |
|-------|------|-----------|--------|
| pessoa_sk | INT64 | Surrogate key | PK |
| codigo_pessoa | STRING | Código único | AK |
| nome | STRING | Nome completo | |
| tipo_pessoa | STRING | Assessor/Cliente | |
| data_cadastro | DATE | Data de cadastro | |
| ativo | BOOLEAN | Status ativo | |

### dim_estruturas
Dimensão da estrutura organizacional.

| Campo | Tipo | Descrição | Chave |
|-------|------|-----------|--------|
| estrutura_sk | INT64 | Surrogate key | PK |
| codigo_estrutura | STRING | Código único | AK |
| nome_estrutura | STRING | Nome da estrutura | |
| tipo_estrutura | STRING | Regional/Escritório | |
| estrutura_pai | STRING | Código estrutura pai | |

## Fatos

### fact_captacao
Tabela fato de movimentações de captação.

| Campo | Tipo | Descrição | Chave |
|-------|------|-----------|--------|
| data_sk | INT64 | FK para dim_tempo | FK |
| pessoa_sk | INT64 | FK para dim_pessoas | FK |
| estrutura_sk | INT64 | FK para dim_estruturas | FK |
| valor_aplicacao | NUMERIC | Valor aplicado | |
| valor_resgate | NUMERIC | Valor resgatado | |
| valor_liquido | NUMERIC | Aplicação - Resgate | |
| quantidade_operacoes | INT64 | Número de operações | |

## Regras de Negócio

1. **Captação Líquida** = Aplicações - Resgates
2. **Dia Útil**: Excluir fins de semana e feriados nacionais
3. **Hierarquia Estrutura**: Escritório → Regional → Nacional

## Histórico de Evolução

- v1.0.0 (2025-06-03): Versão inicial do modelo

## Referências

- [ARQ-001 - Visão Geral Data Warehouse](../../estrategico/arquiteturas/ARQ-001-visao-geral-datawarehouse.md)
- [ETL-001 - Pipeline Bronze-Silver](../processos-etl/ETL-001-pipeline-bronze-silver.md)
