# ARQ-001 - Visão Geral Data Warehouse

---
versão: 1.0.0
última_atualização: 2025-06-03
responsável: equipe.dados@m7investimentos.com.br
tags: [arquitetura, datawarehouse, visao-geral, estratégico]
---

## Objetivo

Documentar a arquitetura técnica do Data Warehouse da M7 Investimentos, seus componentes principais e suas interações em nível estratégico.

## Visão Geral

O Data Warehouse da M7 Investimentos segue uma arquitetura de camadas (Bronze, Silver, Gold) para processamento e organização de dados.

### Componentes Principais

1. **Camada Bronze**: Dados brutos vindos dos sistemas fonte
2. **Camada Silver**: Dados limpos e padronizados
3. **Camada Gold**: Dados agregados e prontos para consumo

## Fluxo de Dados

```
Sistemas Fonte → Bronze → Silver → Gold → Consumo
```

## Decisões Arquiteturais

- **Tecnologia**: BigQuery (Google Cloud Platform)
- **Orquestração**: A definir
- **Padrão de nomenclatura**: Prefixos por camada (bronze_, silver_, gold_)

## Requisitos Não-Funcionais

- **Escalabilidade**: Suportar crescimento de 200% ao ano
- **Performance**: Queries gold < 5 segundos
- **Disponibilidade**: 99.5% uptime

## Referências

- [MOD-001 - Modelo Dimensional](../../tatico/modelos-dados/MOD-001-modelo-dimensional.md)
- [ETL-001 - Pipeline Bronze-Silver](../../tatico/processos-etl/ETL-001-pipeline-bronze-silver.md)
