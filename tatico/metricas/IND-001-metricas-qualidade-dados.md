# IND-001 - Métricas de Qualidade de Dados

---
versão: 1.0.0
última_atualização: 2025-06-03
responsável: equipe.dados@m7investimentos.com.br
tags: [indicadores, qualidade-dados, kpi, monitoramento]
---

## Objetivo

Definir e documentar os indicadores de qualidade de dados do Data Warehouse para monitoramento e gestão contínua.

## Indicadores

### 1. Taxa de Completude

**Descrição**: Percentual de campos preenchidos versus total de campos obrigatórios.

**Fórmula**: 
```
(Campos Preenchidos / Total Campos Obrigatórios) × 100
```

**Detalhamento**:
- **Fonte de dados**: Todas as tabelas silver_*
- **Frequência**: Diária
- **Meta**: ≥ 99%
- **Tolerância**: 98% - 99%
- **Ação se desvio**: Análise de campos faltantes e correção na origem

### 2. Taxa de Conformidade

**Descrição**: Percentual de registros que atendem às regras de negócio definidas.

**Fórmula**:
```
(Registros Conformes / Total Registros) × 100
```

**Detalhamento**:
- **Fonte de dados**: Validações em silver_*
- **Frequência**: Diária
- **Meta**: ≥ 99.5%
- **Tolerância**: 99% - 99.5%
- **Ação se desvio**: Investigar regras violadas

### 3. Tempo de Processamento ETL

**Descrição**: Tempo total para processar pipeline Bronze → Silver → Gold.

**Fórmula**:
```
Timestamp Fim - Timestamp Início (em minutos)
```

**Detalhamento**:
- **Fonte de dados**: Logs de processamento
- **Frequência**: Por execução
- **Meta**: ≤ 120 minutos
- **Tolerância**: 120 - 150 minutos
- **Ação se desvio**: Otimização de queries

### 4. Taxa de Rejeição

**Descrição**: Percentual de registros rejeitados durante processamento.

**Fórmula**:
```
(Registros Rejeitados / Total Registros Processados) × 100
```

**Detalhamento**:
- **Fonte de dados**: Tabelas de quarentena
- **Frequência**: Diária
- **Meta**: ≤ 0.5%
- **Tolerância**: 0.5% - 1%
- **Ação se desvio**: Análise de padrões de rejeição

### 5. Disponibilidade do Data Warehouse

**Descrição**: Percentual de tempo que o DW está disponível para consultas.

**Fórmula**:
```
(Tempo Disponível / Tempo Total) × 100
```

**Detalhamento**:
- **Fonte de dados**: Monitoramento BigQuery
- **Frequência**: Mensal
- **Meta**: ≥ 99.5%
- **Tolerância**: 99% - 99.5%
- **Ação se desvio**: Análise de incidentes

## Dashboard

Os indicadores são visualizados em:
- **Ferramenta**: Looker Studio
- **URL**: [Dashboard Qualidade Dados](https://lookerstudio.google.com/...)
- **Atualização**: Real-time para métricas operacionais

## Responsáveis

- **Coleta**: Automática via BigQuery
- **Análise**: Equipe de Dados
- **Reporte**: Mensal para gestão

## Histórico de Metas

| Indicador | 2024 | 2025 Q1 | 2025 Q2 |
|-----------|------|---------|---------|
| Completude | 98% | 99% | 99% |
| Conformidade | 99% | 99.5% | 99.5% |
| Tempo ETL | 180min | 150min | 120min |
| Rejeição | 1% | 0.75% | 0.5% |
| Disponibilidade | 99% | 99.5% | 99.5% |

## Referências

- [ETL-001 - Pipeline Bronze-Silver](../processos-etl/ETL-001-pipeline-bronze-silver.md)
- [MOD-001 - Modelo Dimensional](../modelos-dados/MOD-001-modelo-dimensional.md)
