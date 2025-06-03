# ETL-001 - Pipeline Bronze-Silver

---
versão: 1.0.0
última_atualização: 2025-06-03
responsável: equipe.dados@m7investimentos.com.br
tags: [etl, bronze, silver, pipeline, transformação]
---

## Objetivo

Especificar o pipeline de transformação de dados da camada Bronze (dados brutos) para a camada Silver (dados limpos e padronizados).

## Fonte de Dados

- **Origem**: Tabelas bronze_* no dataset bronze
- **Conectores**: BigQuery native

## Transformações Aplicadas

1. **Limpeza de Dados**
   - Remoção de duplicatas
   - Tratamento de nulos
   - Padronização de formatos

2. **Validação de Qualidade**
   - Verificação de tipos de dados
   - Validação de ranges
   - Checagem de integridade referencial

3. **Padronização**
   - Nomenclatura de colunas
   - Formatos de data/hora
   - Encoding de caracteres

## Destino

- **Dataset**: silver
- **Padrão de nomenclatura**: silver_[tipo]_[entidade]
  - tipo: dim (dimensão) ou fact (fato)
  - entidade: nome da entidade de negócio

## Agendamento

- **Frequência**: Diária
- **Horário**: 02:00 AM BRT
- **Dependências**: Conclusão da ingestão Bronze

## Tratamento de Erros

- Registros com erro são direcionados para tabela de quarentena
- Notificação via email para equipe de dados
- Retry automático após 30 minutos (máximo 3 tentativas)

## Métricas de Qualidade

- Taxa de sucesso: > 99%
- Tempo de processamento: < 2 horas
- Registros rejeitados: < 1%

## Referências

- [ARQ-001 - Visão Geral Data Warehouse](../../estrategico/arquiteturas/ARQ-001-visao-geral-datawarehouse.md)
- [MOD-001 - Modelo Dimensional](../modelos-dados/MOD-001-modelo-dimensional.md)
