---
título: Modelo de Views de Performance Gold - Consumo e Análise
tipo: MOD
código: MOD-IND-006
versão: 1.0.0
data_criação: 2025-01-18
última_atualização: 2025-01-18
próxima_revisão: 2025-07-18
responsável: bruno.chiaramonti@multisete.com
aprovador: arquitetura.dados@m7investimentos.com.br
tags: [modelo, gold, views, performance, pivot, ranking, dashboard]
status: aprovado
confidencialidade: interno
---

# MOD-IND-006 - Views de Performance Gold

## 1. Objetivo

Documentar as views de consumo da camada Gold que transformam o modelo EAV (Entity-Attribute-Value) da tabela `gold.card_metas` em formatos otimizados para diferentes casos de uso: análises pivotadas, scores ponderados, rankings comparativos e dashboards executivos. Estas views são a interface principal entre os dados calculados e as ferramentas de visualização/análise.

## 2. Escopo e Contexto

### 2.1 Domínio de Negócio
- **Área**: Analytics / BI / Gestão de Performance
- **Processos suportados**: 
  - Dashboards executivos de performance
  - Rankings e benchmarking entre assessores
  - Análise de tendências e séries temporais
  - Cálculo de comissionamento variável
  - Exportação de dados para ML/AI
- **Stakeholders**: 
  - Diretoria e C-Level
  - Gestores de Performance
  - Analistas de BI
  - Assessores (self-service)
  - Data Scientists

### 2.2 Contexto Técnico
- **Tipo**: Views SQL (não materializadas)
- **Performance**: Otimizadas para DirectQuery
- **Plataforma**: SQL Server 2019+
- **Schema**: gold
- **Padrão**: Views especializadas por caso de uso

## 3. Arquitetura das Views

### 3.1 Hierarquia e Dependências
```
gold.card_metas (Tabela EAV Base)
    │
    ├─── vw_card_metas_pivot ─────────── Transforma EAV em colunar
    │
    ├─── vw_card_metas_weighted_score ── Calcula scores consolidados
    │     │
    │     └─── vw_card_metas_ranking ──── Rankings baseados em score
    │
    ├─── vw_card_metas_dashboard ──────── Visão completa para BI
    │
    └─── vw_card_metas_serie_temporal ─── Análise temporal/tendências
```

### 3.2 Matriz de Casos de Uso
| View | Caso de Uso Principal | Usuários | Performance |
|------|----------------------|----------|-------------|
| pivot | Relatórios tabulares | Analistas | < 1s |
| weighted_score | Score cards executivos | Gestão | < 500ms |
| ranking | Competições/gamificação | Todos | < 2s |
| dashboard | Power BI/Tableau | BI Team | < 2s |
| serie_temporal | Análise de tendências | Analytics | < 3s |

## 4. Detalhamento das Views

### 4.1 VIEW: gold.vw_card_metas_pivot

**Propósito**: Transformar o modelo EAV em formato colunar tradicional, com uma coluna para cada indicador principal.

**Estrutura de Saída**:
| Coluna | Tipo | Descrição | Exemplo |
|--------|------|-----------|---------|
| period_start | DATE | Início do período | "2025-01-01" |
| codigo_assessor_crm | VARCHAR(20) | ID do assessor | "AAI001" |
| nome_pessoa | VARCHAR(200) | Nome completo | "João Silva" |
| equipe_comercial | VARCHAR(100) | Equipe/squad | "Elite Sul" |
| captacao_liquida | DECIMAL(18,4) | Valor realizado | 450000.00 |
| captacao_liquida_meta | DECIMAL(18,4) | Meta do período | 500000.00 |
| captacao_liquida_ating | DECIMAL(5,2) | % atingimento | 90.00 |
| captacao_liquida_ponderado | DECIMAL(5,2) | Contribuição score | 36.00 |
| [outros_indicadores...] | ... | Padrão repetido | ... |

**SQL Pattern**:
```sql
MAX(CASE WHEN attribute_code = 'INDICATOR_X' THEN realized_value END) as indicator_x,
MAX(CASE WHEN attribute_code = 'INDICATOR_X' THEN target_value END) as indicator_x_meta,
MAX(CASE WHEN attribute_code = 'INDICATOR_X' THEN achievement_percentage END) as indicator_x_ating,
MAX(CASE WHEN attribute_code = 'INDICATOR_X' THEN weighted_achievement END) as indicator_x_ponderado
```

**Características**:
- Facilita consumo por ferramentas que não suportam EAV
- Uma linha por assessor/período
- Colunas fixas (requer manutenção ao adicionar indicadores)
- Filtro automático para indicator_type = 'CARD'

### 4.2 VIEW: gold.vw_card_metas_weighted_score

**Propósito**: Calcular e consolidar o score total ponderado de cada assessor, com estatísticas de performance.

**Estrutura de Saída**:
| Coluna | Tipo | Descrição | Cálculo |
|--------|------|-----------|---------|
| codigo_assessor_crm | VARCHAR(20) | ID assessor (CRM real) | entity_id |
| nome_pessoa | VARCHAR(200) | Nome | JOIN dim_pessoas |
| tipo_pessoa | VARCHAR(20) | Classificação | Via CurrentStructure CTE |
| equipe_comercial | VARCHAR(100) | Equipe | Via CurrentStructure CTE |
| regional | VARCHAR(100) | Regional | Via CurrentStructure CTE |
| score_ponderado | DECIMAL(5,2) | Score total | SUM(weighted_achievement) |
| classificacao_performance | VARCHAR(20) | Faixa performance | CASE score |
| percentil_periodo | DECIMAL(5,1) | Posição relativa | PERCENT_RANK() |
| qtd_indicadores_card | INT | Total CARD | COUNT(type='CARD') |
| soma_pesos | DECIMAL(5,2) | Validação | SUM(weight) |
| qtd_superado | INT | Metas superadas | COUNT(status='SUPERADO') |
| qtd_atingido | INT | Metas atingidas | COUNT(status='ATINGIDO') |
| media_atingimento | DECIMAL(5,2) | Média simples | AVG(achievement_%) |
| amplitude_performance | DECIMAL(5,2) | Max - Min | Consistência |

**Regras de Classificação**:
```sql
CASE 
    WHEN score_ponderado >= 120 THEN 'EXCELENTE'
    WHEN score_ponderado >= 100 THEN 'BOM'
    WHEN score_ponderado >= 80 THEN 'REGULAR'
    ELSE 'ABAIXO'
END
```

**Filtros Importantes**:
- Apenas registros com soma_pesos >= 99% (validação)
- Apenas is_calculated = 1
- Window functions para percentil

### 4.3 VIEW: gold.vw_card_metas_ranking

**Propósito**: Gerar múltiplos rankings simultâneos para análise comparativa e gamificação.

**Rankings Calculados**:
| Ranking | Escopo | Ordenação | Window Partition |
|---------|--------|-----------|------------------|
| ranking_indicador | Por indicador | achievement_% DESC | period, indicator |
| ranking_equipe | Dentro da equipe | achievement_% DESC | period, indicator, team |
| ranking_geral | Score total | score_ponderado DESC | period |
| ranking_tipo | Por tipo assessor | score_ponderado DESC | period, tipo |

**Estrutura de Saída Adicional**:
| Coluna | Descrição | Exemplo |
|--------|-----------|---------|
| posicao_indicador | "X/Y" formato | "3/150" |
| quartil_indicador | Classificação quartil | "PRIMEIRO QUARTIL" |
| total_participantes | Denominador ranking | 150 |

**Lógica de Quartil**:
```sql
CASE 
    WHEN ranking <= 3 THEN 'TOP 3'
    WHEN ranking <= 10 THEN 'TOP 10'
    WHEN ranking <= CEILING(total * 0.25) THEN 'PRIMEIRO QUARTIL'
    WHEN ranking <= CEILING(total * 0.50) THEN 'SEGUNDO QUARTIL'
    WHEN ranking <= CEILING(total * 0.75) THEN 'TERCEIRO QUARTIL'
    ELSE 'QUARTO QUARTIL'
END
```

### 4.4 VIEW: gold.vw_card_metas_dashboard

**Propósito**: View completa e desnormalizada otimizada para ferramentas de BI com todos os dados necessários para dashboards.

**Características Especiais**:
- Inclui dados temporais (meses_empresa)
- Flag is_periodo_atual para filtros rápidos
- Cálculos MoM/YoY via LAG()
- Agregações pré-calculadas
- Star rating system (0-5 estrelas)

**Campos Exclusivos**:
| Campo | Descrição | Uso BI |
|-------|-----------|--------|
| periodo_texto | "Janeiro/2025" | Labels |
| is_periodo_atual | 0/1 | Filtro padrão |
| meses_empresa | Senioridade | Segmentação |
| estrelas_indicador | 0-5 scale | Visualização |
| realizado_mes_anterior | LAG(1) | Trend arrows |
| realizado_ano_anterior | LAG(12) | YoY analysis |
| qtd_erros | Qualidade | Alertas |

**Otimizações BI**:
- Tipos de dados compatíveis com DirectQuery
- Nomenclatura user-friendly
- Agregações pré-computadas
- Índices alinhados com filter context

### 4.5 VIEW: gold.vw_card_metas_serie_temporal

**Propósito**: Análise de séries temporais com médias móveis, tendências e variações percentuais.

**Cálculos Temporais**:
| Métrica | Janela | Descrição |
|---------|--------|-----------|
| media_movel_3m | 3 meses | Suavização curto prazo |
| media_movel_12m | 12 meses | Tendência longo prazo |
| variacao_mom_pct | Mês anterior | Growth rate mensal |
| variacao_yoy_pct | Ano anterior | Growth rate anual |
| tendencia_3m | Classificação | CRESCENTE/ESTAVEL/DECRESCENTE |
| ranking_historico | All time | Melhor/pior mês histórico |

**SQL Pattern para Médias Móveis**:
```sql
AVG(realized_value) OVER (
    PARTITION BY entity_id, attribute_code 
    ORDER BY period_start 
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
) as media_movel_3m
```

## 5. Performance e Otimização

### 5.1 Estratégias de Performance

| Estratégia | Implementação | Impacto |
|------------|---------------|---------|
| Índices base | 6 índices em card_metas | Base queries < 100ms |
| Estatísticas | Weekly stats update | Planos otimizados |
| Filter pushdown | Predicates nas CTEs | Reduz I/O |
| Column reduction | SELECT específico | Menos memória |
| NOLOCK hints | Onde apropriado | Evita blocks |

### 5.2 Benchmarks de Performance

| View | Registros | Tempo Médio | Máximo Aceitável |
|------|-----------|-------------|------------------|
| pivot | 500 | 200ms | 1s |
| weighted_score | 500 | 150ms | 500ms |
| ranking | 10.000 | 800ms | 2s |
| dashboard | 10.000 | 1.2s | 2s |
| serie_temporal | 60.000 | 2.1s | 3s |

### 5.3 Query Hints Recomendados
```sql
-- Para Power BI DirectQuery
OPTION (MAXDOP 4, RECOMPILE)

-- Para exports grandes
OPTION (MAXDOP 8, FAST 1000)

-- Para real-time dashboards
WITH (NOLOCK) -- onde apropriado
```

## 6. Integração com Ferramentas

### 6.1 Power BI

**Configurações Recomendadas**:
- Import Mode: `vw_card_metas_dashboard` (snapshot diário)
- DirectQuery: Outras views (real-time)
- Relationships: Criar no modelo, não no DB
- RLS: Implementar no Power BI layer

**DAX Measures Sugeridas**:
```dax
Score Médio = AVERAGE([score_ponderado])
Taxa Atingimento = COUNTROWS(FILTER(Table, [achievement_status] = "ATINGIDO")) / COUNTROWS(Table)
Ranking Dinâmico = RANKX(ALL(Table[assessor]), [Score Total], , DESC)
```

### 6.2 Excel/Power Query

```powerquery
let
    Source = Sql.Database("server", "M7Medallion"),
    GoldScore = Source{[Schema="gold",Item="vw_card_metas_weighted_score"]}[Data],
    Filtered = Table.SelectRows(GoldScore, each [period_start] = Date.StartOfMonth(Date.From(DateTime.LocalNow())))
in
    Filtered
```

### 6.3 Python/Pandas

```python
import pandas as pd
import sqlalchemy

# Connection string
engine = sqlalchemy.create_engine('mssql+pyodbc://...')

# Query com chunks para grandes volumes
query = """
SELECT * FROM gold.vw_card_metas_serie_temporal
WHERE period_start >= DATEADD(MONTH, -12, GETDATE())
"""

df = pd.read_sql(query, engine, chunksize=10000)
```

## 7. Segurança e Governança

### 7.1 Permissões

| Role | Views Permitidas | Restrições |
|------|------------------|------------|
| assessor_role | ranking (próprio) | WHERE entity_id = USER |
| gestor_role | Todas (equipe) | WHERE equipe IN (...) |
| executive_role | Todas | Sem restrição |
| bi_role | Todas (read-only) | Sem DML |

### 7.2 Row Level Security (RLS)

```sql
-- Exemplo de política RLS
CREATE FUNCTION dbo.fn_security_performance(@entity_id VARCHAR(20))
RETURNS TABLE
WITH SCHEMABINDING
AS RETURN
    SELECT 1 AS allowed
    WHERE @entity_id = USER_NAME()
       OR IS_MEMBER('PerformanceManager') = 1;

-- Aplicar nas views
ALTER VIEW gold.vw_card_metas_dashboard
WITH SCHEMABINDING
AS
SELECT * FROM gold.card_metas
WHERE dbo.fn_security_performance(entity_id) = 1;
```

## 8. Manutenção e Evolução

### 8.1 Checklist de Manutenção Mensal

- [ ] Verificar performance das views (DMVs)
- [ ] Atualizar estatísticas
- [ ] Revisar novos indicadores para vw_pivot
- [ ] Validar cálculos de ranking
- [ ] Testar integração Power BI
- [ ] Documentar mudanças

### 8.2 Processo para Novos Indicadores

1. **Adicionar em vw_card_metas_pivot**:
   ```sql
   MAX(CASE WHEN attribute_code = 'NEW_IND' THEN realized_value END) as new_ind,
   -- Repetir padrão para meta, ating, ponderado
   ```

2. **Sem mudanças necessárias**:
   - weighted_score (automático)
   - ranking (automático)
   - dashboard (automático)
   - serie_temporal (automático)

3. **Testar impacto performance**
4. **Atualizar documentação**

## 9. Troubleshooting

### 9.1 Problemas Comuns

| Sintoma | Causa Provável | Diagnóstico | Solução |
|---------|----------------|-------------|---------|
| View lenta | Stats desatualizadas | sp_BlitzCache | UPDATE STATISTICS |
| Valores NULL | Período sem dados | Check card_metas | Validar processing |
| Ranking errado | Empates | ORDER BY secundário | Adicionar tiebreaker |
| Pivot incompleto | Novo indicador | Query vw definition | Atualizar view |

### 9.2 Queries de Diagnóstico

```sql
-- Performance das views
SELECT 
    v.name as view_name,
    qs.execution_count,
    qs.total_elapsed_time / 1000000.0 as total_seconds,
    qs.total_elapsed_time / qs.execution_count / 1000.0 as avg_ms
FROM sys.views v
JOIN sys.dm_exec_query_stats qs 
    ON qs.object_id = v.object_id
WHERE v.schema_id = SCHEMA_ID('gold')
ORDER BY avg_ms DESC;

-- Validar dados base
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT entity_id) as unique_entities,
    COUNT(DISTINCT attribute_code) as unique_indicators,
    SUM(CASE WHEN has_error = 1 THEN 1 ELSE 0 END) as error_count
FROM gold.card_metas
WHERE period_start = EOMONTH(DATEADD(MONTH, -1, GETDATE()));
```

## 10. Exemplos de Uso

### 10.1 Dashboard Executivo
```sql
-- Top 10 performers do mês
SELECT TOP 10 
    nome_pessoa,
    equipe_comercial,
    score_ponderado,
    classificacao_performance,
    qtd_superado,
    qtd_atingido
FROM gold.vw_card_metas_weighted_score
WHERE period_start = EOMONTH(DATEADD(MONTH, -1, GETDATE()))
ORDER BY score_ponderado DESC;
```

### 10.2 Análise Comparativa
```sql
-- Ranking de captação por equipe
SELECT 
    equipe_comercial,
    indicador_nome,
    nome_pessoa,
    realized_value,
    ranking_equipe,
    posicao_equipe,
    quartil_indicador
FROM gold.vw_card_metas_ranking
WHERE period_start = '2025-01-01'
  AND indicador = 'CAPT_LIQ'
  AND ranking_equipe <= 5
ORDER BY equipe_comercial, ranking_equipe;
```

### 10.3 Tendência Individual
```sql
-- Evolução últimos 12 meses
SELECT 
    period_start,
    indicador,
    realized_value,
    media_movel_3m,
    variacao_mom_pct,
    tendencia_3m,
    destaque_historico
FROM gold.vw_card_metas_serie_temporal
WHERE codigo_assessor_crm = 'AAI001'
  AND indicador = 'CAPT_LIQ'
  AND period_start >= DATEADD(MONTH, -12, GETDATE())
ORDER BY period_start;
```

## 11. Referências

### 11.1 Scripts SQL
- [QRY-IND-007-create_gold_performance_views.sql](../../operacional/queries/gold/QRY-IND-007-create_gold_performance_views.sql)

### 11.2 Documentação Relacionada
- [MOD-IND-005 - Card Metas Gold](MOD-IND-005-card-metas-gold.md)
- [ARQ-IND-001 - Performance Tracking System](../../estrategico/arquiteturas/ARQ-IND-001-performance-tracking-system.md)

### 11.3 Recursos Externos
- [SQL Server Query Store Best Practices](https://docs.microsoft.com/en-us/sql/relational-databases/performance/query-store-best-practices)
- [Power BI DirectQuery Guidance](https://docs.microsoft.com/en-us/power-bi/connect-data/desktop-directquery-about)

## 12. Histórico de Mudanças

| Versão | Data | Autor | Descrição |
|--------|------|-------|-----------|
| 1.0.0 | 2025-01-18 | bruno.chiaramonti | Criação inicial do documento |
| 1.1.0 | 2025-01-20 | bruno.chiaramonti | Documentação da integração organizacional via CTEs e entity_id com CRM real |

---

**Documento criado por**: Bruno Chiaramonti  
**Data**: 2025-01-18  
**Versão**: 1.1.0  
**Status**: Aprovado