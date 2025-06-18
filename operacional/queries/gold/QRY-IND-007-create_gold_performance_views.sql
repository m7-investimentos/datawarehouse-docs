-- ==============================================================================
-- QRY-IND-007-create_gold_performance_views
-- ==============================================================================
-- Tipo: Views - Criação de Views de Consumo
-- Versão: 1.0.0
-- Última atualização: 2025-01-18
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [gold, views, performance, pivot, ranking, dashboard]
-- Status: aprovado
-- Banco de Dados: SQL Server 2019+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criar views de consumo da camada Gold para transformar o modelo EAV
em formatos mais amigáveis para consumo por Power BI, aplicações e análises.

Casos de uso:
- View Pivot: Transforma EAV em formato colunar (uma coluna por indicador)
- View Weighted Score: Calcula score total ponderado por pessoa
- View Ranking: Gera rankings por período e indicador
- View Dashboard: Visão consolidada para Power BI com todas as métricas

Frequência de execução: Views são consultadas em tempo real
Performance esperada: < 2 segundos para queries típicas
*/

USE M7Medallion;
GO

-- ==============================================================================
-- 2. VIEW 1: CARD METAS PIVOT
-- ==============================================================================
-- Drop view se existir
IF OBJECT_ID('gold.vw_card_metas_pivot', 'V') IS NOT NULL
    DROP VIEW gold.vw_card_metas_pivot;
GO

-- Criar view pivot (transforma EAV em colunar)
CREATE VIEW gold.vw_card_metas_pivot
AS
WITH IndicatorPivot AS (
    SELECT 
        cm.period_start,
        cm.period_end,
        cm.entity_id as codigo_assessor_crm,
        p.nome_pessoa,
        p.tipo_pessoa,
        p.equipe_comercial,
        p.regional,
        cm.attribute_code,
        cm.target_value,
        cm.realized_value,
        cm.achievement_percentage,
        cm.weighted_achievement,
        cm.achievement_status
    FROM gold.card_metas cm
    LEFT JOIN silver.dim_pessoas p ON cm.entity_id = p.codigo_assessor_crm
    WHERE cm.indicator_type = 'CARD'
      AND cm.is_calculated = 1
)
SELECT 
    period_start,
    period_end,
    codigo_assessor_crm,
    nome_pessoa,
    tipo_pessoa,
    equipe_comercial,
    regional,
    
    -- Indicadores financeiros
    MAX(CASE WHEN attribute_code = 'CAPT_LIQ' THEN realized_value END) as captacao_liquida,
    MAX(CASE WHEN attribute_code = 'CAPT_LIQ' THEN target_value END) as captacao_liquida_meta,
    MAX(CASE WHEN attribute_code = 'CAPT_LIQ' THEN achievement_percentage END) as captacao_liquida_ating,
    MAX(CASE WHEN attribute_code = 'CAPT_LIQ' THEN weighted_achievement END) as captacao_liquida_ponderado,
    
    MAX(CASE WHEN attribute_code = 'REC_LIQ' THEN realized_value END) as receita_liquida,
    MAX(CASE WHEN attribute_code = 'REC_LIQ' THEN target_value END) as receita_liquida_meta,
    MAX(CASE WHEN attribute_code = 'REC_LIQ' THEN achievement_percentage END) as receita_liquida_ating,
    MAX(CASE WHEN attribute_code = 'REC_LIQ' THEN weighted_achievement END) as receita_liquida_ponderado,
    
    -- Indicadores de volume
    MAX(CASE WHEN attribute_code = 'ABERT_300K' THEN realized_value END) as aberturas_300k,
    MAX(CASE WHEN attribute_code = 'ABERT_300K' THEN target_value END) as aberturas_300k_meta,
    MAX(CASE WHEN attribute_code = 'ABERT_300K' THEN achievement_percentage END) as aberturas_300k_ating,
    MAX(CASE WHEN attribute_code = 'ABERT_300K' THEN weighted_achievement END) as aberturas_300k_ponderado,
    
    MAX(CASE WHEN attribute_code = 'CLIENT_300K_CDI' THEN realized_value END) as clientes_300k_cdi,
    MAX(CASE WHEN attribute_code = 'CLIENT_300K_CDI' THEN target_value END) as clientes_300k_cdi_meta,
    MAX(CASE WHEN attribute_code = 'CLIENT_300K_CDI' THEN achievement_percentage END) as clientes_300k_cdi_ating,
    MAX(CASE WHEN attribute_code = 'CLIENT_300K_CDI' THEN weighted_achievement END) as clientes_300k_cdi_ponderado,
    
    -- Indicadores de qualidade
    MAX(CASE WHEN attribute_code = 'IEA' THEN realized_value END) as iea,
    MAX(CASE WHEN attribute_code = 'IEA' THEN target_value END) as iea_meta,
    MAX(CASE WHEN attribute_code = 'IEA' THEN achievement_percentage END) as iea_ating,
    MAX(CASE WHEN attribute_code = 'IEA' THEN weighted_achievement END) as iea_ponderado,
    
    MAX(CASE WHEN attribute_code = 'NPS' THEN realized_value END) as nps,
    MAX(CASE WHEN attribute_code = 'NPS' THEN target_value END) as nps_meta,
    MAX(CASE WHEN attribute_code = 'NPS' THEN achievement_percentage END) as nps_ating,
    MAX(CASE WHEN attribute_code = 'NPS' THEN weighted_achievement END) as nps_ponderado,
    
    -- Outros indicadores genéricos (adicionar conforme necessário)
    MAX(CASE WHEN attribute_code NOT IN ('CAPT_LIQ','REC_LIQ','ABERT_300K','CLIENT_300K_CDI','IEA','NPS') 
             THEN attribute_code + ': ' + CAST(realized_value AS VARCHAR(20)) END) as outros_indicadores
    
FROM IndicatorPivot
GROUP BY 
    period_start,
    period_end,
    codigo_assessor_crm,
    nome_pessoa,
    tipo_pessoa,
    equipe_comercial,
    regional;
GO

-- ==============================================================================
-- 3. VIEW 2: WEIGHTED SCORE
-- ==============================================================================
-- Drop view se existir
IF OBJECT_ID('gold.vw_card_metas_weighted_score', 'V') IS NOT NULL
    DROP VIEW gold.vw_card_metas_weighted_score;
GO

-- Criar view de score ponderado
CREATE VIEW gold.vw_card_metas_weighted_score
AS
WITH ScoreCalc AS (
    SELECT 
        cm.period_start,
        cm.period_end,
        cm.entity_id as codigo_assessor_crm,
        p.nome_pessoa,
        p.tipo_pessoa,
        p.equipe_comercial,
        p.regional,
        p.gestor_direto,
        
        -- Métricas de score
        COUNT(CASE WHEN cm.indicator_type = 'CARD' THEN 1 END) as qtd_indicadores_card,
        SUM(CASE WHEN cm.indicator_type = 'CARD' THEN cm.indicator_weight ELSE 0 END) as soma_pesos,
        SUM(CASE WHEN cm.indicator_type = 'CARD' THEN cm.weighted_achievement ELSE 0 END) as score_ponderado,
        
        -- Estatísticas de atingimento
        COUNT(CASE WHEN cm.achievement_status = 'SUPERADO' THEN 1 END) as qtd_superado,
        COUNT(CASE WHEN cm.achievement_status = 'ATINGIDO' THEN 1 END) as qtd_atingido,
        COUNT(CASE WHEN cm.achievement_status = 'PARCIAL' THEN 1 END) as qtd_parcial,
        COUNT(CASE WHEN cm.achievement_status = 'NAO_ATINGIDO' THEN 1 END) as qtd_nao_atingido,
        
        -- Média de atingimento (sem ponderação)
        AVG(CASE WHEN cm.indicator_type = 'CARD' AND cm.achievement_percentage IS NOT NULL 
                 THEN cm.achievement_percentage END) as media_atingimento,
        
        -- Indicadores com maior/menor performance
        MAX(CASE WHEN cm.indicator_type = 'CARD' THEN cm.achievement_percentage END) as melhor_atingimento,
        MIN(CASE WHEN cm.indicator_type = 'CARD' THEN cm.achievement_percentage END) as pior_atingimento
        
    FROM gold.card_metas cm
    LEFT JOIN silver.dim_pessoas p ON cm.entity_id = p.codigo_assessor_crm
    WHERE cm.is_calculated = 1
    GROUP BY 
        cm.period_start,
        cm.period_end,
        cm.entity_id,
        p.nome_pessoa,
        p.tipo_pessoa,
        p.equipe_comercial,
        p.regional,
        p.gestor_direto
),
ClassificationCalc AS (
    SELECT 
        *,
        -- Classificação de performance
        CASE 
            WHEN score_ponderado >= 120 THEN 'EXCELENTE'
            WHEN score_ponderado >= 100 THEN 'BOM'
            WHEN score_ponderado >= 80 THEN 'REGULAR'
            ELSE 'ABAIXO'
        END as classificacao_performance,
        
        -- Percentil dentro do período
        PERCENT_RANK() OVER (
            PARTITION BY period_start 
            ORDER BY score_ponderado
        ) * 100 as percentil_periodo
        
    FROM ScoreCalc
    WHERE soma_pesos >= 99 -- Validar apenas com pesos completos
)
SELECT 
    period_start,
    period_end,
    codigo_assessor_crm,
    nome_pessoa,
    tipo_pessoa,
    equipe_comercial,
    regional,
    gestor_direto,
    qtd_indicadores_card,
    soma_pesos,
    ROUND(score_ponderado, 2) as score_ponderado,
    classificacao_performance,
    ROUND(percentil_periodo, 1) as percentil_periodo,
    qtd_superado,
    qtd_atingido,
    qtd_parcial,
    qtd_nao_atingido,
    ROUND(media_atingimento, 2) as media_atingimento,
    ROUND(melhor_atingimento, 2) as melhor_atingimento,
    ROUND(pior_atingimento, 2) as pior_atingimento,
    -- Delta entre melhor e pior
    ROUND(melhor_atingimento - pior_atingimento, 2) as amplitude_performance
FROM ClassificationCalc;
GO

-- ==============================================================================
-- 4. VIEW 3: RANKING
-- ==============================================================================
-- Drop view se existir
IF OBJECT_ID('gold.vw_card_metas_ranking', 'V') IS NOT NULL
    DROP VIEW gold.vw_card_metas_ranking;
GO

-- Criar view de rankings
CREATE VIEW gold.vw_card_metas_ranking
AS
WITH RankingBase AS (
    SELECT 
        cm.period_start,
        cm.period_end,
        cm.entity_id as codigo_assessor_crm,
        p.nome_pessoa,
        p.tipo_pessoa,
        p.equipe_comercial,
        p.regional,
        cm.attribute_code as indicador,
        cm.attribute_name as indicador_nome,
        cm.realized_value,
        cm.target_value,
        cm.achievement_percentage,
        cm.weighted_achievement,
        
        -- Rankings por indicador
        RANK() OVER (
            PARTITION BY cm.period_start, cm.attribute_code 
            ORDER BY cm.achievement_percentage DESC
        ) as ranking_indicador,
        
        -- Total de participantes no ranking
        COUNT(*) OVER (
            PARTITION BY cm.period_start, cm.attribute_code
        ) as total_participantes,
        
        -- Rankings por equipe
        RANK() OVER (
            PARTITION BY cm.period_start, cm.attribute_code, p.equipe_comercial
            ORDER BY cm.achievement_percentage DESC
        ) as ranking_equipe,
        
        -- Total na equipe
        COUNT(*) OVER (
            PARTITION BY cm.period_start, cm.attribute_code, p.equipe_comercial
        ) as total_equipe
        
    FROM gold.card_metas cm
    LEFT JOIN silver.dim_pessoas p ON cm.entity_id = p.codigo_assessor_crm
    WHERE cm.indicator_type = 'CARD'
      AND cm.achievement_percentage IS NOT NULL
),
ScoreRanking AS (
    SELECT 
        period_start,
        period_end,
        codigo_assessor_crm,
        nome_pessoa,
        score_ponderado,
        
        -- Ranking geral por score
        RANK() OVER (
            PARTITION BY period_start 
            ORDER BY score_ponderado DESC
        ) as ranking_geral,
        
        -- Ranking por tipo
        RANK() OVER (
            PARTITION BY period_start, tipo_pessoa 
            ORDER BY score_ponderado DESC
        ) as ranking_tipo,
        
        -- Ranking por equipe
        RANK() OVER (
            PARTITION BY period_start, equipe_comercial 
            ORDER BY score_ponderado DESC
        ) as ranking_score_equipe
        
    FROM gold.vw_card_metas_weighted_score
)
SELECT 
    rb.period_start,
    rb.period_end,
    rb.codigo_assessor_crm,
    rb.nome_pessoa,
    rb.tipo_pessoa,
    rb.equipe_comercial,
    rb.regional,
    rb.indicador,
    rb.indicador_nome,
    rb.realized_value,
    rb.target_value,
    rb.achievement_percentage,
    rb.ranking_indicador,
    rb.total_participantes,
    
    -- Posição relativa
    CAST(rb.ranking_indicador AS VARCHAR) + '/' + CAST(rb.total_participantes AS VARCHAR) as posicao_indicador,
    
    -- Top performers
    CASE 
        WHEN rb.ranking_indicador <= 3 THEN 'TOP 3'
        WHEN rb.ranking_indicador <= 10 THEN 'TOP 10'
        WHEN rb.ranking_indicador <= CEILING(rb.total_participantes * 0.25) THEN 'PRIMEIRO QUARTIL'
        WHEN rb.ranking_indicador <= CEILING(rb.total_participantes * 0.50) THEN 'SEGUNDO QUARTIL'
        WHEN rb.ranking_indicador <= CEILING(rb.total_participantes * 0.75) THEN 'TERCEIRO QUARTIL'
        ELSE 'QUARTO QUARTIL'
    END as quartil_indicador,
    
    rb.ranking_equipe,
    rb.total_equipe,
    CAST(rb.ranking_equipe AS VARCHAR) + '/' + CAST(rb.total_equipe AS VARCHAR) as posicao_equipe,
    
    -- Rankings de score geral
    sr.score_ponderado,
    sr.ranking_geral,
    sr.ranking_tipo,
    sr.ranking_score_equipe
    
FROM RankingBase rb
LEFT JOIN ScoreRanking sr ON 
    rb.codigo_assessor_crm = sr.codigo_assessor_crm AND
    rb.period_start = sr.period_start;
GO

-- ==============================================================================
-- 5. VIEW 4: DASHBOARD PRINCIPAL
-- ==============================================================================
-- Drop view se existir
IF OBJECT_ID('gold.vw_card_metas_dashboard', 'V') IS NOT NULL
    DROP VIEW gold.vw_card_metas_dashboard;
GO

-- Criar view consolidada para dashboards
CREATE VIEW gold.vw_card_metas_dashboard
AS
WITH CurrentPeriod AS (
    -- Identificar período mais recente
    SELECT MAX(period_start) as ultimo_periodo
    FROM gold.card_metas
),
DashboardData AS (
    SELECT 
        cm.period_start,
        cm.period_end,
        YEAR(cm.period_start) as ano,
        MONTH(cm.period_start) as mes,
        DATENAME(MONTH, cm.period_start) + '/' + CAST(YEAR(cm.period_start) AS VARCHAR(4)) as periodo_texto,
        
        -- Dados do assessor
        cm.entity_id as codigo_assessor_crm,
        p.nome_pessoa,
        p.tipo_pessoa,
        p.equipe_comercial,
        p.regional,
        p.gestor_direto,
        p.data_admissao,
        DATEDIFF(MONTH, p.data_admissao, cm.period_start) as meses_empresa,
        
        -- Dados do indicador
        cm.attribute_code as indicador_codigo,
        cm.attribute_name as indicador_nome,
        cm.indicator_type,
        cm.indicator_category,
        cm.indicator_weight as peso,
        
        -- Valores e metas
        cm.target_value as meta,
        cm.stretch_value as meta_stretch,
        cm.minimum_value as meta_minima,
        cm.realized_value as realizado,
        
        -- Performance
        cm.achievement_percentage as percentual_atingimento,
        cm.weighted_achievement as atingimento_ponderado,
        cm.achievement_status as status_atingimento,
        
        -- Flags
        cm.is_inverted as indicador_invertido,
        cm.has_error as teve_erro,
        
        -- Comparação com período atual
        CASE 
            WHEN cm.period_start = (SELECT ultimo_periodo FROM CurrentPeriod) 
            THEN 1 ELSE 0 
        END as is_periodo_atual,
        
        -- Metadados
        cm.processing_date as data_processamento,
        cm.processing_duration_ms as tempo_calculo_ms
        
    FROM gold.card_metas cm
    LEFT JOIN silver.dim_pessoas p ON cm.entity_id = p.codigo_assessor_crm
    WHERE cm.is_calculated = 1
),
Aggregations AS (
    -- Agregações por assessor/período para métricas adicionais
    SELECT 
        period_start,
        codigo_assessor_crm,
        
        -- Score total
        SUM(CASE WHEN indicator_type = 'CARD' THEN atingimento_ponderado ELSE 0 END) as score_total,
        
        -- Contadores
        COUNT(DISTINCT indicador_codigo) as qtd_indicadores,
        COUNT(CASE WHEN indicator_type = 'CARD' THEN 1 END) as qtd_indicadores_card,
        COUNT(CASE WHEN status_atingimento = 'SUPERADO' THEN 1 END) as qtd_superado,
        COUNT(CASE WHEN status_atingimento = 'ATINGIDO' THEN 1 END) as qtd_atingido,
        COUNT(CASE WHEN teve_erro = 1 THEN 1 END) as qtd_erros
        
    FROM DashboardData
    GROUP BY period_start, codigo_assessor_crm
)
SELECT 
    d.*,
    a.score_total,
    a.qtd_indicadores,
    a.qtd_indicadores_card,
    a.qtd_superado,
    a.qtd_atingido,
    a.qtd_erros,
    
    -- Categorização adicional
    CASE 
        WHEN d.percentual_atingimento >= 120 THEN 5  -- Estrelas
        WHEN d.percentual_atingimento >= 110 THEN 4
        WHEN d.percentual_atingimento >= 100 THEN 3
        WHEN d.percentual_atingimento >= 90 THEN 2
        WHEN d.percentual_atingimento >= 80 THEN 1
        ELSE 0
    END as estrelas_indicador,
    
    -- Análise YoY/MoM (requer histórico)
    LAG(d.realizado, 1) OVER (
        PARTITION BY d.codigo_assessor_crm, d.indicador_codigo 
        ORDER BY d.period_start
    ) as realizado_mes_anterior,
    
    LAG(d.realizado, 12) OVER (
        PARTITION BY d.codigo_assessor_crm, d.indicador_codigo 
        ORDER BY d.period_start
    ) as realizado_ano_anterior
    
FROM DashboardData d
LEFT JOIN Aggregations a ON 
    d.period_start = a.period_start AND 
    d.codigo_assessor_crm = a.codigo_assessor_crm;
GO

-- ==============================================================================
-- 6. VIEW 5: ANÁLISE TEMPORAL
-- ==============================================================================
-- Drop view se existir
IF OBJECT_ID('gold.vw_card_metas_serie_temporal', 'V') IS NOT NULL
    DROP VIEW gold.vw_card_metas_serie_temporal;
GO

-- Criar view para análise de série temporal
CREATE VIEW gold.vw_card_metas_serie_temporal
AS
WITH SerieBase AS (
    SELECT 
        cm.entity_id as codigo_assessor_crm,
        p.nome_pessoa,
        cm.attribute_code as indicador,
        cm.period_start,
        YEAR(cm.period_start) as ano,
        MONTH(cm.period_start) as mes,
        cm.realized_value,
        cm.target_value,
        cm.achievement_percentage,
        
        -- Médias móveis
        AVG(cm.realized_value) OVER (
            PARTITION BY cm.entity_id, cm.attribute_code 
            ORDER BY cm.period_start 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as media_movel_3m,
        
        AVG(cm.realized_value) OVER (
            PARTITION BY cm.entity_id, cm.attribute_code 
            ORDER BY cm.period_start 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) as media_movel_12m,
        
        -- Crescimento MoM
        LAG(cm.realized_value, 1) OVER (
            PARTITION BY cm.entity_id, cm.attribute_code 
            ORDER BY cm.period_start
        ) as valor_mes_anterior,
        
        -- Crescimento YoY
        LAG(cm.realized_value, 12) OVER (
            PARTITION BY cm.entity_id, cm.attribute_code 
            ORDER BY cm.period_start
        ) as valor_ano_anterior,
        
        -- Ranking histórico
        RANK() OVER (
            PARTITION BY cm.entity_id, cm.attribute_code 
            ORDER BY cm.realized_value DESC
        ) as ranking_historico
        
    FROM gold.card_metas cm
    LEFT JOIN silver.dim_pessoas p ON cm.entity_id = p.codigo_assessor_crm
    WHERE cm.indicator_type = 'CARD'
      AND cm.is_calculated = 1
)
SELECT 
    codigo_assessor_crm,
    nome_pessoa,
    indicador,
    period_start,
    ano,
    mes,
    realized_value,
    target_value,
    achievement_percentage,
    
    -- Médias móveis
    ROUND(media_movel_3m, 2) as media_movel_3m,
    ROUND(media_movel_12m, 2) as media_movel_12m,
    
    -- Variações
    valor_mes_anterior,
    CASE 
        WHEN valor_mes_anterior > 0 
        THEN ROUND(((realized_value - valor_mes_anterior) / valor_mes_anterior) * 100, 2)
        ELSE NULL 
    END as variacao_mom_pct,
    
    valor_ano_anterior,
    CASE 
        WHEN valor_ano_anterior > 0 
        THEN ROUND(((realized_value - valor_ano_anterior) / valor_ano_anterior) * 100, 2)
        ELSE NULL 
    END as variacao_yoy_pct,
    
    -- Tendência
    CASE 
        WHEN realized_value > media_movel_3m THEN 'CRESCENTE'
        WHEN realized_value < media_movel_3m THEN 'DECRESCENTE'
        ELSE 'ESTAVEL'
    END as tendencia_3m,
    
    ranking_historico,
    
    -- Melhor/pior mês
    CASE 
        WHEN ranking_historico = 1 THEN 'MELHOR MÊS'
        WHEN ranking_historico = (
            SELECT COUNT(DISTINCT period_start) 
            FROM gold.card_metas 
            WHERE entity_id = SerieBase.codigo_assessor_crm 
              AND attribute_code = SerieBase.indicador
        ) THEN 'PIOR MÊS'
        ELSE NULL
    END as destaque_historico
    
FROM SerieBase;
GO

-- ==============================================================================
-- 7. PERMISSÕES
-- ==============================================================================
-- Conceder permissões de leitura para todas as views
GRANT SELECT ON gold.vw_card_metas_pivot TO db_datareader;
GRANT SELECT ON gold.vw_card_metas_weighted_score TO db_datareader;
GRANT SELECT ON gold.vw_card_metas_ranking TO db_datareader;
GRANT SELECT ON gold.vw_card_metas_dashboard TO db_datareader;
GRANT SELECT ON gold.vw_card_metas_serie_temporal TO db_datareader;
GO

-- ==============================================================================
-- 8. ESTATÍSTICAS
-- ==============================================================================
-- Criar estatísticas para otimizar queries nas views
CREATE STATISTICS stat_card_metas_period ON gold.card_metas(period_start, entity_id);
CREATE STATISTICS stat_card_metas_indicator ON gold.card_metas(attribute_code, indicator_type);
CREATE STATISTICS stat_card_metas_achievement ON gold.card_metas(achievement_percentage, weighted_achievement);
GO

-- ==============================================================================
-- 9. EXEMPLOS DE USO
-- ==============================================================================
/*
-- 1. Consultar visão pivot para um período
SELECT * FROM gold.vw_card_metas_pivot
WHERE period_start = '2025-01-01'
ORDER BY codigo_assessor_crm;

-- 2. Top 10 assessores por score ponderado
SELECT TOP 10 * FROM gold.vw_card_metas_weighted_score
WHERE period_start = '2025-01-01'
ORDER BY score_ponderado DESC;

-- 3. Ranking de captação líquida
SELECT * FROM gold.vw_card_metas_ranking
WHERE period_start = '2025-01-01'
  AND indicador = 'CAPT_LIQ'
  AND ranking_indicador <= 20
ORDER BY ranking_indicador;

-- 4. Dashboard para Power BI - período atual
SELECT * FROM gold.vw_card_metas_dashboard
WHERE is_periodo_atual = 1
  AND indicator_type = 'CARD'
ORDER BY codigo_assessor_crm, peso DESC;

-- 5. Análise temporal de um assessor
SELECT * FROM gold.vw_card_metas_serie_temporal
WHERE codigo_assessor_crm = 'AAI001'
  AND indicador = 'CAPT_LIQ'
  AND period_start >= DATEADD(MONTH, -12, GETDATE())
ORDER BY period_start;

-- 6. Verificar performance das views
SET STATISTICS TIME ON;
SET STATISTICS IO ON;

SELECT COUNT(*) FROM gold.vw_card_metas_dashboard WHERE period_start = '2025-01-01';

SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
*/

-- ==============================================================================
-- 10. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                  | Descrição
--------|------------|------------------------|--------------------------------------------
1.0.0   | 2025-01-18 | bruno.chiaramonti     | Criação inicial das views Gold
*/

-- ==============================================================================
-- 11. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Views otimizadas para Power BI com DirectQuery
- Índices na tabela base garantem performance adequada
- View Dashboard é a mais completa para análises
- View Pivot facilita consumo por ferramentas que não suportam EAV
- Série temporal permite análises de tendência

Performance:
- Todas as views devem responder em < 2 segundos
- Para grandes volumes, considerar indexed views
- Estatísticas devem ser atualizadas semanalmente

Manutenção:
- Adicionar novos indicadores na view pivot conforme necessário
- Revisar índices mensalmente baseado em query patterns

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

PRINT 'Views Gold criadas com sucesso!';
GO