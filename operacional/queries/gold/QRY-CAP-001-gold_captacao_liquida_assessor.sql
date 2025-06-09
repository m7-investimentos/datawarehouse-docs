-- Remover view existente se necessário
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[gold].[captacao_liquida_assessor]'))
    DROP VIEW [gold].[captacao_liquida_assessor]
GO

CREATE VIEW [gold].[captacao_liquida_assessor] AS
WITH ultimo_dia_mes AS (
    SELECT 
        YEAR(data_ref) AS ano,
        MONTH(data_ref) AS mes,
        MAX(data_ref) AS ultimo_dia_disponivel
    FROM 
        [silver].[silver_fact_captacao_bruta]
    GROUP BY 
        YEAR(data_ref), 
        MONTH(data_ref)
)
SELECT 
    fcb.data_ref,
    YEAR(fcb.data_ref) AS ano,
    MONTH(fcb.data_ref) AS mes,
    fcb.cod_assessor,
    fcb.origem_captacao,
    COUNT(DISTINCT fcb.conta_xp_cliente) AS qtd_clientes,
    SUM(fcb.valor_captacao) AS valor_captacao_total,
    AVG(fcb.valor_captacao) AS valor_captacao_medio,
    MIN(fcb.valor_captacao) AS valor_captacao_minimo,
    MAX(fcb.valor_captacao) AS valor_captacao_maximo
FROM 
    [silver].[silver_fact_captacao_bruta] fcb
    INNER JOIN ultimo_dia_mes udm
        ON fcb.data_ref = udm.ultimo_dia_disponivel
GROUP BY 
    fcb.data_ref,
    YEAR(fcb.data_ref),
    MONTH(fcb.data_ref),
    fcb.cod_assessor,
    fcb.origem_captacao
GO