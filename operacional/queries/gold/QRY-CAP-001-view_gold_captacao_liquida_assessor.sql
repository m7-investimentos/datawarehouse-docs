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
        [silver].[fact_captacao_bruta]
    GROUP BY 
        YEAR(data_ref), 
        MONTH(data_ref)
)
SELECT 
    fcb.data_ref,
    YEAR(fcb.data_ref) AS ano,
    MONTH(fcb.data_ref) AS mes,
    fcb.cod_assessor,
    -- Captação Bruta
    SUM(fcb.captacao_bruta_xp) AS captacao_bruta_xp,
    SUM(fcb.captacao_bruta_transferencia) AS captacao_bruta_transferencia,
    SUM(fcb.captacao_bruta_total) AS captacao_bruta_total,
    -- Resgate Bruto
    COALESCE(SUM(fr.resgate_bruto_xp), 0) AS resgate_bruto_xp,
    COALESCE(SUM(fr.resgate_bruto_transferencia), 0) AS resgate_bruto_transferencia,
    COALESCE(SUM(fr.resgate_bruto_total), 0) AS resgate_bruto_total,
    -- Captação Líquida
    SUM(fcb.captacao_bruta_xp) - COALESCE(SUM(fr.resgate_bruto_xp), 0) AS captacao_liquida_xp,
    SUM(fcb.captacao_bruta_transferencia) - COALESCE(SUM(fr.resgate_bruto_transferencia), 0) AS captacao_liquida_transferencia,
    SUM(fcb.captacao_bruta_total) - COALESCE(SUM(fr.resgate_bruto_total), 0) AS captacao_liquida_total
FROM 
    [silver].[fact_captacao_bruta] fcb
    INNER JOIN ultimo_dia_mes udm
        ON fcb.data_ref = udm.ultimo_dia_disponivel
    LEFT JOIN [silver].[fact_resgates] fr
        ON fcb.data_ref = fr.data_ref
        AND fcb.cod_assessor = fr.cod_assessor
GROUP BY 
    fcb.data_ref,
    YEAR(fcb.data_ref),
    MONTH(fcb.data_ref),
    fcb.cod_assessor
GO