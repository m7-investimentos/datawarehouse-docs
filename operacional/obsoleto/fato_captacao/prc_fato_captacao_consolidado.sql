USE [M7InvestimentosOLAP];
GO

CREATE OR ALTER PROCEDURE [dbo].[prc_stage2fact_captacao_consolidado]
AS
BEGIN
    SET NOCOUNT ON;

    WITH CaptacaoPorAssessor AS (
        SELECT 
            cp.data_ref,
            cp.crm_id,
            SUM(cp.captacao_liquida_parcial) AS captacao_liquida_parcial,
            SUM(cp.captacao_bruta) AS captacao_bruta,
            SUM(cp.resgate) AS resgate,
            YEAR(cp.data_ref) AS ano,
            MONTH(cp.data_ref) AS mes
        FROM [M7InvestimentosOLAP].[fato].[fato_captacao_liquida_parcial] cp
        GROUP BY cp.data_ref, cp.crm_id, YEAR(cp.data_ref), MONTH(cp.data_ref)
    ),

    TransferenciasPorAssessor AS (
        SELECT 
            tf.data_transferencia AS data_ref,
            ca.crm_id,
            SUM(CASE 
                    WHEN tipo_transf = 'interna M7' AND ca.crm_id = tf.crm_id_destino THEN PL_transferencia
                    WHEN tipo_transf = 'interna M7' AND ca.crm_id = tf.crm_id_origem THEN -PL_transferencia
                    ELSE 0 
                END) AS valor_trasnf_interna_m7,
            SUM(CASE 
                    WHEN tipo_transf = 'trasnferencia de escritorio' AND ca.crm_id = tf.crm_id_destino THEN PL_transferencia
                    ELSE 0 
                END) AS valor_transf_escritorio,
            SUM(CASE 
                    WHEN tipo_transf = 'saida' AND ca.crm_id = tf.crm_id_origem THEN -PL_transferencia
                    ELSE 0 
                END) AS valor_transf_saida,
            SUM(CASE 
                    WHEN tipo_transf = 'nova conta' AND ca.crm_id = tf.crm_id_destino THEN PL_transferencia
                    ELSE 0 
                END) AS valor_transf_nova_conta,
            YEAR(tf.data_transferencia) AS ano,
            MONTH(tf.data_transferencia) AS mes
        FROM [M7InvestimentosOLAP].[fato].[fato_transf_clientes] tf
        CROSS APPLY (
            SELECT crm_id_origem AS crm_id
            UNION ALL
            SELECT crm_id_destino
        ) ca
        WHERE (tipo_transf IN ('interna M7', 'trasnferencia de escritorio', 'nova conta'))
          AND EXISTS (SELECT 1 FROM [dbo].[dim_pessoas] dp WHERE dp.crm_id = ca.crm_id)
        GROUP BY tf.data_transferencia, ca.crm_id, YEAR(tf.data_transferencia), MONTH(tf.data_transferencia)
    ),

    AcumuladoMensal AS (
        SELECT 
            crm_id,
            ano,
            mes,
            SUM(captacao_bruta) AS captacao_bruta_mes,
            SUM(resgate) AS resgate_mes
        FROM CaptacaoPorAssessor
        GROUP BY crm_id, ano, mes
    )

    INSERT INTO [M7InvestimentosOLAP].[fato].[fato_captacao_consolidado] (
        data_ref,
        crm_id,
        captacao_bruta,
        valor_nova_conta,
        valor_transf_escritorio,
        resgate,
        valor_transf_saida,
        captacao_liquida_parcial,
        captacao_liquida_m7,
        captacao_liquida_xp,
        valor_trasnf_interna_m7,
        indice_eficiencia_captacao,
        relacao_captacao_resgate,
        taxa_retencao_captacao
    )
    SELECT 
        COALESCE(c.data_ref, t.data_ref) AS data_ref,
        COALESCE(c.crm_id, t.crm_id) AS crm_id,
        COALESCE(c.captacao_bruta, 0) AS captacao_bruta,
        COALESCE(t.valor_transf_nova_conta, 0) AS valor_nova_conta,
        COALESCE(t.valor_transf_escritorio, 0) AS valor_transf_escritorio,
        COALESCE(c.resgate, 0) AS resgate,
        COALESCE(t.valor_transf_saida, 0) AS valor_transf_saida,
        COALESCE(c.captacao_liquida_parcial, 0) AS captacao_liquida_parcial,
        COALESCE(c.captacao_liquida_parcial, 0) + COALESCE(t.valor_transf_escritorio, 0) + COALESCE(t.valor_transf_saida, 0) + COALESCE(t.valor_transf_nova_conta, 0) AS captacao_liquida_m7,
        COALESCE(c.captacao_liquida_parcial, 0) + COALESCE(t.valor_transf_nova_conta, 0) AS captacao_liquida_xp,
        COALESCE(t.valor_trasnf_interna_m7, 0) AS valor_trasnf_interna_m7,
        CASE 
            WHEN (COALESCE(c.captacao_bruta, 0) + COALESCE(c.resgate, 0) + ABS(COALESCE(t.valor_trasnf_interna_m7, 0)) + COALESCE(t.valor_transf_escritorio, 0) + ABS(COALESCE(t.valor_transf_saida, 0)) + COALESCE(t.valor_transf_nova_conta, 0)) > 0
            THEN COALESCE(c.captacao_liquida_parcial, 0) /
                 (COALESCE(c.captacao_bruta, 0) + COALESCE(c.resgate, 0) + ABS(COALESCE(t.valor_trasnf_interna_m7, 0)) + COALESCE(t.valor_transf_escritorio, 0) + ABS(COALESCE(t.valor_transf_saida, 0)) + COALESCE(t.valor_transf_nova_conta, 0))
            ELSE 0
        END AS indice_eficiencia_captacao,
        CASE 
            WHEN ABS(am.resgate_mes) > 0
            THEN am.captacao_bruta_mes / ABS(am.resgate_mes)
            ELSE NULL
        END AS relacao_captacao_resgate,
        CASE 
            WHEN COALESCE(c.captacao_bruta, 0) > 0
            THEN COALESCE(c.captacao_liquida_parcial, 0) / COALESCE(c.captacao_bruta, 0)
            ELSE NULL
        END AS taxa_retencao_captacao
    FROM CaptacaoPorAssessor c
    FULL OUTER JOIN TransferenciasPorAssessor t ON c.data_ref = t.data_ref AND c.crm_id = t.crm_id
    LEFT JOIN AcumuladoMensal am ON COALESCE(c.crm_id, t.crm_id) = am.crm_id AND COALESCE(c.ano, t.ano) = am.ano AND COALESCE(c.mes, t.mes) = am.mes
    WHERE NOT EXISTS (
        SELECT 1 
        FROM [M7InvestimentosOLAP].[fato].[fato_captacao_consolidado] f
        WHERE f.data_ref = COALESCE(c.data_ref, t.data_ref) AND f.crm_id = COALESCE(c.crm_id, t.crm_id)
    );
END;
GO