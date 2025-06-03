USE [M7InvestimentosOLAP];
GO

CREATE OR ALTER PROCEDURE [dbo].[prc_stage2fact_captacao_liquida_parcial]
AS
BEGIN
    SET NOCOUNT ON;

    -- Primeiro, garantir que todos os cod_xp existam na dim_clientes
    INSERT INTO [M7InvestimentosOLAP].[DS].[dim_clientes] (cod_xp, status_cliente)
    SELECT DISTINCT s.cod_xp, 'ATIVO' as status_cliente
    FROM [M7InvestimentosOLAP].[stage].[stage_captacao_layer1] s
    WHERE s.data_ref >= '2024-01-01'
    AND s.cod_xp NOT IN (
        SELECT DISTINCT cod_xp 
        FROM [M7InvestimentosOLAP].[DS].[dim_clientes]
    );

    -- Limpar a tabela de fato
    TRUNCATE TABLE [M7InvestimentosOLAP].[fato].[fato_captacao_liquida_parcial];

    -- Agora fazer o INSERT na tabela de fato
    INSERT INTO [M7InvestimentosOLAP].[fato].[fato_captacao_liquida_parcial] (
        data_ref,
        cod_xp,
        crm_id,
        captacao_bruta_asset,
        captacao_bruta_cbx,
        captacao_bruta_cex,
        captacao_bruta_coe,
        captacao_bruta_fundos_exclusivos,
        captacao_bruta_fundos_pco,
        captacao_bruta_ota,
        captacao_bruta_prev,
        captacao_bruta_prev_aporte,
        captacao_bruta_prev_port_entrada,
        captacao_bruta_prev_port_saida,
        captacao_bruta_prev_resgate,
        captacao_bruta_rf,
        captacao_bruta_stvm,
        captacao_bruta_td,
        captacao_bruta_ted,
        resgate_asset,
        resgate_cbx,
        resgate_cex,
        resgate_coe,
        resgate_fundos_exclusivos,
        resgate_fundos_pco,
        resgate_ota,
        resgate_prev,
        resgate_prev_aporte,
        resgate_prev_port_entrada,
        resgate_prev_port_saida,
        resgate_prev_resgate,
        resgate_rf,
        resgate_stvm,
        resgate_td,
        resgate_ted,
        captacao_liquida_parcial,
        captacao_bruta,
        resgate
    )
    SELECT 
        s.data_ref,
        s.cod_xp,
        dp.crm_id,

        SUM(CASE WHEN tipo_de_captacao = 'asset'                          AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'cbx'                            AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'cex'                            AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'coe'                            AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'fundos exclusivos'             AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'fundos pco'                    AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'ota'                            AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'prev'                           AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'prev aporte'                    AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'prev portabilidade entrada'     AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'prev portabilidade saida'       AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'prev resgate'                  AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'rf'                             AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'stvm'                           AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'td'                             AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'ted'                            AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END),

        SUM(CASE WHEN tipo_de_captacao = 'asset'                          AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'cbx'                            AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'cex'                            AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'coe'                            AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'fundos exclusivos'             AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'fundos pco'                    AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'ota'                            AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'prev'                           AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'prev aporte'                    AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'prev portabilidade entrada'     AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'prev portabilidade saida'       AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'prev resgate'                  AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'rf'                             AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'stvm'                           AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'td'                             AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),
        SUM(CASE WHEN tipo_de_captacao = 'ted'                            AND valor_captacao < 0 THEN valor_captacao ELSE 0 END),

        SUM(CASE WHEN sinal_captacao = 1 THEN valor_captacao ELSE 0 END) + 
        SUM(CASE WHEN sinal_captacao = -1 THEN valor_captacao ELSE 0 END) AS captacao_liquida_parcial,

        SUM(CASE WHEN sinal_captacao = 1 THEN valor_captacao ELSE 0 END) AS captacao_bruta,
        SUM(CASE WHEN sinal_captacao = -1 THEN ABS(valor_captacao) ELSE 0 END) * -1 AS resgate

    FROM [M7InvestimentosOLAP].[stage].[stage_captacao_layer1] s
    INNER JOIN [M7InvestimentosOLAP].[dim].[dim_pessoas] dp ON s.cod_aai = dp.cod_aai
    WHERE s.data_ref >= '2024-01-01'
    GROUP BY s.data_ref, s.cod_xp, s.cod_aai, dp.crm_id;
END;
GO
