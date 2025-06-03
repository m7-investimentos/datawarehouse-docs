-- Criação da view fato_captacao utilizando CTE para pivotamento dos tipos de captação
USE [M7InvestimentosOLAP];
GO

ALTER   VIEW [dbo].[vw_fato_captacao_liquida_parcial] AS
SELECT 
    s.data_ref,
    s.cod_xp,
    s.cod_aai,
    dp.crm_id,
    -- Todos os tipos de captação como colunas separadas
    SUM(CASE WHEN tipo_de_captacao = 'asset' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_asset,
    SUM(CASE WHEN tipo_de_captacao = 'cbx' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_cbx,
    SUM(CASE WHEN tipo_de_captacao = 'cex' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_cex,
    SUM(CASE WHEN tipo_de_captacao = 'coe' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_coe,
    SUM(CASE WHEN tipo_de_captacao = 'fundos exclusivos' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_fundos_exclusivos,
    SUM(CASE WHEN tipo_de_captacao = 'fundos pco' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_fundos_pco,
    SUM(CASE WHEN tipo_de_captacao = 'ota' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_ota,
    SUM(CASE WHEN tipo_de_captacao = 'prev' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_prev,
    SUM(CASE WHEN tipo_de_captacao = 'prev aporte' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_prev_aporte,
    SUM(CASE WHEN tipo_de_captacao = 'prev portabilidade entrada' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_prev_port_entrada,
    SUM(CASE WHEN tipo_de_captacao = 'prev portabilidade saida' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_prev_port_saida,
    SUM(CASE WHEN tipo_de_captacao = 'prev resgate' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_prev_resgate,
    SUM(CASE WHEN tipo_de_captacao = 'rf' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_rf,
    SUM(CASE WHEN tipo_de_captacao = 'stvm' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_stvm,
    SUM(CASE WHEN tipo_de_captacao = 'td' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_td,
    SUM(CASE WHEN tipo_de_captacao = 'ted' AND valor_captacao >= 0 THEN valor_captacao ELSE 0 END) AS captacao_bruta_ted,


    SUM(CASE WHEN tipo_de_captacao = 'asset' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_asset,
    SUM(CASE WHEN tipo_de_captacao = 'cbx' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_cbx,
    SUM(CASE WHEN tipo_de_captacao = 'cex' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_cex,
    SUM(CASE WHEN tipo_de_captacao = 'coe' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_coe,
    SUM(CASE WHEN tipo_de_captacao = 'fundos exclusivos' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_fundos_exclusivos,
    SUM(CASE WHEN tipo_de_captacao = 'fundos pco' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_fundos_pco,
    SUM(CASE WHEN tipo_de_captacao = 'ota' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_ota,
    SUM(CASE WHEN tipo_de_captacao = 'prev' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_prev,
    SUM(CASE WHEN tipo_de_captacao = 'prev aporte' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_prev_aporte,
    SUM(CASE WHEN tipo_de_captacao = 'prev portabilidade entrada' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_prev_port_entrada,
    SUM(CASE WHEN tipo_de_captacao = 'prev portabilidade saida' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_prev_port_saida,
    SUM(CASE WHEN tipo_de_captacao = 'prev resgate' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_prev_resgate,
    SUM(CASE WHEN tipo_de_captacao = 'rf' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_rf,
    SUM(CASE WHEN tipo_de_captacao = 'stvm' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_stvm,
    SUM(CASE WHEN tipo_de_captacao = 'td' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_td,
    SUM(CASE WHEN tipo_de_captacao = 'ted' AND valor_captacao < 0 THEN valor_captacao ELSE 0 END) AS resgate_ted,
    
    -- Captação líquida parcial (valor líquido total)
    SUM(valor_captacao) AS captacao_liquida_parcial,
    
    -- Total de aportes (soma dos valores com sinal positivo)
    SUM(CASE WHEN sinal_captacao = 1 THEN valor_captacao ELSE 0 END) AS captacao_bruta,
    
    -- Total de resgates (soma dos valores com sinal negativo)
    SUM(CASE WHEN sinal_captacao = -1 THEN ABS(valor_captacao) ELSE 0 END)*-1 AS resgate,
    
    -- Metadata
    MAX(s.data_carga) AS data_carga
FROM 
    [M7InvestimentosOLAP].[dbo].[stage_captacao_layer1] s
JOIN
    [M7InvestimentosOLAP].[dbo].[dim_pessoas] dp ON s.cod_aai = dp.cod_aai
GROUP BY 
    s.data_ref, s.cod_xp, s.cod_aai, dp.crm_id;
GO