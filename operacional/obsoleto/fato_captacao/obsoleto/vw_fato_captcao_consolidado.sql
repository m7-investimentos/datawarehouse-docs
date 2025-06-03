USE [M7InvestimentosOLAP];
GO
CREATE OR ALTER VIEW [dbo].[vw_fato_captacao] AS
WITH CaptacaoPorAssessor AS (
    -- Juntar captações com a dimensão de pessoas para obter o crm_id do assessor
    SELECT 
        cp.data_ref,
        cp.crm_id,
        SUM(cp.captacao_liquida_parcial) AS captacao_liquida_parcial,
        SUM(cp.captacao_bruta) AS captacao_bruta,
        SUM(cp.resgate) AS resgate,
        YEAR(cp.data_ref) AS ano,
        MONTH(cp.data_ref) AS mes
    FROM 
        [dbo].[fato_captacao_liquida_parcial] cp
    GROUP BY 
        cp.data_ref, cp.crm_id, YEAR(cp.data_ref), MONTH(cp.data_ref)
),
TransferenciasPorAssessor AS (
    -- Agregar transferências considerando origem (negativo) e destino (positivo)
    SELECT 
        tf.data_transferencia AS data_ref,
        crm_id AS crm_id, -- Pode ser origem ou destino
        -- Para transferências internas, registra positivo no destino e negativo na origem
        SUM(CASE 
                WHEN tipo_transf = 'interna M7' AND crm_id = crm_id_destino THEN PL_transferencia
                WHEN tipo_transf = 'interna M7' AND crm_id = crm_id_origem THEN -PL_transferencia
                ELSE 0 
            END) AS valor_trasnf_interna_m7,
        
        -- Para transferências de escritório, apenas valor positivo no destino
        SUM(CASE 
                WHEN tipo_transf = 'trasnferencia de escritorio' AND crm_id = crm_id_destino THEN PL_transferencia
                ELSE 0 
            END) AS valor_transf_escritorio,
        
        -- Para saídas, considerar apenas a origem (valor negativo)
        SUM(CASE 
                WHEN tipo_transf = 'saida' AND crm_id = crm_id_origem THEN -PL_transferencia
                ELSE 0 
            END) AS valor_transf_saida,
        
        -- Para novas contas, considerar apenas o destino (valor positivo)
        SUM(CASE 
                WHEN tipo_transf = 'nova conta' AND crm_id = crm_id_destino THEN PL_transferencia
                ELSE 0 
            END) AS valor_transf_nova_conta,
        
        YEAR(tf.data_transferencia) AS ano,
        MONTH(tf.data_transferencia) AS mes
    FROM 
        [dbo].[fato_transf_clientes] tf
    -- Desdobrar cada transferência em duas linhas: uma para origem e outra para destino
    CROSS APPLY (
        SELECT crm_id_origem AS crm_id
        UNION ALL 
        SELECT crm_id_destino
        -- Filtrar apenas quando o tipo for relevante para o crm_id
        WHERE (tipo_transf IN ('interna M7', 'trasnferencia de escritorio') OR 
              (tipo_transf = 'nova conta'))
    ) ca
    -- Filtrar apenas crm_id que existem na dimensão pessoas
    WHERE EXISTS (SELECT 1 FROM [dbo].[dim_pessoas] dp WHERE dp.crm_id = ca.crm_id)
    GROUP BY 
        tf.data_transferencia, crm_id, YEAR(tf.data_transferencia), MONTH(tf.data_transferencia)
),
-- Acumulado mensal apenas para a relação captação/resgate
AcumuladoMensal AS (
    SELECT 
        crm_id,
        ano,
        mes,
        SUM(captacao_bruta) AS captacao_bruta_mes,
        SUM(resgate) AS resgate_mes
    FROM 
        CaptacaoPorAssessor
    GROUP BY 
        crm_id, ano, mes
)
-- Combinar captações e transferências usando crm_id como chave
SELECT 
    COALESCE(c.data_ref, t.data_ref) AS data_ref,
    COALESCE(c.crm_id, t.crm_id) AS crm_id,
    
    -- 1. VOLUME DE ENTRADAS (como mencionado na estrutura de árvore)
    COALESCE(c.captacao_bruta, 0) AS captacao_bruta,
    COALESCE(t.valor_transf_nova_conta, 0) AS valor_nova_conta,  -- Novos Clientes
    COALESCE(t.valor_transf_escritorio, 0) AS valor_transf_escritorio,  -- Transferências de outros escritórios
    
    -- 2. VOLUME DE SAÍDAS (como mencionado na estrutura de árvore)
    COALESCE(c.resgate, 0) AS resgate,  -- Saídas por resgates
    COALESCE(t.valor_transf_saida, 0) AS valor_transf_saida,  -- Saídas por transferências
    
    -- 3. CAPTAÇÃO LÍQUIDA (diferentes visões conforme o negócio)
    COALESCE(c.captacao_liquida_parcial, 0) AS captacao_liquida_parcial,
    
    -- Calculando a captação líquida M7 
    COALESCE(c.captacao_liquida_parcial, 0) + 
    COALESCE(t.valor_transf_escritorio, 0) + 
    COALESCE(t.valor_transf_saida, 0) + 
    COALESCE(t.valor_transf_nova_conta, 0) AS captacao_liquida_m7,
    
    -- Calculando a captação líquida XP 
    COALESCE(c.captacao_liquida_parcial, 0) + 
    COALESCE(t.valor_transf_nova_conta, 0) AS captacao_liquida_xp,
    
    -- 4. MOVIMENTAÇÕES INTERNAS (mencionadas como importantes para análise de assessores)
    COALESCE(t.valor_trasnf_interna_m7, 0) AS valor_trasnf_interna_m7,
    
    -- 5. INDICADORES DE EFICIÊNCIA (mencionados explicitamente no documento)
    -- Eficiência do assessor (captação líquida vs. todas as movimentações)
    CASE 
        WHEN (COALESCE(c.captacao_bruta, 0) + COALESCE(c.resgate, 0) + 
              ABS(COALESCE(t.valor_trasnf_interna_m7, 0)) + COALESCE(t.valor_transf_escritorio, 0) + 
              ABS(COALESCE(t.valor_transf_saida, 0)) + COALESCE(t.valor_transf_nova_conta, 0)) > 0
        THEN COALESCE(c.captacao_liquida_parcial, 0) / 
             (COALESCE(c.captacao_bruta, 0) + COALESCE(c.resgate, 0) + 
              ABS(COALESCE(t.valor_trasnf_interna_m7, 0)) + COALESCE(t.valor_transf_escritorio, 0) + 
              ABS(COALESCE(t.valor_transf_saida, 0)) + COALESCE(t.valor_transf_nova_conta, 0))
        ELSE 0
    END AS indice_eficiencia_assessor,
    
    -- 6. APORTES VS RESGATES - Usando acumulado mensal em vez de valor diário
    CASE 
        WHEN ABS(am.resgate_mes) > 0
        THEN am.captacao_bruta_mes / ABS(am.resgate_mes)
        ELSE NULL
    END AS relacao_captacao_resgate,
    
    -- 7. CAPTAÇÃO LÍQUIDA VS BRUTA (mencionado como índice de retenção)
    CASE 
        WHEN COALESCE(c.captacao_bruta, 0) > 0
        THEN COALESCE(c.captacao_liquida_parcial, 0) / COALESCE(c.captacao_bruta, 0)
        ELSE NULL
    END AS taxa_retencao_captacao
    
FROM 
    CaptacaoPorAssessor c
FULL OUTER JOIN 
    TransferenciasPorAssessor t ON c.data_ref = t.data_ref AND c.crm_id = t.crm_id
LEFT JOIN
    AcumuladoMensal am ON 
        COALESCE(c.crm_id, t.crm_id) = am.crm_id AND 
        COALESCE(c.ano, t.ano) = am.ano AND 
        COALESCE(c.mes, t.mes) = am.mes;