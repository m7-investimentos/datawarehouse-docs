SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   VIEW [silver].[vw_fact_indice_esforco_assessor]
AS
WITH CTE_Base AS (
    SELECT 
        ano_mes,
        cod_assessor,
        -- Métricas principais
        CAST(iea_final AS DECIMAL(18,8)) AS indice_esforco_assessor,
        CAST(esforco_prospeccao AS DECIMAL(18,8)) AS esforco_prospeccao,
        CAST(esforco_relacionamento AS DECIMAL(18,8)) AS esforco_relacionamento,
        
        -- Métricas de prospecção
        CAST(atingimento_lead_starts AS DECIMAL(18,8)) AS prospeccao_atingimento_lead_starts,
        CAST(atingimento_habilitacoes AS DECIMAL(18,8)) AS prospeccao_atingimento_habilitacoes,
        CAST(atingimento_conversao AS DECIMAL(18,8)) AS prospeccao_atingimento_conversao,
        CAST(atingimento_carteiras_simuladas_novos AS DECIMAL(18,8)) AS prospeccao_atingimento_carteiras_simuladas_novos,
        captacao_de_novos_clientes_por_aai AS prospeccao_captacao_de_novos_clientes_por_aai,
        
        -- Métricas de relacionamento
        CAST(atingimento_contas_aportarem AS DECIMAL(18,8)) AS relacionamento_atingimento_contas_aportarem,
        CAST(atingimento_ordens_enviadas AS DECIMAL(18,8)) AS relacionamento_atingimento_ordens_enviadas,
        CAST(atingimento_contas_acessadas_hub AS DECIMAL(18,8)) AS relacionamento_atingimento_contas_acessadas_hub,
        captacao_da_base AS relacionamento_captacao_da_base
    FROM bronze.xp_iea
)
SELECT 
    ano_mes,
    cod_assessor,
    esforco_prospeccao,
    esforco_relacionamento,
    indice_esforco_assessor,
    
    -- Média simples dos últimos 3 meses
    CAST(
        AVG(indice_esforco_assessor) OVER (
            PARTITION BY cod_assessor 
            ORDER BY ano_mes 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS DECIMAL(18,8)
    ) AS indice_esforco_assessor_acum_3_meses,
    
    -- Média simples dos últimos 6 meses
    CAST(
        AVG(indice_esforco_assessor) OVER (
            PARTITION BY cod_assessor 
            ORDER BY ano_mes 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS DECIMAL(18,8)
    ) AS indice_esforco_assessor_acum_6_meses,
    
    -- Média simples dos últimos 12 meses
    CAST(
        AVG(indice_esforco_assessor) OVER (
            PARTITION BY cod_assessor 
            ORDER BY ano_mes 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS DECIMAL(18,8)
    ) AS indice_esforco_assessor_acum_12_meses,
    
    -- Métricas de prospecção
    prospeccao_atingimento_carteiras_simuladas_novos,
    prospeccao_atingimento_conversao,
    prospeccao_atingimento_habilitacoes,
    prospeccao_atingimento_lead_starts,
    prospeccao_captacao_de_novos_clientes_por_aai,
    
    -- Métricas de relacionamento
    relacionamento_atingimento_contas_acessadas_hub,
    relacionamento_atingimento_contas_aportarem,
    relacionamento_atingimento_ordens_enviadas,
    relacionamento_captacao_da_base
    
FROM CTE_Base;
GO
