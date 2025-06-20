SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   VIEW [gold].[vw_rentabilidade_assessor] AS
SELECT 
    r.ano_mes,
    r.ano,
    r.mes_num as mes,
    r.mes as nome_mes,
    r.trimestre,
    r.semestre,
    h.cod_assessor,
    p.crm_id as codigo_crm_assessor,
    p.nome_pessoa as nome_assessor,
    p.assessor_nivel as nivel_assessor,
    e.id_estrutura as estrutura_id,
    est.nome_estrutura as estrutura_nome,
    
    -- Quantidade total de clientes 300k+
    COUNT(DISTINCT r.conta_xp_cliente) as qtd_clientes_300k_mais,
    
    -- ===== MÉTRICAS MENSAIS (Original) =====
    -- Clientes acima do CDI (rentabilidade > 100% CDI)
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade > c.taxa_cdi_mes 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi,
    
    -- Clientes com rentabilidade >= 80% do CDI
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade >= (c.taxa_cdi_mes * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi,
    
    -- Clientes com rentabilidade >= 50% do CDI
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade >= (c.taxa_cdi_mes * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi,
    
    -- Clientes com rentabilidade positiva
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva,
    
    -- Percentual de clientes acima do CDI
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade > c.taxa_cdi_mes THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi,

    -- ===== MÉTRICAS 3 MESES =====
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_3_meses > c.taxa_cdi_3_meses 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi_3m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_3_meses >= (c.taxa_cdi_3_meses * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi_3m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_3_meses >= (c.taxa_cdi_3_meses * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi_3m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_3_meses > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva_3m,
    
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade_acumulada_3_meses > c.taxa_cdi_3_meses THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi_3m,

    -- ===== MÉTRICAS 6 MESES =====
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_6_meses > c.taxa_cdi_6_meses 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi_6m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_6_meses >= (c.taxa_cdi_6_meses * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi_6m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_6_meses >= (c.taxa_cdi_6_meses * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi_6m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_6_meses > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva_6m,
    
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade_acumulada_6_meses > c.taxa_cdi_6_meses THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi_6m,

    -- ===== MÉTRICAS 12 MESES =====
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_12_meses > c.taxa_cdi_12_meses 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi_12m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_12_meses >= (c.taxa_cdi_12_meses * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi_12m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_12_meses >= (c.taxa_cdi_12_meses * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi_12m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_12_meses > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva_12m,
    
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade_acumulada_12_meses > c.taxa_cdi_12_meses THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi_12m,

    -- ===== MÉTRICAS TRIMESTRE =====
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_trimestre > c.taxa_cdi_trimestre 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi_trimestre,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_trimestre >= (c.taxa_cdi_trimestre * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi_trimestre,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_trimestre >= (c.taxa_cdi_trimestre * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi_trimestre,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_trimestre > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva_trimestre,
    
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade_acumulada_trimestre > c.taxa_cdi_trimestre THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi_trimestre,

    -- ===== MÉTRICAS SEMESTRE =====
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_semestre > c.taxa_cdi_semestre 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi_semestre,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_semestre >= (c.taxa_cdi_semestre * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi_semestre,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_semestre >= (c.taxa_cdi_semestre * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi_semestre,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_semestre > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva_semestre,
    
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade_acumulada_semestre > c.taxa_cdi_semestre THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi_semestre,

    -- ===== MÉTRICAS ANO =====
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_ano > c.taxa_cdi_ano 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi_ano,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_ano >= (c.taxa_cdi_ano * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi_ano,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_ano >= (c.taxa_cdi_ano * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi_ano,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_ano > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva_ano,
    
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade_acumulada_ano > c.taxa_cdi_ano THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi_ano

FROM silver.fact_rentabilidade_clientes r

-- Join com CDI
INNER JOIN silver.fact_cdi_historico c 
    ON r.ano_mes = CAST(c.ano_mes AS INT)

-- Join com patrimônio (filtrando >= 300k)
INNER JOIN silver.fact_patrimonio pat 
    ON r.conta_xp_cliente = pat.conta_xp_cliente 
    AND YEAR(pat.data_ref) * 100 + MONTH(pat.data_ref) = r.ano_mes
    AND pat.patrimonio_xp >= 300000

-- Join com histórico de clientes para pegar o assessor
INNER JOIN silver.fact_cliente_perfil_historico h
    ON r.conta_xp_cliente = h.conta_xp_cliente
    AND YEAR(h.data_ref) * 100 + MONTH(h.data_ref) = r.ano_mes

-- Join com dim_clientes para filtrar apenas PF
INNER JOIN silver.dim_clientes cli
    ON r.conta_xp_cliente = cli.cod_xp
    AND cli.cpf IS NOT NULL  -- Apenas pessoas físicas

-- Join com pessoas para dados do assessor
INNER JOIN silver.dim_pessoas p
    ON h.cod_assessor = p.cod_aai

-- Join com estrutura (opcional - assessor pode não estar em estrutura)
LEFT JOIN silver.fact_estrutura_pessoas e
    ON p.crm_id = e.crm_id
    AND DATEFROMPARTS(r.ano, r.mes_num, 1) >= e.data_entrada
    AND DATEFROMPARTS(r.ano, r.mes_num, 1) <= ISNULL(e.data_saida, '9999-12-31')

-- Join com dimensão de estruturas
LEFT JOIN silver.dim_estruturas est
    ON e.id_estrutura = est.id_estrutura

WHERE h.cod_assessor IS NOT NULL
  AND r.ano >= 2024

GROUP BY 
    r.ano_mes,
    r.ano,
    r.mes_num,
    r.mes,
    r.trimestre,
    r.semestre,
    h.cod_assessor,
    p.crm_id,
    p.nome_pessoa,
    p.assessor_nivel,
    e.id_estrutura,
    est.nome_estrutura;
GO
