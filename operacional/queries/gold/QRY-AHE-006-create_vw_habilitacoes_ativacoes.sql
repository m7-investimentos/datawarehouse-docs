SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   VIEW [gold].[vw_habilitacoes_ativacoes] AS

WITH 
-- CTE1: Agregação mensal base com segmentação por valor e estrutura vigente
base_mensal AS (
    SELECT 
        -- Chaves de agrupamento
        CAST(FORMAT(f.data_ref, 'yyyyMM') AS INT) as ano_mes,
        f.crm_id, -- Mantém o crm_id para fazer o join com dim_pessoas
        -- Pega a estrutura vigente na data do evento
        COALESCE(ep.id_estrutura, f.id_estrutura) as id_estrutura,
        
        -- Métricas de Ativações
        COUNT(CASE 
            WHEN f.tipo_movimentacao = 'ativacao' 
            AND f.faixa_pl != 'ate 300k' 
            THEN 1 
        END) as qtd_ativacoes_300k_mais,
        
        COUNT(CASE 
            WHEN f.tipo_movimentacao = 'ativacao' 
            AND f.faixa_pl = 'ate 300k' 
            THEN 1 
        END) as qtd_ativacoes_300k_menos,
        
        -- Métricas de Habilitações
        COUNT(CASE 
            WHEN f.tipo_movimentacao = 'habilitacao' 
            AND f.faixa_pl != 'ate 300k' 
            THEN 1 
        END) as qtd_habilitacoes_300k_mais,
        
        COUNT(CASE 
            WHEN f.tipo_movimentacao = 'habilitacao' 
            AND f.faixa_pl = 'ate 300k' 
            THEN 1 
        END) as qtd_habilitacoes_300k_menos
        
    FROM silver.fact_ativacoes_habilitacoes_evasoes f
    -- Join para pegar a estrutura vigente do assessor na data do evento
    LEFT JOIN silver.fact_estrutura_pessoas ep
        ON ep.crm_id = f.crm_id
        AND f.data_ref >= ep.data_entrada
        AND (ep.data_saida IS NULL OR f.data_ref <= ep.data_saida)
    WHERE f.tipo_movimentacao IN ('ativacao', 'habilitacao')
    GROUP BY 
        CAST(FORMAT(f.data_ref, 'yyyyMM') AS INT),
        f.crm_id,
        COALESCE(ep.id_estrutura, f.id_estrutura)
),

-- CTE2: Enriquecimento com dados do calendário e dimensões
dados_enriquecidos AS (
    SELECT DISTINCT
        b.ano_mes,
        c.ano,
        c.mes,
        c.nome_mes,
        c.trimestre,
        CASE 
            WHEN c.mes BETWEEN 1 AND 6 THEN 'S1'
            ELSE 'S2'
        END as semestre,
        p.cod_aai as cod_assessor, -- ALTERADO: Agora usa cod_aai da dim_pessoas
        b.crm_id as crm_id_assessor, -- Mantendo o crm_id original
        COALESCE(p.nome_pessoa, 'Assessor não identificado') as nome_assessor,
        p.assessor_nivel as nivel_assessor,
        b.id_estrutura as estrutura_id,
        COALESCE(e.nome_estrutura, 'Estrutura não identificada') as estrutura_nome,
        b.qtd_ativacoes_300k_mais,
        b.qtd_ativacoes_300k_menos,
        b.qtd_habilitacoes_300k_mais,
        b.qtd_habilitacoes_300k_menos
    FROM base_mensal b
    -- Join com calendário usando o primeiro dia do mês
    INNER JOIN silver.dim_calendario c 
        ON c.ano_mes = CAST(b.ano_mes AS CHAR(6))
        AND c.dia = 1
    -- Join com pessoas para dados do assessor
    LEFT JOIN silver.dim_pessoas p 
        ON p.crm_id = b.crm_id
    -- Join com estruturas usando id_estrutura correto
    LEFT JOIN silver.dim_estruturas e 
        ON e.id_estrutura = b.id_estrutura
),

-- CTE3: Cálculo de acumulados por período (trimestre, semestre, ano)
acumulados_periodo AS (
    SELECT 
        d1.ano_mes,
        d1.ano,
        d1.mes,
        d1.nome_mes,
        d1.trimestre,
        d1.semestre,
        d1.cod_assessor,
        d1.crm_id_assessor,
        d1.nome_assessor,
        d1.nivel_assessor,
        d1.estrutura_id,
        d1.estrutura_nome,
        
        -- Métricas mensais
        d1.qtd_ativacoes_300k_mais,
        d1.qtd_ativacoes_300k_menos,
        d1.qtd_habilitacoes_300k_mais,
        d1.qtd_habilitacoes_300k_menos,
        
        -- Acumulados do trimestre
        SUM(d2.qtd_ativacoes_300k_mais) as qtd_ativacoes_300k_mais_trimestre,
        SUM(d2.qtd_ativacoes_300k_menos) as qtd_ativacoes_300k_menos_trimestre,
        SUM(d2.qtd_habilitacoes_300k_mais) as qtd_habilitacoes_300k_mais_trimestre,
        SUM(d2.qtd_habilitacoes_300k_menos) as qtd_habilitacoes_300k_menos_trimestre,
        
        -- Acumulados do semestre
        SUM(d3.qtd_ativacoes_300k_mais) as qtd_ativacoes_300k_mais_semestre,
        SUM(d3.qtd_ativacoes_300k_menos) as qtd_ativacoes_300k_menos_semestre,
        SUM(d3.qtd_habilitacoes_300k_mais) as qtd_habilitacoes_300k_mais_semestre,
        SUM(d3.qtd_habilitacoes_300k_menos) as qtd_habilitacoes_300k_menos_semestre,
        
        -- Acumulados do ano
        SUM(d4.qtd_ativacoes_300k_mais) as qtd_ativacoes_300k_mais_ano,
        SUM(d4.qtd_ativacoes_300k_menos) as qtd_ativacoes_300k_menos_ano,
        SUM(d4.qtd_habilitacoes_300k_mais) as qtd_habilitacoes_300k_mais_ano,
        SUM(d4.qtd_habilitacoes_300k_menos) as qtd_habilitacoes_300k_menos_ano
        
    FROM dados_enriquecidos d1
    
    -- Join para acumulado trimestre
    LEFT JOIN dados_enriquecidos d2 
        ON d2.cod_assessor = d1.cod_assessor 
        AND d2.ano = d1.ano 
        AND d2.trimestre = d1.trimestre
        AND d2.ano_mes <= d1.ano_mes
    
    -- Join para acumulado semestre
    LEFT JOIN dados_enriquecidos d3 
        ON d3.cod_assessor = d1.cod_assessor 
        AND d3.ano = d1.ano 
        AND d3.semestre = d1.semestre
        AND d3.ano_mes <= d1.ano_mes
    
    -- Join para acumulado ano
    LEFT JOIN dados_enriquecidos d4 
        ON d4.cod_assessor = d1.cod_assessor 
        AND d4.ano = d1.ano
        AND d4.ano_mes <= d1.ano_mes
        
    GROUP BY 
        d1.ano_mes, d1.ano, d1.mes, d1.nome_mes, d1.trimestre, d1.semestre,
        d1.cod_assessor, d1.crm_id_assessor, d1.nome_assessor, d1.nivel_assessor,
        d1.estrutura_id, d1.estrutura_nome,
        d1.qtd_ativacoes_300k_mais, d1.qtd_ativacoes_300k_menos,
        d1.qtd_habilitacoes_300k_mais, d1.qtd_habilitacoes_300k_menos
),

-- CTE4: Cálculo de janelas móveis (3, 6, 12 meses)
janelas_moveis AS (
    SELECT 
        a1.*,
        
        -- Janela móvel 3 meses
        SUM(a2.qtd_ativacoes_300k_mais) as qtd_ativacoes_300k_mais_3_meses,
        SUM(a2.qtd_ativacoes_300k_menos) as qtd_ativacoes_300k_menos_3_meses,
        SUM(a2.qtd_habilitacoes_300k_mais) as qtd_habilitacoes_300k_mais_3_meses,
        SUM(a2.qtd_habilitacoes_300k_menos) as qtd_habilitacoes_300k_menos_3_meses,
        
        -- Janela móvel 6 meses
        SUM(a3.qtd_ativacoes_300k_mais) as qtd_ativacoes_300k_mais_6_meses,
        SUM(a3.qtd_ativacoes_300k_menos) as qtd_ativacoes_300k_menos_6_meses,
        SUM(a3.qtd_habilitacoes_300k_mais) as qtd_habilitacoes_300k_mais_6_meses,
        SUM(a3.qtd_habilitacoes_300k_menos) as qtd_habilitacoes_300k_menos_6_meses,
        
        -- Janela móvel 12 meses
        SUM(a4.qtd_ativacoes_300k_mais) as qtd_ativacoes_300k_mais_12_meses,
        SUM(a4.qtd_ativacoes_300k_menos) as qtd_ativacoes_300k_menos_12_meses,
        SUM(a4.qtd_habilitacoes_300k_mais) as qtd_habilitacoes_300k_mais_12_meses,
        SUM(a4.qtd_habilitacoes_300k_menos) as qtd_habilitacoes_300k_menos_12_meses
        
    FROM acumulados_periodo a1
    
    -- Join para janela 3 meses
    LEFT JOIN dados_enriquecidos a2 
        ON a2.cod_assessor = a1.cod_assessor
        AND a2.ano_mes BETWEEN 
            CASE 
                WHEN a1.mes <= 3 THEN (a1.ano - 1) * 100 + (12 + a1.mes - 2)
                ELSE a1.ano * 100 + (a1.mes - 2)
            END
            AND a1.ano_mes
    
    -- Join para janela 6 meses
    LEFT JOIN dados_enriquecidos a3 
        ON a3.cod_assessor = a1.cod_assessor
        AND a3.ano_mes BETWEEN 
            CASE 
                WHEN a1.mes <= 6 THEN (a1.ano - 1) * 100 + (12 + a1.mes - 5)
                ELSE a1.ano * 100 + (a1.mes - 5)
            END
            AND a1.ano_mes
    
    -- Join para janela 12 meses
    LEFT JOIN dados_enriquecidos a4 
        ON a4.cod_assessor = a1.cod_assessor
        AND a4.ano_mes BETWEEN ((a1.ano - 1) * 100 + a1.mes + 1) AND a1.ano_mes
        
    GROUP BY 
        a1.ano_mes, a1.ano, a1.mes, a1.nome_mes, a1.trimestre, a1.semestre,
        a1.cod_assessor, a1.crm_id_assessor, a1.nome_assessor, a1.nivel_assessor,
        a1.estrutura_id, a1.estrutura_nome,
        a1.qtd_ativacoes_300k_mais, a1.qtd_ativacoes_300k_menos,
        a1.qtd_habilitacoes_300k_mais, a1.qtd_habilitacoes_300k_menos,
        a1.qtd_ativacoes_300k_mais_trimestre, a1.qtd_ativacoes_300k_menos_trimestre,
        a1.qtd_habilitacoes_300k_mais_trimestre, a1.qtd_habilitacoes_300k_menos_trimestre,
        a1.qtd_ativacoes_300k_mais_semestre, a1.qtd_ativacoes_300k_menos_semestre,
        a1.qtd_habilitacoes_300k_mais_semestre, a1.qtd_habilitacoes_300k_menos_semestre,
        a1.qtd_ativacoes_300k_mais_ano, a1.qtd_ativacoes_300k_menos_ano,
        a1.qtd_habilitacoes_300k_mais_ano, a1.qtd_habilitacoes_300k_menos_ano
)

-- Query final
SELECT 
    ano_mes,
    ano,
    mes,
    nome_mes,
    trimestre,
    semestre,
    cod_assessor,
    crm_id_assessor,
    nome_assessor,
    nivel_assessor,
    estrutura_id,
    estrutura_nome,
    
    -- Ativações Mensal
    COALESCE(qtd_ativacoes_300k_mais, 0) as qtd_ativacoes_300k_mais,
    COALESCE(qtd_ativacoes_300k_menos, 0) as qtd_ativacoes_300k_menos,
    
    -- Ativações Acumuladas Período
    COALESCE(qtd_ativacoes_300k_mais_trimestre, 0) as qtd_ativacoes_300k_mais_trimestre,
    COALESCE(qtd_ativacoes_300k_menos_trimestre, 0) as qtd_ativacoes_300k_menos_trimestre,
    COALESCE(qtd_ativacoes_300k_mais_semestre, 0) as qtd_ativacoes_300k_mais_semestre,
    COALESCE(qtd_ativacoes_300k_menos_semestre, 0) as qtd_ativacoes_300k_menos_semestre,
    COALESCE(qtd_ativacoes_300k_mais_ano, 0) as qtd_ativacoes_300k_mais_ano,
    COALESCE(qtd_ativacoes_300k_menos_ano, 0) as qtd_ativacoes_300k_menos_ano,
    
    -- Ativações Janela Móvel
    COALESCE(qtd_ativacoes_300k_mais_3_meses, 0) as qtd_ativacoes_300k_mais_3_meses,
    COALESCE(qtd_ativacoes_300k_menos_3_meses, 0) as qtd_ativacoes_300k_menos_3_meses,
    COALESCE(qtd_ativacoes_300k_mais_6_meses, 0) as qtd_ativacoes_300k_mais_6_meses,
    COALESCE(qtd_ativacoes_300k_menos_6_meses, 0) as qtd_ativacoes_300k_menos_6_meses,
    COALESCE(qtd_ativacoes_300k_mais_12_meses, 0) as qtd_ativacoes_300k_mais_12_meses,
    COALESCE(qtd_ativacoes_300k_menos_12_meses, 0) as qtd_ativacoes_300k_menos_12_meses,
    
    -- Habilitações Mensal
    COALESCE(qtd_habilitacoes_300k_mais, 0) as qtd_habilitacoes_300k_mais,
    COALESCE(qtd_habilitacoes_300k_menos, 0) as qtd_habilitacoes_300k_menos,
    
    -- Habilitações Acumuladas Período
    COALESCE(qtd_habilitacoes_300k_mais_trimestre, 0) as qtd_habilitacoes_300k_mais_trimestre,
    COALESCE(qtd_habilitacoes_300k_menos_trimestre, 0) as qtd_habilitacoes_300k_menos_trimestre,
    COALESCE(qtd_habilitacoes_300k_mais_semestre, 0) as qtd_habilitacoes_300k_mais_semestre,
    COALESCE(qtd_habilitacoes_300k_menos_semestre, 0) as qtd_habilitacoes_300k_menos_semestre,
    COALESCE(qtd_habilitacoes_300k_mais_ano, 0) as qtd_habilitacoes_300k_mais_ano,
    COALESCE(qtd_habilitacoes_300k_menos_ano, 0) as qtd_habilitacoes_300k_menos_ano,
    
    -- Habilitações Janela Móvel
    COALESCE(qtd_habilitacoes_300k_mais_3_meses, 0) as qtd_habilitacoes_300k_mais_3_meses,
    COALESCE(qtd_habilitacoes_300k_menos_3_meses, 0) as qtd_habilitacoes_300k_menos_3_meses,
    COALESCE(qtd_habilitacoes_300k_mais_6_meses, 0) as qtd_habilitacoes_300k_mais_6_meses,
    COALESCE(qtd_habilitacoes_300k_menos_6_meses, 0) as qtd_habilitacoes_300k_menos_6_meses,
    COALESCE(qtd_habilitacoes_300k_mais_12_meses, 0) as qtd_habilitacoes_300k_mais_12_meses,
    COALESCE(qtd_habilitacoes_300k_menos_12_meses, 0) as qtd_habilitacoes_300k_menos_12_meses
    
    
FROM janelas_moveis;
GO
