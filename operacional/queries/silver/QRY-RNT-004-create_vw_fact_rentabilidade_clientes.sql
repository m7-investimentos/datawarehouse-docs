SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   VIEW [silver].[vw_fact_rentabilidade_clientes]
AS
WITH CTE_prioridade AS (
    -- Primeiro, vamos identificar qual é a data_relatorio mais recente para cada ano_mes
    SELECT 
        conta_xp_cliente,
        ano,
        mes_num,
        mes,
        portfolio_rentabilidade,
        acumulado_ano,
        data_relatorio,
        data_carga,
        -- Ranking por data_relatorio mais recente para cada cliente/ano/mes
        ROW_NUMBER() OVER (
            PARTITION BY conta_xp_cliente, ano, mes_num 
            ORDER BY data_relatorio DESC, data_carga DESC
        ) AS rn
    FROM [M7Medallion].[bronze].[xperformance_rentabilidade_cliente]
),
CTE_base AS (
    SELECT 
        -- Identificação do cliente
        conta_xp_cliente,
        
        -- Dimensões temporais básicas
        ano,
        mes_num,
        mes,
        
        -- Campos calculados de período
        CAST(ano * 100 + mes_num AS INT) AS ano_mes,  -- Formato YYYYMM (6 dígitos)
        CASE 
            WHEN mes_num BETWEEN 1 AND 6 THEN 'S1'
            WHEN mes_num BETWEEN 7 AND 12 THEN 'S2'
        END AS semestre,
        CASE 
            WHEN mes_num BETWEEN 1 AND 3 THEN 'Q1'
            WHEN mes_num BETWEEN 4 AND 6 THEN 'Q2'
            WHEN mes_num BETWEEN 7 AND 9 THEN 'Q3'
            WHEN mes_num BETWEEN 10 AND 12 THEN 'Q4'
        END AS trimestre,
        
        -- Métricas de rentabilidade (convertendo de percentual para decimal)
        CAST(portfolio_rentabilidade / 100.0 AS DECIMAL(18,8)) AS rentabilidade,
        
        -- Metadados para auditoria
        data_relatorio,
        data_carga
        
    FROM CTE_prioridade
    WHERE rn = 1  -- Apenas o registro mais recente para cada cliente/ano/mes
),
CTE_com_acumulados AS (
    SELECT 
        *,
        -- Rentabilidade acumulada do trimestre (produto dos meses do trimestre atual)
        CASE 
            WHEN mes_num IN (1,4,7,10) THEN rentabilidade
            WHEN mes_num IN (2,5,8,11) THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num IN (3,6,9,12) THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
        END AS rent_acum_trimestre,
        
        -- Rentabilidade acumulada do semestre (produto dos meses do semestre atual)
        CASE 
            WHEN mes_num IN (1,7) THEN rentabilidade
            WHEN mes_num = 2 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 3 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 4 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 5 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 6 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            -- Repete a lógica para o segundo semestre
            WHEN mes_num = 8 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 9 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 10 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 11 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 12 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
        END AS rent_acum_semestre
    FROM CTE_base
)
SELECT 
    conta_xp_cliente,
    ano_mes,
    ano,
    semestre,
    trimestre,
    mes_num,
    mes,
    rentabilidade,
    
    -- Rentabilidade acumulada 3 meses (janela móvel)
    CAST(
        ((1 + COALESCE(rentabilidade, 0))
         * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
        ) - 1 
    AS DECIMAL(18,8)) AS rentabilidade_acumulada_3_meses,
    
    -- Rentabilidade acumulada 6 meses (janela móvel)
    CAST(
        ((1 + COALESCE(rentabilidade, 0))
         * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
        ) - 1 
    AS DECIMAL(18,8)) AS rentabilidade_acumulada_6_meses,
    
    -- Rentabilidade acumulada 12 meses (janela móvel)
    CAST(
        ((1 + COALESCE(rentabilidade, 0))
         * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 7) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 8) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 9) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 10) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 11) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
        ) - 1 
    AS DECIMAL(18,8)) AS rentabilidade_acumulada_12_meses,
    
    -- Rentabilidades acumuladas de período fiscal
    CAST(rent_acum_trimestre AS DECIMAL(18,8)) AS rentabilidade_acumulada_trimestre,
    CAST(rent_acum_semestre AS DECIMAL(18,8)) AS rentabilidade_acumulada_semestre,
    
    -- Rentabilidade acumulada do ano (calculada, não da bronze)
    CAST(
        CASE 
            WHEN mes_num = 1 THEN rentabilidade
            WHEN mes_num = 2 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 3 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 4 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 5 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 6 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 7 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 8 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 7) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 9 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 7) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 8) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 10 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 7) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 8) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 9) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 11 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 7) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 8) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 9) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 10) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 12 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 7) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 8) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 9) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 10) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 11) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
        END
    AS DECIMAL(18,8)) AS rentabilidade_acumulada_ano
    
FROM CTE_com_acumulados
GO
