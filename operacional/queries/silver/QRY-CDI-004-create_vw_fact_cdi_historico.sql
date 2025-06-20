SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   VIEW [silver].[vw_fact_cdi_historico]
AS
WITH CTE_dias_numerados AS (
    -- Numera os dias dentro de cada mês
    SELECT 
        data_ref,
        YEAR(data_ref) AS ano,
        MONTH(data_ref) AS mes_num,
        taxa_cdi / 100.0 AS taxa_cdi_dia,
        ROW_NUMBER() OVER (PARTITION BY YEAR(data_ref), MONTH(data_ref) ORDER BY data_ref) AS dia_no_mes
    FROM [bronze].[bc_cdi_historico]
),
CTE_com_lags_diarios AS (
    -- Cria LAGs para cada dia do mês
    SELECT 
        *,
        LAG(taxa_cdi_dia, 1) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag1,
        LAG(taxa_cdi_dia, 2) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag2,
        LAG(taxa_cdi_dia, 3) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag3,
        LAG(taxa_cdi_dia, 4) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag4,
        LAG(taxa_cdi_dia, 5) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag5,
        LAG(taxa_cdi_dia, 6) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag6,
        LAG(taxa_cdi_dia, 7) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag7,
        LAG(taxa_cdi_dia, 8) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag8,
        LAG(taxa_cdi_dia, 9) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag9,
        LAG(taxa_cdi_dia, 10) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag10,
        LAG(taxa_cdi_dia, 11) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag11,
        LAG(taxa_cdi_dia, 12) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag12,
        LAG(taxa_cdi_dia, 13) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag13,
        LAG(taxa_cdi_dia, 14) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag14,
        LAG(taxa_cdi_dia, 15) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag15,
        LAG(taxa_cdi_dia, 16) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag16,
        LAG(taxa_cdi_dia, 17) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag17,
        LAG(taxa_cdi_dia, 18) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag18,
        LAG(taxa_cdi_dia, 19) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag19,
        LAG(taxa_cdi_dia, 20) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag20,
        LAG(taxa_cdi_dia, 21) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag21,
        LAG(taxa_cdi_dia, 22) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag22
    FROM CTE_dias_numerados
),
CTE_dias_para_mes AS (
    -- Calcula a taxa acumulada do mês usando CASE
    SELECT 
        data_ref,
        ano,
        mes_num,
        taxa_cdi_dia,
        
        CASE dia_no_mes
            WHEN 1 THEN taxa_cdi_dia
            WHEN 2 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0))) - 1
            WHEN 3 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0))) - 1
            WHEN 4 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0))) - 1
            WHEN 5 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0))) - 1
            WHEN 6 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0))) - 1
            WHEN 7 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0))) - 1
            WHEN 8 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0))) - 1
            WHEN 9 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0))) - 1
            WHEN 10 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0))) - 1
            WHEN 11 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0))) - 1
            WHEN 12 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0))) - 1
            WHEN 13 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0))) - 1
            WHEN 14 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0))) - 1
            WHEN 15 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0))) - 1
            WHEN 16 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0))) - 1
            WHEN 17 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0))) - 1
            WHEN 18 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0)) * (1 + COALESCE(lag17, 0))) - 1
            WHEN 19 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0)) * (1 + COALESCE(lag17, 0)) * (1 + COALESCE(lag18, 0))) - 1
            WHEN 20 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0)) * (1 + COALESCE(lag17, 0)) * (1 + COALESCE(lag18, 0)) * (1 + COALESCE(lag19, 0))) - 1
            WHEN 21 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0)) * (1 + COALESCE(lag17, 0)) * (1 + COALESCE(lag18, 0)) * (1 + COALESCE(lag19, 0)) * (1 + COALESCE(lag20, 0))) - 1
            WHEN 22 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0)) * (1 + COALESCE(lag17, 0)) * (1 + COALESCE(lag18, 0)) * (1 + COALESCE(lag19, 0)) * (1 + COALESCE(lag20, 0)) * (1 + COALESCE(lag21, 0))) - 1
            ELSE ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0)) * (1 + COALESCE(lag17, 0)) * (1 + COALESCE(lag18, 0)) * (1 + COALESCE(lag19, 0)) * (1 + COALESCE(lag20, 0)) * (1 + COALESCE(lag21, 0)) * (1 + COALESCE(lag22, 0))) - 1
        END AS taxa_cdi_mes_acum
        
    FROM CTE_com_lags_diarios
),
CTE_mes_final AS (
    -- Pega apenas o último dia de cada mês (que tem a taxa mensal completa)
    SELECT 
        ano,
        mes_num,
        MAX(data_ref) AS ultimo_dia_mes,
        MAX(taxa_cdi_mes_acum) AS taxa_cdi_mes
    FROM CTE_dias_para_mes
    GROUP BY ano, mes_num
),
CTE_base_mensal AS (
    -- Base mensal para cálculos acumulados
    SELECT 
        ano,
        mes_num,
        taxa_cdi_mes,
        
        -- Criando ano_mes no formato YYYYMM (6 dígitos)
        CAST(ano * 100 + mes_num AS INT) AS ano_mes,
        
        -- Trimestre
        CASE 
            WHEN mes_num BETWEEN 1 AND 3 THEN 'Q1'
            WHEN mes_num BETWEEN 4 AND 6 THEN 'Q2'
            WHEN mes_num BETWEEN 7 AND 9 THEN 'Q3'
            WHEN mes_num BETWEEN 10 AND 12 THEN 'Q4'
        END AS trimestre,
        
        -- Semestre
        CASE 
            WHEN mes_num BETWEEN 1 AND 6 THEN 'S1'
            WHEN mes_num BETWEEN 7 AND 12 THEN 'S2'
        END AS semestre
        
    FROM CTE_mes_final
),
CTE_com_acumulados AS (
    SELECT 
        *,
        -- Taxa acumulada 3 meses (janela móvel)
        CAST(
            ((1 + COALESCE(taxa_cdi_mes, 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (ORDER BY ano_mes), 0))
            ) - 1 
        AS DECIMAL(18,8)) AS taxa_cdi_3_meses,
        
        -- Taxa acumulada 6 meses (janela móvel)
        CAST(
            ((1 + COALESCE(taxa_cdi_mes, 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (ORDER BY ano_mes), 0))
            ) - 1 
        AS DECIMAL(18,8)) AS taxa_cdi_6_meses,
        
        -- Taxa acumulada 12 meses (janela móvel)
        CAST(
            ((1 + COALESCE(taxa_cdi_mes, 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 7) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 8) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 9) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 10) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 11) OVER (ORDER BY ano_mes), 0))
            ) - 1 
        AS DECIMAL(18,8)) AS taxa_cdi_12_meses,
        
        -- Taxa acumulada do trimestre
        CASE 
            WHEN mes_num IN (1,4,7,10) THEN taxa_cdi_mes
            WHEN mes_num IN (2,5,8,11) THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num IN (3,6,9,12) THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
        END AS taxa_cdi_trimestre,
        
        -- Taxa acumulada do semestre
        CASE 
            WHEN mes_num IN (1,7) THEN taxa_cdi_mes
            WHEN mes_num = 2 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 3 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 4 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 5 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 6 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 8 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 9 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 10 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 11 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 12 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
        END AS taxa_cdi_semestre,
        
        -- Taxa acumulada do ano
        CASE 
            WHEN mes_num = 1 THEN taxa_cdi_mes
            WHEN mes_num = 2 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 3 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 4 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 5 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 6 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 7 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 8 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 7) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 9 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 7) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 8) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 10 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 7) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 8) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 9) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 11 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 7) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 8) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 9) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 10) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 12 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 7) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 8) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 9) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 10) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 11) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
        END AS taxa_cdi_ano
        
    FROM CTE_base_mensal
)
-- JOIN final para trazer os dados diários junto com os cálculos mensais
SELECT 
    d.data_ref,
    FORMAT(d.data_ref, 'yyyyMM') AS ano_mes,
    YEAR(d.data_ref) AS ano,
    MONTH(d.data_ref) AS mes_num,
    m.trimestre,
    m.semestre,
    
    -- Taxa do dia
    CAST(d.taxa_cdi / 100.0 AS DECIMAL(18,8)) AS taxa_cdi_dia,
    
    -- Taxas acumuladas (todas baseadas nos cálculos mensais)
    CAST(m.taxa_cdi_mes AS DECIMAL(18,8)) AS taxa_cdi_mes,
    CAST(m.taxa_cdi_3_meses AS DECIMAL(18,8)) AS taxa_cdi_3_meses,
    CAST(m.taxa_cdi_6_meses AS DECIMAL(18,8)) AS taxa_cdi_6_meses,
    CAST(m.taxa_cdi_12_meses AS DECIMAL(18,8)) AS taxa_cdi_12_meses,
    CAST(m.taxa_cdi_trimestre AS DECIMAL(18,8)) AS taxa_cdi_trimestre,
    CAST(m.taxa_cdi_semestre AS DECIMAL(18,8)) AS taxa_cdi_semestre,
    CAST(m.taxa_cdi_ano AS DECIMAL(18,8)) AS taxa_cdi_ano
    
FROM [bronze].[bc_cdi_historico] d
INNER JOIN CTE_com_acumulados m
    ON YEAR(d.data_ref) = m.ano 
    AND MONTH(d.data_ref) = m.mes_num
GO
