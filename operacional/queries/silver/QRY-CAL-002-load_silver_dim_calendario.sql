-- Povoar a tabela dim_calendario
SET DATEFORMAT YMD;  

WITH CTE_Datas AS (
    SELECT CAST('2023-01-01' AS DATE) AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt)
    FROM CTE_Datas
    WHERE dt < '2025-12-31'
)
INSERT INTO [M7InvestimentosOLAP].[dim].[dim_calendario]
(
    data_ref,
    dia,
    mes,
    ano,
    ano_mes,
    nome_mes,
    trimestre,
    numero_da_semana,
    dia_da_semana,
    dia_da_semana_num,
    tipo_dia
)
SELECT
    dt AS data,
    DAY(dt)                       AS dia,
    MONTH(dt)                     AS mes,
    YEAR(dt)                      AS ano,
    CONVERT(CHAR(6), YEAR(dt)*100 + MONTH(dt)) AS ano_mes,
    DATENAME(MONTH, dt)           AS nome_mes,     
    CASE 
        WHEN MONTH(dt) IN (1,2,3)    THEN 'Q1'
        WHEN MONTH(dt) IN (4,5,6)    THEN 'Q2'
        WHEN MONTH(dt) IN (7,8,9)    THEN 'Q3'
        WHEN MONTH(dt) IN (10,11,12) THEN 'Q4'
    END                            AS trimestre,
    DATEPART(WEEK, dt)            AS numero_da_semana,
    DATENAME(WEEKDAY, dt)         AS dia_da_semana,
    -- segunda = 1, domingo = 7
    CASE DATEPART(WEEKDAY, dt)
        WHEN 1 THEN 7
        ELSE DATEPART(WEEKDAY, dt) - 1
    END                            AS dia_da_semana_num,
    CASE
        WHEN dt IN (
            -- feriados
            '2025-01-01',  
            '2025-03-03',  
            '2025-03-04', 
            '2025-04-18',  
            '2025-04-21', 
            '2025-05-01',  
            '2025-09-07',  
            '2025-10-12', 
            '2025-11-02', 
            '2025-11-15',  
            '2025-12-25'   
        )
            THEN 'Feriado'
        WHEN DATENAME(WEEKDAY, dt) = 'Saturday' THEN 'Sábado'
        WHEN DATENAME(WEEKDAY, dt) = 'Sunday'   THEN 'Domingo'
        ELSE 'Útil'
    END                            AS tipo_dia
FROM CTE_Datas
OPTION (MAXRECURSION 0); 