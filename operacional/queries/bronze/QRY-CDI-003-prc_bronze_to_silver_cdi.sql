USE [M7InvestimentosOLAP];
GO

CREATE OR ALTER PROCEDURE [dbo].[prc_stage2fact_cdi]
AS
BEGIN
    SET NOCOUNT ON;

    WITH base AS (
        SELECT 
            data_ref,
            CAST(FORMAT(data_ref, 'yyyyMM') AS INT) AS ano_mes,
            cdi_dia
        FROM [dbo].[stage_cdi]
    ),
    mensal AS (
        SELECT 
            ano_mes,
            EXP(SUM(LOG(1 + cdi_dia))) - 1 AS cdi_mes
        FROM base
        GROUP BY ano_mes
    ),
    acumulado_3m AS (
        SELECT 
            b1.ano_mes,
            EXP(SUM(LOG(1 + m2.cdi_mes))) - 1 AS cdi_3m
        FROM mensal b1
        JOIN mensal m2 ON m2.ano_mes BETWEEN 
            CAST(FORMAT(DATEADD(MONTH, -2, DATEFROMPARTS(b1.ano_mes / 100, b1.ano_mes % 100, 1)), 'yyyyMM') AS INT)
            AND b1.ano_mes
        GROUP BY b1.ano_mes
    ),
    acumulado_6m AS (
        SELECT 
            b1.ano_mes,
            EXP(SUM(LOG(1 + m2.cdi_mes))) - 1 AS cdi_6m
        FROM mensal b1
        JOIN mensal m2 ON m2.ano_mes BETWEEN 
            CAST(FORMAT(DATEADD(MONTH, -5, DATEFROMPARTS(b1.ano_mes / 100, b1.ano_mes % 100, 1)), 'yyyyMM') AS INT)
            AND b1.ano_mes
        GROUP BY b1.ano_mes
    ),
    acumulado_12m AS (
        SELECT 
            b1.ano_mes,
            EXP(SUM(LOG(1 + m2.cdi_mes))) - 1 AS cdi_12m
        FROM mensal b1
        JOIN mensal m2 ON m2.ano_mes BETWEEN 
            CAST(FORMAT(DATEADD(MONTH, -11, DATEFROMPARTS(b1.ano_mes / 100, b1.ano_mes % 100, 1)), 'yyyyMM') AS INT)
            AND b1.ano_mes
        GROUP BY b1.ano_mes
    ),
    final AS (
        SELECT 
            b.data_ref,
            b.ano_mes,
            b.cdi_dia,
            ROUND(m.cdi_mes, 8) AS cdi_mes,
            ROUND(a3.cdi_3m, 8) AS cdi_3m,
            ROUND(a6.cdi_6m, 8) AS cdi_6m,
            ROUND(a12.cdi_12m, 8) AS cdi_12m
        FROM base b
        JOIN mensal m ON b.ano_mes = m.ano_mes
        LEFT JOIN acumulado_3m a3 ON b.ano_mes = a3.ano_mes
        LEFT JOIN acumulado_6m a6 ON b.ano_mes = a6.ano_mes
        LEFT JOIN acumulado_12m a12 ON b.ano_mes = a12.ano_mes
    )

    INSERT INTO [dbo].[fato_cdi] (
        data_ref, ano_mes, 
        cdi_dia, cdi_mes, 
        cdi_3m, cdi_6m, cdi_12m
    )
    SELECT 
        f.data_ref, f.ano_mes, 
        f.cdi_dia, f.cdi_mes, 
        f.cdi_3m, f.cdi_6m, f.cdi_12m
    FROM final f
    WHERE NOT EXISTS (
        SELECT 1 
        FROM [dbo].[fato_cdi] fc 
        WHERE fc.data_ref = f.data_ref
    );
END;
GO
