USE [M7Medallion];
GO

CREATE OR ALTER PROCEDURE [modelagem_b2s].[prc_bronze_to_silver_ipca]
AS
BEGIN
    SET NOCOUNT ON;

    WITH base AS (
        SELECT 
            data_ref,
            CAST(FORMAT(data_ref, 'yyyyMM') AS INT) AS ano_mes,
            ipca_mes
        FROM [bronze].[bronze_ipca]
    ),
    acumulado_3m AS (
        SELECT 
            b1.data_ref,
            EXP(SUM(LOG(1 + b2.ipca_mes))) - 1 AS ipca_3m
        FROM base b1
        JOIN base b2 
            ON b2.data_ref BETWEEN DATEADD(MONTH, -2, b1.data_ref) AND b1.data_ref
        GROUP BY b1.data_ref
    ),
    acumulado_6m AS (
        SELECT 
            b1.data_ref,
            EXP(SUM(LOG(1 + b2.ipca_mes))) - 1 AS ipca_6m
        FROM base b1
        JOIN base b2 
            ON b2.data_ref BETWEEN DATEADD(MONTH, -5, b1.data_ref) AND b1.data_ref
        GROUP BY b1.data_ref
    ),
    acumulado_12m AS (
        SELECT 
            b1.data_ref,
            EXP(SUM(LOG(1 + b2.ipca_mes))) - 1 AS ipca_12m
        FROM base b1
        JOIN base b2 
            ON b2.data_ref BETWEEN DATEADD(MONTH, -11, b1.data_ref) AND b1.data_ref
        GROUP BY b1.data_ref
    )

    INSERT INTO [fato].[fato_ipca] (
        data_ref, ano_mes, ipca_mes, ipca_3m, ipca_6m, ipca_12m
    )
    SELECT 
        b.data_ref,
        b.ano_mes,
        b.ipca_mes,
        ROUND(a3.ipca_3m, 6),
        ROUND(a6.ipca_6m, 6),
        ROUND(a12.ipca_12m, 6)
    FROM base b
    LEFT JOIN acumulado_3m a3 ON b.data_ref = a3.data_ref
    LEFT JOIN acumulado_6m a6 ON b.data_ref = a6.data_ref
    LEFT JOIN acumulado_12m a12 ON b.data_ref = a12.data_ref
    WHERE NOT EXISTS (
        SELECT 1
        FROM [fato].[fato_ipca] f
        WHERE f.data_ref = b.data_ref
    );
END;
GO
