SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [silver].[prc_load_silver_fact_patrimonio]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        TRUNCATE TABLE [M7Medallion].[silver].[fact_patrimonio];
        
        INSERT INTO [M7Medallion].[silver].[fact_patrimonio] (
            data_ref,
            conta_xp_cliente,
            patrimonio_xp,
            patrimonio_declarado,
            share_of_wallet,
            patrimonio_open_investment
        )
        SELECT 
            pos.data_ref,
            TRY_CAST(pos.cod_xp AS INT) AS conta_xp_cliente,
            pos.net_em_M AS patrimonio_xp,
            
            -- patrimonio_declarado: rpa.patrimonio já é DECIMAL(18,2)
            CASE 
                WHEN rpa.patrimonio IS NULL OR rpa.patrimonio = 0 THEN NULL
                ELSE rpa.patrimonio
            END AS patrimonio_declarado,
            
            -- share_of_wallet: divisão simples entre patrimonio_xp e patrimonio_declarado
            CASE 
                WHEN rpa.patrimonio IS NULL OR rpa.patrimonio = 0 OR pos.net_em_M IS NULL THEN NULL
                ELSE pos.net_em_M / rpa.patrimonio
            END AS share_of_wallet,
            
            oin.patrimonio_open_investment
            
        FROM 
            [M7Medallion].[bronze].[xp_positivador] pos
            
        LEFT JOIN (
            SELECT 
                cod_xp,
                patrimonio,
                ROW_NUMBER() OVER (PARTITION BY cod_xp ORDER BY data_carga DESC) as rn
            FROM [M7Medallion].[bronze].[xp_rpa_clientes]
            WHERE cod_xp IS NOT NULL
        ) rpa ON TRY_CAST(pos.cod_xp AS INT) = rpa.cod_xp AND rpa.rn = 1
        
        LEFT JOIN (
            SELECT 
                cod_conta,
                SUM(valor_bruto) AS patrimonio_open_investment
            FROM [M7Medallion].[bronze].[xp_open_investment_extrato]
            WHERE cod_conta IS NOT NULL
            GROUP BY cod_conta
        ) oin ON TRY_CAST(pos.cod_xp AS INT) = oin.cod_conta
        
        WHERE 
            pos.cod_xp IS NOT NULL
            AND pos.data_ref IS NOT NULL
            AND TRY_CAST(pos.cod_xp AS INT) IS NOT NULL;
            
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END;
GO
