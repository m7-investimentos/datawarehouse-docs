USE [M7InvestimentosOLAP];
GO

CREATE OR ALTER PROCEDURE [dbo].[prc_insert_cod_xp_missing]
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO [M7InvestimentosOLAP].[DS].[dim_clientes] (cod_xp)
    SELECT DISTINCT s.cod_xp
    FROM [M7InvestimentosOLAP].[stage].[stage_positivador_layer2] s
    LEFT JOIN [M7InvestimentosOLAP].[DS].[dim_clientes] d ON s.cod_xp = d.cod_xp
    WHERE d.cod_xp IS NULL;
END;
GO 