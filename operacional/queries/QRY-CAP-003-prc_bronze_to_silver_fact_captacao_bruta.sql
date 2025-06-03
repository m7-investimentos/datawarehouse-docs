USE [M7Medallion];
GO

CREATE OR ALTER PROCEDURE [dbo].[prc_bronze_to_silver_captacao_bruta]
AS
BEGIN
    SET NOCOUNT ON;
    
    TRUNCATE TABLE [M7Medallion].[silver].[silver_fact_captacao_bruta];
    
    INSERT INTO [M7Medallion].[silver].[silver_fact_captacao_bruta] (
        data_ref,
        conta_xp_cliente,
        cod_assessor,
        origem_captacao,
        valor_captacao
    )
    SELECT 
        s.data_ref,
        s.cod_xp AS conta_xp_cliente,
        s.cod_aai AS cod_assessor,
        s.tipo_de_captacao AS origem_captacao,
        s.valor_captacao AS valor_captacao 
    FROM [M7Medallion].[bronze].[bronze_xp_captacao] s
    WHERE s.data_ref IS NOT NULL
      AND s.cod_xp IS NOT NULL
      AND s.cod_aai IS NOT NULL
      AND s.tipo_de_captacao IS NOT NULL
      AND s.valor_captacao IS NOT NULL
      AND s.sinal_captacao IS NOT NULL;
    
END;
GO

-- Adicionar descrição/comentário para a procedure
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Procedure responsável por transformar dados da camada Bronze para a camada Silver. Realiza limpeza e mapeamento de colunas: cod_xp→conta_xp_cliente, cod_aai→cod_assessor, tipo_de_captacao→origem_captacao.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'PROCEDURE', @level1name = N'prc_bronze_to_silver_captacao_bruta';
GO