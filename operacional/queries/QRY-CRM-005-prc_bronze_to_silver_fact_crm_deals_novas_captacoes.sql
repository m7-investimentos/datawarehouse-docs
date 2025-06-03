USE [M7Medallion];
GO

CREATE OR ALTER PROCEDURE [dbo].[prc_bronze_to_silver_fact_crm_deals_novas_captacoes]
AS
BEGIN
    SET NOCOUNT ON;

    -- Limpa a tabela silver
    TRUNCATE TABLE [M7Medallion].[silver].[silver_fact_crm_deals_novas_captacoes];

    -- Insere dados da bronze para silver (apenas renomeando id para id_deal)
    INSERT INTO [M7Medallion].[silver].[silver_fact_crm_deals_novas_captacoes]
    (
        id_deal,
        stage_id,
        opportunity,
        date_modify,
        stage_semantic_id,
        closedate
    )
    SELECT 
        id AS id_deal,
        stage_id,
        opportunity,
        date_modify,
        stage_semantic_id,
        closedate

    FROM [M7Medallion].[bronze].[bronze_crm_deals_novas_captacoes];

END;
GO

