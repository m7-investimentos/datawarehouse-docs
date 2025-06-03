USE [M7Medallion];
GO

CREATE OR ALTER PROCEDURE [dbo].[prc_bronze_to_silver_dim_crm_deals_novas_captacoes]
AS
BEGIN
    SET NOCOUNT ON;

    -- Limpa a tabela silver
    TRUNCATE TABLE [M7Medallion].[silver].[silver_dim_crm_deals_novas_captacoes];

    -- Insere dados da bronze para silver (apenas renomeando id para id_deal)
    INSERT INTO [M7Medallion].[silver].[silver_dim_crm_deals_novas_captacoes]
    (
        id_deal,
        date_create,
        title,
        company_id,
        contact_id,
        assigned_by_id,
        lead_id,
        comments,
        source_id,
        source_description,
        origin_id
    )
    SELECT 
        id AS id_deal,
        date_create,
        title,
        company_id,
        contact_id,
        assigned_by_id,
        lead_id,
        comments,
        source_id,
        source_description,
        origin_id
    FROM [M7Medallion].[bronze].[bronze_crm_deals_novas_captacoes];

END;
GO

