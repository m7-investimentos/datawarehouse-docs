CREATE TABLE [M7Medallion].[silver].[silver_dim_crm_deals_novas_captacoes] (
    id_deal VARCHAR(20) NOT NULL,
    date_create DATE NULL,
    title VARCHAR(500) NULL,
    company_id VARCHAR(20) NULL,
    contact_id VARCHAR(20) NULL,
    assigned_by_id VARCHAR(20) NULL,
    lead_id VARCHAR(20) NULL,
    comments VARCHAR(MAX) NULL,
    source_id VARCHAR(300) NULL,
    source_description VARCHAR(500) NULL,
    origin_id VARCHAR(20) NULL
);
GO


EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'tabela de deals de novas captações vinda do CRM Bitrix24', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_dim_crm_deals_novas_captacoes';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'id do deal, identificador único do deal no CRM Bitrix24', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_dim_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'id_deal';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'data de criação do deal', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_dim_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'date_create';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'título do deal', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_dim_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'title';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'id da empresa, identificador único da empresa no CRM Bitrix24', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_dim_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'company_id';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'id do contato, identificador único do contato no CRM Bitrix24', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_dim_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'contact_id';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'id do usuário atribuído ao deal, identificador único do assessor comercial atribuído ao deal no CRM Bitrix24', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_dim_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'assigned_by_id';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'id do lead, identificador único do lead no CRM Bitrix24', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_dim_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'lead_id';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'comentários adicionais sobre o deal', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_dim_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'comments';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'id da fonte do deal, identificador único da fonte do deal no CRM Bitrix24', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_dim_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'source_id';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'descrição da fonte do deal', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_dim_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'source_description';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'id da origem do deal, identificador único da origem do deal no CRM Bitrix24', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_dim_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'origin_id';
GO


