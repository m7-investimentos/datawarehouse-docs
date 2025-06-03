CREATE TABLE [M7Medallion].[silver].[silver_fact_crm_deals_novas_captacoes] (
    id_deal VARCHAR(20) NOT NULL,
    stage_id VARCHAR(50) NULL,
    opportunity VARCHAR(200) NULL,
    date_modify DATE NULL,
    stage_semantic_id VARCHAR(200) NULL,
    closedate DATE NULL
);
GO


-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'tabela de deals de novas captações vinda do CRM Bitrix24, acontecimentos que evoluem com o passar do tempo, por isso fato.', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_crm_deals_novas_captacoes';
GO

-- Descrições dos campos
EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'id do deal, identificador único do deal no CRM Bitrix24', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'id_deal';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'id do estágio do deal no funil de vendas', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'stage_id';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'valor de oportunidade do deal', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'opportunity';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'data da última modificação do deal', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'date_modify';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'id semântico do estágio (sigla)', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'stage_semantic_id';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'data de fechamento do deal', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_crm_deals_novas_captacoes', 
@level2type=N'COLUMN',
@level2name=N'closedate';
GO


