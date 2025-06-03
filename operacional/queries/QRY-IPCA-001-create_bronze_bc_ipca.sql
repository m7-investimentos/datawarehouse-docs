CREATE TABLE [M7InvestimentosOLAP].[stage].[stage_ipca] (
    data_carga DATE NOT NULL DEFAULT (CAST(GETDATE() AS DATE)),
    data_ref DATE NOT NULL,
    ipca_mes DECIMAL(10,8) NOT NULL,
    CONSTRAINT fk_stage_ipca_dim_calendario FOREIGN KEY (data_ref)
        REFERENCES [M7InvestimentosOLAP].[dim].[dim_calendario](data_ref)
);
GO

USE [M7InvestimentosOLAP]
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela de stage que contém os dados de IPCA. obtida a partir da API do banco central do brasil' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_ipca'
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data em que o registro foi carregado' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_ipca', @level2type=N'COLUMN',@level2name=N'data_carga';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do IPCA' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_ipca', @level2type=N'COLUMN',@level2name=N'data_ref';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor do IPCA do mês' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_ipca', @level2type=N'COLUMN',@level2name=N'ipca_mes';
GO




