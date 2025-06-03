CREATE TABLE [M7Medallion].[silver].[silver_fact_ipca] (
    data_ref    DATE NOT NULL,
    ano_mes     char(6) NOT NULL,
    ipca_mes    DECIMAL(14,8) NOT NULL,
    ipca_3m     DECIMAL(14,8) NOT NULL,
    ipca_6m     DECIMAL(14,8) NOT NULL,
    ipca_12m    DECIMAL(14,8) NOT NULL,
    CONSTRAINT fk_fato_ipca_dim_calendario FOREIGN KEY (data_ref)
        REFERENCES [M7InvestimentosOLAP].[dim].[dim_calendario](data_ref)
);
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela fato que contém os dados de IPCA. dados processados a partir da stage_ipca' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_ipca';
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do IPCA' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_ipca', @level2type=N'COLUMN',@level2name=N'data_ref';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano e mês de referência no formato AAAAMM' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_ipca', @level2type=N'COLUMN',@level2name=N'ano_mes';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor do IPCA do mês' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_ipca', @level2type=N'COLUMN',@level2name=N'ipca_mes';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor do IPCA acumulado em 3 meses' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_ipca', @level2type=N'COLUMN',@level2name=N'ipca_3m';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor do IPCA acumulado em 6 meses' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_ipca', @level2type=N'COLUMN',@level2name=N'ipca_6m';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor do IPCA acumulado em 12 meses' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_ipca', @level2type=N'COLUMN',@level2name=N'ipca_12m';
GO

