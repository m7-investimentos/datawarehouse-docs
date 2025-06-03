CREATE TABLE [M7InvestimentosOLAP].[fato].[fato_cdi] (
    data_ref    DATE NOT NULL,
    ano_mes     INT NOT NULL,
    cdi_dia     DECIMAL(14,8) NOT NULL,
    cdi_mes     DECIMAL(14,8) NOT NULL,
    cdi_3m      DECIMAL(14,8) NOT NULL,
    cdi_6m      DECIMAL(14,8) NOT NULL,
    cdi_12m     DECIMAL(14,8) NOT NULL,
    CONSTRAINT fk_fato_cdi_dim_calendario FOREIGN KEY (data_ref)
        REFERENCES [M7InvestimentosOLAP].[dim].[dim_calendario](data_ref)
);
GO

CREATE UNIQUE INDEX ix_fato_cdi_data_ref ON [M7InvestimentosOLAP].[fato].[fato_cdi] (data_ref);
GO


-- adicionar comentarios
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela fato que contém os valores do CDI em diferentes períodos.' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_cdi';
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do CDI' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_cdi', @level2type=N'COLUMN',@level2name=N'data_ref';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano e mês no formato AAAAMM' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_cdi', @level2type=N'COLUMN',@level2name=N'ano_mes';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor do CDI do dia' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_cdi', @level2type=N'COLUMN',@level2name=N'cdi_dia';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor do CDI acumulado no mês' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_cdi', @level2type=N'COLUMN',@level2name=N'cdi_mes';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor do CDI acumulado em 3 meses' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_cdi', @level2type=N'COLUMN',@level2name=N'cdi_3m';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor do CDI acumulado em 6 meses' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_cdi', @level2type=N'COLUMN',@level2name=N'cdi_6m';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor do CDI acumulado em 12 meses' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_cdi', @level2type=N'COLUMN',@level2name=N'cdi_12m';
GO