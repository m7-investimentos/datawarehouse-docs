CREATE TABLE [M7InvestimentosOLAP].[stage].[stage_cdi] (
    data_carga DATE NOT NULL DEFAULT (CAST(GETDATE() AS DATE)),
    data_ref DATE NOT NULL,
    cdi_dia DECIMAL(10,8) NOT NULL,
    CONSTRAINT fk_stage_cdi_dim_calendario FOREIGN KEY (data_ref)
        REFERENCES [M7InvestimentosOLAP].[dim].[dim_calendario](data_ref)
);
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela de stage que contém os valores do CDI diário para processamento. esses dados são obtidos da API do Banco Central do Brasil' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_cdi';
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data em que o registro foi carregado' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_cdi', @level2type=N'COLUMN',@level2name=N'data_carga';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do CDI' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_cdi', @level2type=N'COLUMN',@level2name=N'data_ref';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor do CDI do dia' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_cdi', @level2type=N'COLUMN',@level2name=N'cdi_dia';
GO





