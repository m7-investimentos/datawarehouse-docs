SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [silver].[fact_captacao_bruta](
    [data_ref] [date] NOT NULL,
    [conta_xp_cliente] [int] NOT NULL,
    [cod_assessor] [varchar](50) NOT NULL,
    [origem_captacao] [varchar](100) NOT NULL,
    [captacao_bruta_xp] [decimal](18, 2) NOT NULL,
    [tipo_transferencia] [varchar](100) NOT NULL,
    [captacao_bruta_transferencia] [decimal](18, 2) NOT NULL,
    [captacao_bruta_total] [decimal](18, 2) NOT NULL
) ON [PRIMARY]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do registro' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código da conta do cliente no sistema xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', @level2type=N'COLUMN',@level2name=N'conta_xp_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código do assessor no sistema da xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'origem da captação (ted, prev, ota....)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', @level2type=N'COLUMN',@level2name=N'origem_captacao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor da captação parcial ótica xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', @level2type=N'COLUMN',@level2name=N'captacao_bruta_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tipo de transferência de entrada, se é nova conta ou transferencia de escritorio' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', @level2type=N'COLUMN',@level2name=N'tipo_transferencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor da transferência de entrada. quanto de patrimonio o cliente tinha quando foi transferido para a M7' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', @level2type=N'COLUMN',@level2name=N'captacao_bruta_transferencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captacao bruta = valor da captação parcial + valor da transferência de entrada' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', @level2type=N'COLUMN',@level2name=N'captacao_bruta_total'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela silver que contém os dados de captação bruta por cliente com seu respectivo assessor aai. apenas os valores de captação bruta são considerados nessa tabela, os valores de resgate não são considerados. essa tabela é oriunda dos relatórios de operações no hub da xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_captacao_bruta'
GO