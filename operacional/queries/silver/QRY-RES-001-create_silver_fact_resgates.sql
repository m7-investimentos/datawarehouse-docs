SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [silver].[fact_resgates](
    [data_ref] [date] NOT NULL,
    [conta_xp_cliente] [int] NOT NULL,
    [cod_assessor] [varchar](50) NOT NULL,
    [origem_resgate] [varchar](100) NOT NULL,
    [resgate_bruto_xp] [decimal](18, 2) NOT NULL,
    [tipo_transferencia] [varchar](100) NOT NULL,
    [resgate_bruto_transferencia] [decimal](18, 2) NOT NULL,
    [resgate_bruto_total] [decimal](18, 2) NOT NULL
) ON [PRIMARY]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do registro' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código da conta do cliente no sistema xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'conta_xp_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código do assessor no sistema da xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'origem do resgate (ted, prev, ota....)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'origem_resgate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor dos resgates brutos' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'resgate_bruto_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tipo de transferência de saida, nesse caso, todas sao saida' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'tipo_transferencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor da transferência de saida. quanto de patrimonio o cliente tinha quando foi transferido para a M7' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'resgate_bruto_transferencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate bruto total = valor dos resgates brutos xp + valor da transferência de saida' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'resgate_bruto_total'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela silver que contém os dados de resgates e transferencias de saida. apenas os valores de saida são considerados nessa tabela' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates'
GO