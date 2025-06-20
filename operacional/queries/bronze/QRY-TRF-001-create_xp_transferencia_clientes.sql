SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bronze].[xp_transferencia_clientes](
	[cod_xp] [int] NULL,
	[cod_aai_origem] [varchar](50) NULL,
	[cod_aai_destino] [varchar](50) NULL,
	[data_solicitacao] [date] NULL,
	[data_transferencia] [date] NULL,
	[status] [varchar](50) NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [bronze].[xp_transferencia_clientes] ADD  DEFAULT (CONVERT([date],getdate())) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do cliente sendo transferido' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', @level2type=N'COLUMN',@level2name=N'cod_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do assessor de origem que está transferindo o cliente' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', @level2type=N'COLUMN',@level2name=N'cod_aai_origem'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do assessor de destino que receberá o cliente' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', @level2type=N'COLUMN',@level2name=N'cod_aai_destino'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data em que a transferência foi solicitada' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', @level2type=N'COLUMN',@level2name=N'data_solicitacao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data em que a transferência foi efetivada' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', @level2type=N'COLUMN',@level2name=N'data_transferencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Status atual da transferência (Aprovada, Pendente, Rejeitada)' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', @level2type=N'COLUMN',@level2name=N'status'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data em que os dados foram carregados no sistema' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Dados brutos de transferências de clientes entre assessores. Registra quando um cliente foi transferido de um AAI para outro dentro da XP' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes'
GO
