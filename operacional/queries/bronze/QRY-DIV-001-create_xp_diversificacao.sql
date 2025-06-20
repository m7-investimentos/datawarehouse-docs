SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bronze].[xp_diversificacao](
	[data_ref] [date] NOT NULL,
	[cod_aai] [varchar](50) NULL,
	[cod_xp] [int] NOT NULL,
	[produto] [varchar](50) NULL,
	[sub_produto] [varchar](50) NULL,
	[produto_em_garantia] [varchar](10) NULL,
	[cnpj_fundo] [varchar](14) NULL,
	[ativo] [varchar](255) NULL,
	[emissor] [varchar](255) NULL,
	[data_de_vencimento] [date] NULL,
	[quantidade] [decimal](18, 4) NULL,
	[net] [decimal](18, 4) NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [bronze].[xp_diversificacao] ADD  DEFAULT (CONVERT([date],getdate())) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de referência dos dados' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código único que identifica o assessor do cliente' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao', @level2type=N'COLUMN',@level2name=N'cod_aai'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Número da conta do cliente na XP' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao', @level2type=N'COLUMN',@level2name=N'cod_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Produto do ativo' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao', @level2type=N'COLUMN',@level2name=N'produto'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Sub-produto do ativo' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao', @level2type=N'COLUMN',@level2name=N'sub_produto'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Indicador se o produto está em garantia' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao', @level2type=N'COLUMN',@level2name=N'produto_em_garantia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'CNPJ do fundo' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao', @level2type=N'COLUMN',@level2name=N'cnpj_fundo'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome do ativo' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao', @level2type=N'COLUMN',@level2name=N'ativo'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome do emissor do ativo' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao', @level2type=N'COLUMN',@level2name=N'emissor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de vencimento do ativo' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao', @level2type=N'COLUMN',@level2name=N'data_de_vencimento'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade do ativo' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao', @level2type=N'COLUMN',@level2name=N'quantidade'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Valor líquido do ativo' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao', @level2type=N'COLUMN',@level2name=N'net'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data em que o registro foi carregado' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela de stage que contém os dados de diversificação dos clientes. Obtida nos relatórios de operações no Hub XP' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_diversificacao'
GO
