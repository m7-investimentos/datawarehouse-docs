SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bronze].[xp_iea](
	[ano_mes] [varchar](6) NOT NULL,
	[cod_assessor] [varchar](20) NOT NULL,
	[iea_final] [decimal](9, 6) NOT NULL,
	[captacao_liquida] [decimal](18, 2) NOT NULL,
	[esforco_prospeccao] [decimal](9, 6) NOT NULL,
	[captacao_de_novos_clientes_por_aai] [decimal](16, 2) NOT NULL,
	[atingimento_lead_starts] [decimal](9, 6) NOT NULL,
	[atingimento_habilitacoes] [decimal](9, 6) NOT NULL,
	[atingimento_conversao] [decimal](9, 6) NOT NULL,
	[atingimento_carteiras_simuladas_novos] [decimal](9, 6) NOT NULL,
	[esforco_relacionamento] [decimal](9, 6) NOT NULL,
	[captacao_da_base] [decimal](18, 2) NOT NULL,
	[atingimento_contas_aportarem] [decimal](9, 6) NOT NULL,
	[atingimento_ordens_enviadas] [decimal](9, 6) NOT NULL,
	[atingimento_contas_acessadas_hub] [decimal](9, 6) NOT NULL,
	[data_carga] [datetime] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT ((0)) FOR [iea_final]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT ((0)) FOR [captacao_liquida]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT ((0)) FOR [esforco_prospeccao]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT ((0)) FOR [captacao_de_novos_clientes_por_aai]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT ((0)) FOR [atingimento_lead_starts]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT ((0)) FOR [atingimento_habilitacoes]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT ((0)) FOR [atingimento_conversao]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT ((0)) FOR [atingimento_carteiras_simuladas_novos]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT ((0)) FOR [esforco_relacionamento]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT ((0)) FOR [captacao_da_base]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT ((0)) FOR [atingimento_contas_aportarem]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT ((0)) FOR [atingimento_ordens_enviadas]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT ((0)) FOR [atingimento_contas_acessadas_hub]
GO
ALTER TABLE [bronze].[xp_iea] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de referência dos dados (formato ano-mês).' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'ano_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do assessor no CRM.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Índice de Esforço do Assessor (IEA) final.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'iea_final'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Captação líquida total do período.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'captacao_liquida'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Esforço do assessor na etapa de prospecção.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'esforco_prospeccao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Captação de novos clientes por assessor.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'captacao_de_novos_clientes_por_aai'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Atingimento da meta de lead starts.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'atingimento_lead_starts'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Atingimento da meta de habilitações.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'atingimento_habilitacoes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Atingimento da meta de conversões.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'atingimento_conversao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Atingimento de carteiras simuladas para novos clientes.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'atingimento_carteiras_simuladas_novos'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Esforço do assessor na etapa de relacionamento.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'esforco_relacionamento'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Captação da base de clientes ativa.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'captacao_da_base'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Atingimento de contas com aporte.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'atingimento_contas_aportarem'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Atingimento de ordens enviadas.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'atingimento_ordens_enviadas'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Atingimento de contas acessadas no Hub.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'atingimento_contas_acessadas_hub'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data e hora de carga dos dados na tabela.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela fato com os indicadores do IEA (Índice de Esforço do Assessor), processados a partir da stage_iea.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_iea'
GO
