SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bronze].[xp_open_investment_extrato](
	[segmento] [varchar](20) NULL,
	[cod_assessor] [varchar](20) NULL,
	[cod_conta] [int] NULL,
	[cod_matriz] [smallint] NULL,
	[instituicao_bancaria] [varchar](500) NULL,
	[produtos] [varchar](100) NULL,
	[sub_produtos] [varchar](100) NULL,
	[ativo] [varchar](500) NULL,
	[valor_bruto] [decimal](18, 6) NULL,
	[valor_liquido] [decimal](18, 6) NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [bronze].[xp_open_investment_extrato] ADD  DEFAULT (CONVERT([date],getdate())) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Modelo de atendimento do cliente. B2B=atendido por assessor (maioria), B2C=direto sem assessor, Digital=100% online. WHERE segmento = ''B2B'' para clientes com assessor. Determina estratégia de relacionamento e comissionamento.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', @level2type=N'COLUMN',@level2name=N'segmento'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do assessor com prefixo A (ex: A22507). NULL para B2C/Digital. JOIN com silver_dim_pessoas.cod_aai removendo o A. GROUP BY para análise de patrimônio externo por assessor. Base para cálculo de potencial de migração.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código XP do cliente igual cod_xp em outras tabelas. Chave para JOIN com bronze_xp_captacao, bronze_xp_positivador. Use para cruzar patrimônio externo com interno. COUNT DISTINCT para clientes únicos com open banking.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', @level2type=N'COLUMN',@level2name=N'cod_conta'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do escritório/matriz do assessor. JOIN com silver_dim_estruturas para hierarquia. GROUP BY para análise por escritório. Identifica qual unidade M7 é responsável. Importante para metas regionais.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', @level2type=N'COLUMN',@level2name=N'cod_matriz'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome completo do banco/instituição concorrente. Valores frequentes: BANCO BRADESCO SA, ITAÚ UNIBANCO S.A., BANCO DO BRASIL SA. GROUP BY para market share por instituição. WHERE LIKE ''%BRADESCO%'' para específico banco. Target para campanhas.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', @level2type=N'COLUMN',@level2name=N'instituicao_bancaria'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Categoria macro do investimento. Valores: Renda Fixa, Fundos, Renda Variável, Previdência. GROUP BY para distribuição de portfolio externo. WHERE produtos = ''Renda Fixa'' para conservadores. Base para estratégia de abordagem.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', @level2type=N'COLUMN',@level2name=N'produtos'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Subcategoria quando aplicável. Ex: Bancário (CDB), DI (fundos), Multimercado. Pode ser NULL. Combine com produtos para análise detalhada. Indica sofisticação do investidor. Útil para ofertas direcionadas.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', @level2type=N'COLUMN',@level2name=N'sub_produtos'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome completo/descrição do investimento. Para CDB inclui vencimento (ex: CDB 2025-12-31), para fundos nome completo. Texto livre muito variado. Use LIKE para buscas. Nível mais granular de detalhe disponível.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', @level2type=N'COLUMN',@level2name=N'ativo'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Valor total do investimento antes de impostos em R$. SUM por cliente para patrimônio bruto externo total. Compare com valor_líquido para estimar IR. Base para cálculo de potencial máximo de captação.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', @level2type=N'COLUMN',@level2name=N'valor_bruto'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Valor após IR disponível para resgate em R$. Use este para potencial real de migração. (valor_bruto - valor_líquido) = imposto estimado. SUM por cliente e instituição para priorização de abordagem.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', @level2type=N'COLUMN',@level2name=N'valor_liquido'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de captura via Open Banking. Indica frescor dos dados. WHERE data_carga >= DATEADD(day,-30,GETDATE()) para dados recentes. MAX(data_carga) para última posição. Atualização não é diária.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Dados brutos do extrato de investimentos da Open Investment (corretora parceira). Contém movimentações e posições de clientes que operam através dessa corretora' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato'
GO
