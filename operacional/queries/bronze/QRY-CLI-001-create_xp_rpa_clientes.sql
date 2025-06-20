SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bronze].[xp_rpa_clientes](
	[cod_xp] [int] NULL,
	[nome_cliente] [nvarchar](107) NULL,
	[telefone_cliente] [nvarchar](50) NULL,
	[email_cliente] [nvarchar](54) NULL,
	[patrimonio] [decimal](18, 2) NULL,
	[elegibilidade_cartao] [nvarchar](50) NULL,
	[cpf_cnpj] [nvarchar](50) NULL,
	[suitability] [nvarchar](50) NULL,
	[fee_based] [nvarchar](50) NULL,
	[segmento] [nvarchar](50) NULL,
	[tipo_investidor] [nvarchar](50) NULL,
	[cod_aai] [nvarchar](50) NULL,
	[status_conta_digital] [nvarchar](50) NULL,
	[produto] [nvarchar](50) NULL,
	[data_carga] [datetime2](7) NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [bronze].[xp_rpa_clientes] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ID único do cliente. Chave para joins com outras tabelas. COUNT DISTINCT por assessor para tamanho de carteira. Base para análises individuais de cliente.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'cod_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome completo ou razão social. Use com cuidado - dados pessoais. WHERE nome_cliente LIKE para buscas específicas. ORDER BY para listagens alfabéticas.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'nome_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Telefone de contato do cliente' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'telefone_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Email de contato do cliente' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'email_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Valor do patrimônio do cliente na XP' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'patrimonio'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Indicador de elegibilidade para cartão de crédito XP' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'elegibilidade_cartao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'CPF ou CNPJ mascarado/anonimizado. LEN() = 14 para CNPJ, 11 para CPF. Não use para joins. Permite identificar PF vs PJ. Dados sensíveis protegidos.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'cpf_cnpj'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Perfil de investidor do cliente conforme questionário suitability' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'suitability'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Indicador se o cliente é fee based (taxa fixa)' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'fee_based'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Segmento de classificação do cliente (Varejo, Private, etc)' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'segmento'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tipo de investidor (Pessoa Física ou Pessoa Jurídica)' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'tipo_investidor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do assessor dono da carteira. JOIN com silver_dim_pessoas. GROUP BY para análise por assessor. Base para distribuição de clientes e análise de concentração.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'cod_aai'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Status da conta digital do cliente na XP' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'status_conta_digital'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Principal produto investido pelo cliente' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'produto'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data e hora em que os dados foram carregados no sistema' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Dados brutos de clientes vindos do RPA (Relatório de Performance de Assessor) da XP. Contém base de clientes ativos por assessor' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_rpa_clientes'
GO
