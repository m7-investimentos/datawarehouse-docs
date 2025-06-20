SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [silver].[fact_nps_respostas_envios_aniversario](
	[survey_id] [varchar](20) NOT NULL,
	[conta_xp_cliente] [varchar](20) NOT NULL,
	[cod_assessor] [varchar](20) NOT NULL,
	[data_entrega] [date] NULL,
	[data_resposta] [date] NULL,
	[data_inicio_survey] [datetime] NULL,
	[survey_status] [varchar](50) NULL,
	[convite_aberto] [char](3) NULL,
	[nps_assessor] [decimal](3, 1) NULL,
	[nps_xp] [decimal](3, 1) NULL,
	[recomendaria_assessor] [char](3) NULL,
	[classificacao_nps_assessor] [varchar](10) NULL,
	[classificacao_nps_xp] [varchar](10) NULL,
	[comentario_assessor] [nvarchar](max) NULL,
	[comentario_xp] [nvarchar](max) NULL,
	[razao_nps] [nvarchar](100) NULL,
	[razao_nps_assessor] [nvarchar](100) NULL,
	[razao_nps_xp] [nvarchar](100) NULL,
	[topicos_relevantes] [nvarchar](500) NULL,
	[data_carga] [datetime] NOT NULL,
 CONSTRAINT [PK_fact_nps_respostas_envios_aniversario] PRIMARY KEY CLUSTERED 
(
	[survey_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE [silver].[fact_nps_respostas_envios_aniversario] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Identificador único da pesquisa NPS' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'survey_id'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Número da conta XP do cliente' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'conta_xp_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do assessor responsável pelo cliente' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de envio da pesquisa NPS ao cliente' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'data_entrega'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data em que o cliente respondeu a pesquisa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'data_resposta'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data e hora de início do preenchimento da pesquisa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'data_inicio_survey'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Status atual da pesquisa (completed, delivered_and_reminded, delivery_bounced, delivered, expired, not_sampled)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'survey_status'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Indica se o convite foi aberto pelo cliente (sim/nao)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'convite_aberto'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nota NPS (0-10) atribuída ao assessor' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'nps_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nota NPS (0-10) atribuída à XP' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'nps_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Indica se o cliente recomendaria o assessor (sim/nao)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'recomendaria_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Classificação NPS do assessor: Promotor (9-10), Neutro (7-8), Detrator (0-6)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'classificacao_nps_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Classificação NPS da XP: Promotor (9-10), Neutro (7-8), Detrator (0-6)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'classificacao_nps_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Comentário em texto livre do cliente sobre o assessor' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'comentario_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Comentário em texto livre do cliente sobre a XP' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'comentario_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Razão principal para a nota NPS atribuída' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'razao_nps'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Razão específica para a nota NPS do assessor (ex: cordialidade, conhecimento técnico, etc)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'razao_nps_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Razão específica para a nota NPS da XP (ex: rentabilidade, plataforma, educação financeira)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'razao_nps_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tópicos ou categorias identificadas nos comentários através de análise de texto' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'topicos_relevantes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data e hora de carga dos dados na tabela (preenchido automaticamente)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela fato contendo dados consolidados de pesquisas NPS de aniversário de relacionamento com clientes' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario'
GO
