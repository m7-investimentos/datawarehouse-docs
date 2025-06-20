SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [gold].[rentabilidade_assessor](
	[ano_mes] [int] NOT NULL,
	[ano] [int] NOT NULL,
	[mes] [int] NOT NULL,
	[nome_mes] [varchar](20) NOT NULL,
	[trimestre] [varchar](2) NOT NULL,
	[semestre] [varchar](2) NOT NULL,
	[cod_assessor] [varchar](20) NOT NULL,
	[codigo_crm_assessor] [varchar](20) NULL,
	[nome_assessor] [varchar](200) NOT NULL,
	[nivel_assessor] [varchar](50) NULL,
	[estrutura_id] [int] NULL,
	[estrutura_nome] [varchar](100) NULL,
	[qtd_clientes_300k_mais] [int] NOT NULL,
	[qtd_clientes_acima_cdi] [int] NOT NULL,
	[qtd_clientes_faixa_80_cdi] [int] NOT NULL,
	[qtd_clientes_faixa_50_cdi] [int] NOT NULL,
	[qtd_clientes_rentabilidade_positiva] [int] NOT NULL,
	[perc_clientes_acima_cdi] [decimal](12, 6) NOT NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT ((0)) FOR [qtd_clientes_300k_mais]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT ((0)) FOR [qtd_clientes_acima_cdi]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT ((0)) FOR [qtd_clientes_faixa_80_cdi]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT ((0)) FOR [qtd_clientes_faixa_50_cdi]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT ((0)) FOR [qtd_clientes_rentabilidade_positiva]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT ((0)) FOR [perc_clientes_acima_cdi]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
