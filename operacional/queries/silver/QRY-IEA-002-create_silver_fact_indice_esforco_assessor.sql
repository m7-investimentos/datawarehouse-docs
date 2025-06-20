SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [silver].[fact_indice_esforco_assessor](
	[ano_mes] [varchar](6) NOT NULL,
	[cod_assessor] [varchar](20) NOT NULL,
	[indice_esforco_assessor] [decimal](18, 8) NOT NULL,
	[indice_esforco_assessor_acum_3_meses] [decimal](18, 8) NULL,
	[indice_esforco_assessor_acum_6_meses] [decimal](18, 8) NULL,
	[indice_esforco_assessor_acum_12_meses] [decimal](18, 8) NULL,
	[esforco_prospeccao] [decimal](18, 8) NOT NULL,
	[esforco_relacionamento] [decimal](18, 8) NOT NULL,
	[prospeccao_captacao_de_novos_clientes_por_aai] [decimal](16, 2) NOT NULL,
	[prospeccao_atingimento_lead_starts] [decimal](18, 8) NOT NULL,
	[prospeccao_atingimento_habilitacoes] [decimal](18, 8) NOT NULL,
	[prospeccao_atingimento_conversao] [decimal](18, 8) NOT NULL,
	[prospeccao_atingimento_carteiras_simuladas_novos] [decimal](18, 8) NOT NULL,
	[relacionamento_captacao_da_base] [decimal](18, 2) NOT NULL,
	[relacionamento_atingimento_contas_aportarem] [decimal](18, 8) NOT NULL,
	[relacionamento_atingimento_ordens_enviadas] [decimal](18, 8) NOT NULL,
	[relacionamento_atingimento_contas_acessadas_hub] [decimal](18, 8) NOT NULL,
	[data_carga] [datetime] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [silver].[fact_indice_esforco_assessor] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
