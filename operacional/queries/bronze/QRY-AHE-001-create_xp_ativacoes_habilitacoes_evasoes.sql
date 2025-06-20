SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bronze].[xp_ativacoes_habilitacoes_evasoes](
	[data_ref] [date] NOT NULL,
	[cod_xp] [varchar](20) NOT NULL,
	[cod_aai] [varchar](20) NOT NULL,
	[faixa_pl] [varchar](50) NULL,
	[tipo_movimentacao] [varchar](20) NOT NULL,
	[data_carga] [datetime] NULL
) ON [PRIMARY]
GO
ALTER TABLE [bronze].[xp_ativacoes_habilitacoes_evasoes] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
