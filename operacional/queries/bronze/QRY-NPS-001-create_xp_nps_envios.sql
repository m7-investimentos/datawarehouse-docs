SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bronze].[xp_nps_envios](
	[survey_id] [varchar](50) NULL,
	[cod_assessor] [varchar](20) NULL,
	[customer_id] [varchar](50) NULL,
	[codigo_escritorio] [varchar](20) NULL,
	[data_entrega] [date] NULL,
	[data_resposta] [date] NULL,
	[invitation_opened_date] [datetime] NULL,
	[survey_start_date] [datetime] NULL,
	[resposta_app_nps] [varchar](10) NULL,
	[survey_status] [varchar](50) NULL,
	[pesquisa_relacionamento] [varchar](100) NULL,
	[email] [varchar](255) NULL,
	[invitation_opened] [varchar](10) NULL,
	[sampling_exclusion_cause] [varchar](255) NULL,
	[last_page_seen] [varchar](100) NULL,
	[nps_app_survey_id_original] [varchar](50) NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [bronze].[xp_nps_envios] ADD  DEFAULT (CONVERT([date],getdate())) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela com dados de envios de pesquisas NPS (Net Promoter Score) extraídos do Hub XP' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_nps_envios'
GO
