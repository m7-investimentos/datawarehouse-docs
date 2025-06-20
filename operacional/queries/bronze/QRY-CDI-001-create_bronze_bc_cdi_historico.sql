SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bronze].[bc_cdi_historico](
	[data_ref] [date] NOT NULL,
	[taxa_cdi] [decimal](18, 8) NOT NULL,
	[data_carga] [datetime] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [bronze].[bc_cdi_historico] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência da taxa cdi. representa o dia útil específico da cotação, não incluindo finais de semana ou feriados bancários. chave primária natural da tabela' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bc_cdi_historico', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'taxa cdi do dia em formato percentual. valor representa percentual ao dia (ex: 0.05426600 = 0,054266% a.d.). precisão de 8 casas decimais. range histórico observado: 0.03927000 a 0.05426600. média histórica aproximada: 0.04484157' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bc_cdi_historico', @level2type=N'COLUMN',@level2name=N'taxa_cdi'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'timestamp de carga dos dados no data warehouse. momento exato em que o registro foi inserido na tabela bronze. usado para auditoria e controle de processo etl. formato datetime com precisão de milissegundos' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bc_cdi_historico', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela bronze que armazena o histórico diário bruto das taxas cdi (certificado de depósito interbancário) conforme divulgado pelo banco central do brasil. contém apenas dias úteis e representa a taxa de juros praticada em operações entre bancos. fonte de dados provavelmente oriunda de api do bacen ou sistema interno de captura' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bc_cdi_historico'
GO
