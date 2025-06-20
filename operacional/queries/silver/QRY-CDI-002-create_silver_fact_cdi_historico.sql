SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [silver].[fact_cdi_historico](
	[data_ref] [date] NOT NULL,
	[ano_mes] [varchar](6) NULL,
	[ano] [int] NULL,
	[mes_num] [int] NULL,
	[trimestre] [varchar](2) NULL,
	[semestre] [varchar](2) NULL,
	[taxa_cdi_dia] [decimal](18, 8) NOT NULL,
	[taxa_cdi_mes] [decimal](18, 8) NOT NULL,
	[taxa_cdi_3_meses] [decimal](18, 8) NOT NULL,
	[taxa_cdi_6_meses] [decimal](18, 8) NOT NULL,
	[taxa_cdi_12_meses] [decimal](18, 8) NOT NULL,
	[taxa_cdi_trimestre] [decimal](18, 8) NOT NULL,
	[taxa_cdi_semestre] [decimal](18, 8) NOT NULL,
	[taxa_cdi_ano] [decimal](18, 8) NOT NULL
) ON [PRIMARY]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência da taxa cdi herdada da tabela bronze. mantém integridade referencial com bronze.bc_cdi_historico. chave para joins com outras tabelas do dw' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano e mês concatenados no formato yyyymm (ex: 202506 para junho/2025). facilita agrupamentos e análises mensais. útil para particionamento e otimização de queries' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'ano_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano extraído da data_ref no formato numérico (ex: 2025). facilita filtros e análises anuais' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'número do mês extraído da data_ref (1=janeiro até 12=dezembro). facilita ordenação cronológica e filtros por mês' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'mes_num'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador do trimestre no formato qn (q1=jan-mar, q2=abr-jun, q3=jul-set, q4=out-dez). facilita análises e agrupamentos trimestrais' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador do semestre no formato sn (s1=jan-jun, s2=jul-dez). facilita análises e agrupamentos semestrais' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'taxa cdi diária em formato decimal. conversão da taxa bronze dividida por 100 (ex: 0.00054266 = 0,054266% a.d.). base para todos os cálculos de taxas acumuladas' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'taxa_cdi_dia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'taxa cdi acumulada no mês corrente até a data_ref. soma simples das taxas diárias do mês (não é juros compostos). reinicia a cada novo mês. usado para cálculo de rentabilidade mensal' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'taxa_cdi_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'taxa cdi acumulada dos últimos 3 meses (rolling window). soma das taxas diárias dos últimos ~90 dias. pode ter inconsistências no início da série histórica quando não há 3 meses completos de dados' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'taxa_cdi_3_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'taxa cdi acumulada dos últimos 6 meses (rolling window). soma das taxas diárias dos últimos ~180 dias. pode ter inconsistências no início da série histórica quando não há 6 meses completos de dados' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'taxa_cdi_6_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'taxa cdi acumulada dos últimos 12 meses (rolling window). soma das taxas diárias dos últimos ~365 dias. representa o "cdi anual" mais comumente usado. pode ter inconsistências no início da série histórica' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'taxa_cdi_12_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'taxa cdi acumulada no trimestre corrente (período fixo). soma das taxas desde o início do trimestre atual. reinicia a cada mudança de trimestre. diferente de taxa_cdi_3_meses que é rolling' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'taxa_cdi_trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'taxa cdi acumulada no semestre corrente (período fixo). soma das taxas desde o início do semestre atual. reinicia a cada mudança de semestre. diferente de taxa_cdi_6_meses que é rolling' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'taxa_cdi_semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'taxa cdi acumulada no ano corrente - year to date (ytd). soma das taxas desde 01 de janeiro do ano atual. reinicia todo início de ano. diferente de taxa_cdi_12_meses que é rolling 12 months' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico', @level2type=N'COLUMN',@level2name=N'taxa_cdi_ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela silver fato que processa e enriquece os dados brutos de cdi da camada bronze. calcula automaticamente taxas acumuladas para diferentes períodos (mês, trimestre, semestre, ano, 3/6/12 meses). facilita análises de rentabilidade e cálculos baseados em cdi. sincronizada diariamente com a tabela bronze via processo etl' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cdi_historico'
GO
