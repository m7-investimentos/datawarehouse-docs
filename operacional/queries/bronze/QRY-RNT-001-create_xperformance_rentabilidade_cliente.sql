SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bronze].[xperformance_rentabilidade_cliente](
	[conta_xp_cliente] [int] NOT NULL,
	[data_relatorio] [date] NOT NULL,
	[ano] [int] NOT NULL,
	[mes] [varchar](5) NOT NULL,
	[mes_num] [int] NOT NULL,
	[portfolio_rentabilidade] [decimal](18, 4) NULL,
	[acumulado_ano] [decimal](18, 4) NULL,
	[data_carga] [datetime] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [bronze].[xperformance_rentabilidade_cliente] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código único do cliente na XP. Identificador principal do cliente no sistema.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', @level2type=N'COLUMN',@level2name=N'conta_xp_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de extração do relatório (último dia útil do mês). Fundamental para identificar a versão mais recente dos dados, pois o mesmo período pode aparecer em múltiplos relatórios.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', @level2type=N'COLUMN',@level2name=N'data_relatorio'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Ano de referência da rentabilidade (formato: YYYY). Junto com mes_num forma a chave temporal.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', @level2type=N'COLUMN',@level2name=N'ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome abreviado do mês em português (Jan, Fev, Mar, etc). Usado para visualização.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', @level2type=N'COLUMN',@level2name=N'mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Número do mês (1-12). Usado para ordenação e cálculos temporais.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', @level2type=N'COLUMN',@level2name=N'mes_num'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade mensal do portfolio do cliente EM PERCENTUAL. ATENÇÃO: Valor vem como percentual (ex: 10.5800 = 10.58%). Para uso em cálculos, SEMPRE dividir por 100 para converter em decimal (10.5800/100 = 0.10580).' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', @level2type=N'COLUMN',@level2name=N'portfolio_rentabilidade'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade acumulada no ano EM PERCENTUAL. CUIDADO: Este campo apresenta inconsistências nos dados de origem (valores incorretos). Recomenda-se recalcular dinamicamente. Valor em percentual que deve ser dividido por 100 para uso em cálculos.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', @level2type=N'COLUMN',@level2name=N'acumulado_ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Timestamp de quando o registro foi inserido na tabela bronze. Usado para auditoria e controle de carga.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela bronze com dados brutos de rentabilidade de clientes extraídos dos relatórios XPerformance. Alimentada mensalmente com dados do ano vigente + 2 anos anteriores. Contém duplicatas por data_relatorio.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente'
GO
