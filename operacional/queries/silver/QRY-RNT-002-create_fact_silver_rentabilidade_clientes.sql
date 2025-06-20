SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [silver].[fact_rentabilidade_clientes](
	[conta_xp_cliente] [int] NOT NULL,
	[ano_mes] [int] NOT NULL,
	[ano] [int] NULL,
	[semestre] [varchar](2) NULL,
	[trimestre] [varchar](2) NULL,
	[mes_num] [int] NULL,
	[mes] [varchar](5) NULL,
	[rentabilidade] [decimal](18, 8) NULL,
	[rentabilidade_acumulada_3_meses] [decimal](18, 8) NULL,
	[rentabilidade_acumulada_6_meses] [decimal](18, 8) NULL,
	[rentabilidade_acumulada_12_meses] [decimal](18, 8) NULL,
	[rentabilidade_acumulada_trimestre] [decimal](18, 8) NULL,
	[rentabilidade_acumulada_semestre] [decimal](18, 8) NULL,
	[rentabilidade_acumulada_ano] [decimal](18, 8) NULL,
	[data_carga] [datetime] NULL,
 CONSTRAINT [PK_fact_rentabilidade_clientes] PRIMARY KEY CLUSTERED 
(
	[conta_xp_cliente] ASC,
	[ano_mes] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [silver].[fact_rentabilidade_clientes] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código único do cliente na XP. Parte da chave primária composta (conta_xp_cliente + ano_mes).' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'conta_xp_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Período no formato YYYYMM (202501 = Janeiro/2025). Calculado como (ano * 100 + mes_num). Parte da chave primária composta.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'ano_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Ano de referência (formato: YYYY). Usado para agregações e filtros anuais.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Identificador do semestre (S1 = Jan-Jun, S2 = Jul-Dez). Campo calculado para facilitar agregações semestrais.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Identificador do trimestre (Q1 = Jan-Mar, Q2 = Abr-Jun, Q3 = Jul-Set, Q4 = Out-Dez). Campo calculado para análises trimestrais.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Número do mês (1-12). Mantido para compatibilidade e cálculos.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'mes_num'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome abreviado do mês (Jan, Fev, Mar, etc). Mantido da bronze para visualização.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade mensal EM DECIMAL. IMPORTANTE: Valor já convertido de percentual para decimal (bronze.portfolio_rentabilidade / 100). Ex: 10.58% na bronze = 0.1058 aqui na silver. Pronto para uso em cálculos.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade acumulada dos últimos 3 meses (janela móvel) EM DECIMAL. Calculada usando a fórmula de juros compostos: ((1+r1)*(1+r2)*(1+r3))-1. Inclui o mês atual + 2 meses anteriores.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade_acumulada_3_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade acumulada dos últimos 6 meses (janela móvel) EM DECIMAL. Calculada com juros compostos incluindo mês atual + 5 meses anteriores. Valor já em decimal, pronto para uso.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade_acumulada_6_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade acumulada dos últimos 12 meses (janela móvel) EM DECIMAL. Calculada com juros compostos dos últimos 12 meses. Importante para análise de performance anualizada.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade_acumulada_12_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade acumulada do trimestre ATUAL (não é janela móvel) EM DECIMAL. Reseta no início de cada trimestre (Jan, Abr, Jul, Out). Ex: em março, acumula Jan+Fev+Mar; em abril, começa novo acumulado.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade_acumulada_trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade acumulada do semestre ATUAL (não é janela móvel) EM DECIMAL. Reseta em Janeiro e Julho. Acumula progressivamente dentro do semestre fiscal.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade_acumulada_semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade acumulada do ano ATUAL (YTD - Year to Date) EM DECIMAL. Calculada dinamicamente pela view, não confia no campo da bronze. Reseta em Janeiro de cada ano.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade_acumulada_ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Timestamp de quando o registro foi inserido/atualizado na silver. Preenchido automaticamente com GETDATE(). Para dados históricos preservados, mantém a data_carga original.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela silver com dados processados de rentabilidade. Um registro único por cliente/mês. Inclui cálculos de rentabilidade acumulada em múltiplas janelas temporais. Valores já convertidos para decimal.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes'
GO
