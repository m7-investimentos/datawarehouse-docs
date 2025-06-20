SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [gold].[habilitacoes_ativacoes](
	[ano_mes] [int] NOT NULL,
	[ano] [int] NOT NULL,
	[mes] [int] NOT NULL,
	[nome_mes] [varchar](20) NOT NULL,
	[trimestre] [varchar](2) NOT NULL,
	[semestre] [varchar](2) NOT NULL,
	[cod_assessor] [varchar](20) NOT NULL,
	[crm_id_assessor] [varchar](20) NULL,
	[nome_assessor] [varchar](200) NOT NULL,
	[nivel_assessor] [varchar](50) NULL,
	[estrutura_id] [int] NULL,
	[estrutura_nome] [varchar](100) NULL,
	[qtd_ativacoes_300k_mais] [int] NOT NULL,
	[qtd_ativacoes_300k_menos] [int] NOT NULL,
	[qtd_ativacoes_300k_mais_trimestre] [int] NOT NULL,
	[qtd_ativacoes_300k_menos_trimestre] [int] NOT NULL,
	[qtd_ativacoes_300k_mais_semestre] [int] NOT NULL,
	[qtd_ativacoes_300k_menos_semestre] [int] NOT NULL,
	[qtd_ativacoes_300k_mais_ano] [int] NOT NULL,
	[qtd_ativacoes_300k_menos_ano] [int] NOT NULL,
	[qtd_ativacoes_300k_mais_3_meses] [int] NOT NULL,
	[qtd_ativacoes_300k_menos_3_meses] [int] NOT NULL,
	[qtd_ativacoes_300k_mais_6_meses] [int] NOT NULL,
	[qtd_ativacoes_300k_menos_6_meses] [int] NOT NULL,
	[qtd_ativacoes_300k_mais_12_meses] [int] NOT NULL,
	[qtd_ativacoes_300k_menos_12_meses] [int] NOT NULL,
	[qtd_habilitacoes_300k_mais] [int] NOT NULL,
	[qtd_habilitacoes_300k_menos] [int] NOT NULL,
	[qtd_habilitacoes_300k_mais_trimestre] [int] NOT NULL,
	[qtd_habilitacoes_300k_menos_trimestre] [int] NOT NULL,
	[qtd_habilitacoes_300k_mais_semestre] [int] NOT NULL,
	[qtd_habilitacoes_300k_menos_semestre] [int] NOT NULL,
	[qtd_habilitacoes_300k_mais_ano] [int] NOT NULL,
	[qtd_habilitacoes_300k_menos_ano] [int] NOT NULL,
	[qtd_habilitacoes_300k_mais_3_meses] [int] NOT NULL,
	[qtd_habilitacoes_300k_menos_3_meses] [int] NOT NULL,
	[qtd_habilitacoes_300k_mais_6_meses] [int] NOT NULL,
	[qtd_habilitacoes_300k_menos_6_meses] [int] NOT NULL,
	[qtd_habilitacoes_300k_mais_12_meses] [int] NOT NULL,
	[qtd_habilitacoes_300k_menos_12_meses] [int] NOT NULL,
	[data_carga] [date] NOT NULL,
 CONSTRAINT [PK_indice_esforco_assessor] PRIMARY KEY CLUSTERED 
(
	[ano_mes] ASC,
	[cod_assessor] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_mais]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_menos]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_mais_trimestre]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_menos_trimestre]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_mais_semestre]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_menos_semestre]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_mais_ano]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_menos_ano]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_mais_3_meses]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_menos_3_meses]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_mais_6_meses]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_menos_6_meses]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_mais_12_meses]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_ativacoes_300k_menos_12_meses]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_mais]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_menos]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_mais_trimestre]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_menos_trimestre]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_mais_semestre]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_menos_semestre]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_mais_ano]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_menos_ano]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_mais_3_meses]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_menos_3_meses]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_mais_6_meses]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_menos_6_meses]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_mais_12_meses]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT ((0)) FOR [qtd_habilitacoes_300k_menos_12_meses]
GO
ALTER TABLE [gold].[habilitacoes_ativacoes] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Ano e mês de referência no formato AAAAMM (ex: 202501 para Janeiro/2025). Chave primária composta junto com cod_assessor.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'ano_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Ano de referência dos dados (ex: 2025).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Mês de referência dos dados (1-12).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome do mês por extenso em inglês (ex: January, February).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'nome_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Trimestre do ano (Q1, Q2, Q3, Q4).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Semestre do ano (S1 para primeiro semestre, S2 para segundo semestre).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código único do assessor no sistema. Chave primária composta junto com ano_mes.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Identificador do assessor no sistema CRM. Geralmente igual ao cod_assessor.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'crm_id_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome completo do assessor comercial.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'nome_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nível hierárquico do assessor (Junior, Pleno, Senior). Pode ser nulo.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'nivel_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Identificador único da estrutura organizacional onde o assessor estava alocado no período.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'estrutura_id'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome da estrutura organizacional (ex: Alta Renda-Fortaleza, Varejo-São Paulo).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'estrutura_nome'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de ativações no mês de clientes com patrimônio líquido acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_mais'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de ativações no mês de clientes com patrimônio líquido até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_menos'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade acumulada de ativações no trimestre corrente de clientes com patrimônio acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_mais_trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade acumulada de ativações no trimestre corrente de clientes com patrimônio até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_menos_trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade acumulada de ativações no semestre corrente de clientes com patrimônio acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_mais_semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade acumulada de ativações no semestre corrente de clientes com patrimônio até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_menos_semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade acumulada de ativações no ano corrente de clientes com patrimônio acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_mais_ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade acumulada de ativações no ano corrente de clientes com patrimônio até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_menos_ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de ativações nos últimos 3 meses (incluindo mês atual) de clientes com patrimônio acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_mais_3_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de ativações nos últimos 3 meses (incluindo mês atual) de clientes com patrimônio até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_menos_3_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de ativações nos últimos 6 meses (incluindo mês atual) de clientes com patrimônio acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_mais_6_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de ativações nos últimos 6 meses (incluindo mês atual) de clientes com patrimônio até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_menos_6_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de ativações nos últimos 12 meses (incluindo mês atual) de clientes com patrimônio acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_mais_12_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de ativações nos últimos 12 meses (incluindo mês atual) de clientes com patrimônio até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_ativacoes_300k_menos_12_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de habilitações no mês de clientes com patrimônio líquido acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_mais'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de habilitações no mês de clientes com patrimônio líquido até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_menos'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade acumulada de habilitações no trimestre corrente de clientes com patrimônio acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_mais_trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade acumulada de habilitações no trimestre corrente de clientes com patrimônio até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_menos_trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade acumulada de habilitações no semestre corrente de clientes com patrimônio acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_mais_semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade acumulada de habilitações no semestre corrente de clientes com patrimônio até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_menos_semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade acumulada de habilitações no ano corrente de clientes com patrimônio acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_mais_ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade acumulada de habilitações no ano corrente de clientes com patrimônio até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_menos_ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de habilitações nos últimos 3 meses (incluindo mês atual) de clientes com patrimônio acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_mais_3_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de habilitações nos últimos 3 meses (incluindo mês atual) de clientes com patrimônio até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_menos_3_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de habilitações nos últimos 6 meses (incluindo mês atual) de clientes com patrimônio acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_mais_6_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de habilitações nos últimos 6 meses (incluindo mês atual) de clientes com patrimônio até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_menos_6_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de habilitações nos últimos 12 meses (incluindo mês atual) de clientes com patrimônio acima de R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_mais_12_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de habilitações nos últimos 12 meses (incluindo mês atual) de clientes com patrimônio até R$ 300.000.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'qtd_habilitacoes_300k_menos_12_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data em que os dados foram carregados/atualizados na tabela. Usado para auditoria e controle de processamento.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela consolidada mensal de habilitações e ativações por assessor. Contém métricas agregadas por período (mensal, trimestral, semestral, anual) e janelas móveis (3, 6, 12 meses), segmentadas por valor de patrimônio (acima/abaixo de R$ 300k). Utilizada para análise de performance e acompanhamento de metas dos assessores comerciais.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'habilitacoes_ativacoes'
GO
