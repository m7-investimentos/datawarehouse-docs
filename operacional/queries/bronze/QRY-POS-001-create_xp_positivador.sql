SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bronze].[xp_positivador](
	[data_ref] [date] NOT NULL,
	[cod_xp] [varchar](20) NOT NULL,
	[cod_aai] [varchar](20) NOT NULL,
	[profissao] [varchar](300) NULL,
	[sexo] [varchar](5) NULL,
	[segmento] [varchar](300) NULL,
	[data_cadastro] [date] NULL,
	[fez_segundo_aporte] [bit] NULL,
	[data_nascimento] [date] NULL,
	[status_cliente] [bit] NULL,
	[ativou_em_M] [bit] NULL,
	[evadiu_em_M] [bit] NULL,
	[operou_bolsa] [bit] NULL,
	[operou_fundo] [bit] NULL,
	[operou_renda_fixa] [bit] NULL,
	[aplicacao_financeira_declarada] [decimal](16, 4) NULL,
	[receita_mes] [decimal](16, 4) NULL,
	[receita_bovespa] [decimal](16, 4) NULL,
	[receita_futuros] [decimal](16, 4) NULL,
	[receita_rf_bancarios] [decimal](16, 4) NULL,
	[receita_rf_privados] [decimal](16, 4) NULL,
	[receita_rf_publicos] [decimal](16, 4) NULL,
	[captacao_bruta_em_M] [decimal](16, 4) NULL,
	[resgate_em_M] [decimal](16, 4) NULL,
	[captacao_liquida_em_M] [decimal](16, 4) NULL,
	[captacao_TED] [decimal](16, 4) NULL,
	[captacao_ST] [decimal](16, 4) NULL,
	[captacao_OTA] [decimal](16, 4) NULL,
	[captacao_RF] [decimal](16, 4) NULL,
	[captacao_TD] [decimal](16, 4) NULL,
	[captacao_PREV] [decimal](16, 4) NULL,
	[net_em_M_1] [decimal](16, 4) NULL,
	[net_em_M] [decimal](16, 4) NULL,
	[net_renda_fixa] [decimal](16, 4) NULL,
	[net_fundos_imobiliarios] [decimal](16, 4) NULL,
	[net_renda_variavel] [decimal](16, 4) NULL,
	[net_fundos] [decimal](16, 4) NULL,
	[net_financeiro] [decimal](16, 4) NULL,
	[net_previdencia] [decimal](16, 4) NULL,
	[net_outros] [decimal](16, 4) NULL,
	[receita_aluguel] [decimal](16, 4) NULL,
	[data_carga] [datetime] NULL,
 CONSTRAINT [PK_bronze_xp_positivador] PRIMARY KEY CLUSTERED 
(
	[data_ref] ASC,
	[cod_xp] ASC,
	[cod_aai] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [bronze].[xp_positivador] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data do snapshot mensal (último dia do mês). WHERE data_ref = ''2025-01-31'' para mês específico. MAX(data_ref) para posição atual. Base para séries históricas.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ID do cliente. Combine com data_ref para análise temporal por cliente. COUNT DISTINCT para base ativa. Chave primária composta com data_ref.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'cod_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do assessor. Chave para análises por carteira. JOIN com silver_dim_pessoas. GROUP BY para métricas de assessor. Base para rankings e metas individuais.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'cod_aai'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Profissão declarada texto livre. WHERE profissao LIKE ''%médico%'' para profissionais saúde. Muito variado, use LIKE. GROUP BY TOP 20 para profissões mais comuns. Base para ofertas direcionadas.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'profissao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Gênero: M ou F. WHERE sexo = ''F'' para análises específicas. GROUP BY para distribuição de gênero. Combine com idade para segmentações demográficas.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'sexo'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Classificação de atendimento. Private (alta renda), Varejo, Empresas. Define nível de serviço e produtos elegíveis. WHERE segmento = ''Private'' para alto valor.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'segmento'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de abertura da conta na XP. DATEDIFF(month, data_cadastro, data_ref) para tempo de relacionamento. WHERE YEAR(data_cadastro) = 2024 para safra específica. Base para cohort analysis.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'data_cadastro'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Flag histórico se já fez 2º aporte: 1=sim, 0=não. Indica engajamento inicial. Clientes com 2º aporte têm maior lifetime value. Base para scoring.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'fez_segundo_aporte'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data nascimento do cliente. DATEDIFF(year, data_nascimento, GETDATE()) para idade. Segmente por faixa etária. NULL comum por privacidade. Importante para produtos específicos.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'data_nascimento'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Status ativo: 1=ativo (tem saldo), 0=inativo. WHERE status_cliente = 1 para base ativa. SUM(status_cliente) para contagem rápida de ativos. Fundamental para métricas.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'status_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Flag se cliente ativou no mês: 1=sim (primeira aplicação), 0=não. WHERE ativou_em_M = 1 para novos investidores. SUM para total de ativações. KPI importante.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'ativou_em_M'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Flag se cliente evadiu (zerou saldo): 1=sim, 0=não. WHERE evadiu_em_M = 1 para análise de churn. SUM para total de evasões. Alerta máximo para retenção.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'evadiu_em_M'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Flag se operou bolsa/ações no mês: 1=sim, 0=não. WHERE operou_bolsa = 1 para ativos no produto. Indica diversificação e atividade.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'operou_bolsa'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Flag se operou fundos de investimento no mês: 1=sim, 0=não. WHERE operou_fundo = 1 para ativos no produto. Indica diversificação e atividade.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'operou_fundo'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Flag se operou renda fixa no mês: 1=sim, 0=não. WHERE operou_renda_fixa = 1 para ativos no produto. Indica diversificação e atividade.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'operou_renda_fixa'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio total declarado pelo cliente em R$. Pode incluir valores fora da XP. Compare com net_em_M para validar. WHERE aplicacao_financeira_declarada > 1000000 para HNW.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'aplicacao_financeira_declarada'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Receita total gerada pelo cliente no mês em R$. Soma de todas as receitas. WHERE receita_mes > 1000 para top revenue. Base para segmentação por rentabilidade.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'receita_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Receita específica de corretagem de ações e taxas B3 no mês em R$. Detalha origem da rentabilidade. WHERE receita_bovespa > 0 para clientes ativos no produto.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'receita_bovespa'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Receita específica de operações no mercado futuro no mês em R$. Detalha origem da rentabilidade. WHERE receita_futuros > 0 para clientes ativos no produto.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'receita_futuros'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Receita específica de produtos bancários (CDB, LCI, LCA) no mês em R$. Detalha origem da rentabilidade. WHERE receita_rf_bancarios > 0 para clientes ativos no produto.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'receita_rf_bancarios'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Receita específica de títulos privados (debêntures, CRI, CRA) no mês em R$. Detalha origem da rentabilidade. WHERE receita_rf_privados > 0 para clientes ativos no produto.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'receita_rf_privados'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Receita específica de títulos públicos (Tesouro Direto) no mês em R$. Detalha origem da rentabilidade. WHERE receita_rf_publicos > 0 para clientes ativos no produto.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'receita_rf_publicos'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Total de entradas no mês em R$ (sem descontar resgates). WHERE captacao_bruta_em_M > 0 para clientes que aportaram. Base para análise de engajamento.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'captacao_bruta_em_M'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Total de resgates/saídas no mês em R$. WHERE resgate_em_M > net_em_M * 0.5 para grandes resgates proporcionais. Sinal de alerta para retenção.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'resgate_em_M'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Captação líquida no mês (entradas - saídas) em R$. Métrica chave de crescimento. WHERE captacao_liquida_em_M < 0 para clientes em evasão. SUM para net total.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'captacao_liquida_em_M'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Captação via TED (transferência bancária) no mês em R$. Canal tradicional, tickets maiores. Principal forma de entrada de recursos.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'captacao_TED'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Captação via ST no mês em R$. Verificar significado específico com XP. Menos comum que TED.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'captacao_ST'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Captação via OTA (Oferta Pública/IPO) no mês em R$. Participação em lançamentos. Cliente sofisticado e ativo.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'captacao_OTA'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Captação direta em renda fixa no mês em R$. Preferência por produtos conservadores. Base da pirâmide de investimentos.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'captacao_RF'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Captação via TD (Tesouro Direto) no mês em R$. Indica interesse em títulos públicos. Perfil conservador educado.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'captacao_TD'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Captação em previdência no mês em R$. Aportes recorrentes comuns. Alta fidelização e visão longo prazo.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'captacao_PREV'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio no mês anterior (M-1) em R$. (net_em_M - net_em_M_1) para variação mensal. Identifique crescimento ou redução de patrimônio. Base para análise de tendência.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'net_em_M_1'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio total na XP no mês atual em R$. Soma de todos produtos. SUM para AUM total. AVG para ticket médio. WHERE net_em_M > 0 para clientes com saldo. Métrica principal.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'net_em_M'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio em renda fixa (CDB, LCI, LCA, Tesouro) em R$. Perfil conservador. WHERE net_renda_fixa/net_em_M > 0.8 para muito conservadores. Base para ofertas de diversificação.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'net_renda_fixa'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio em FIIs (Fundos Imobiliários) em R$. Investidor de renda passiva. WHERE net_fundos_imobiliarios > 0 identifica esse perfil. Popular entre aposentados.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'net_fundos_imobiliarios'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio em ações, ETFs, BDRs em R$. Perfil arrojado. WHERE net_renda_variavel > 0 para investidores em bolsa. Correlaciona com maior sofisticação e atividade.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'net_renda_variavel'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio em fundos de investimento em R$. Inclui multimercados, ações, renda fixa. Produto de maior margem. WHERE net_fundos/net_em_M > 0.5 para fund lovers.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'net_fundos'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Saldo em conta corrente/financeiro em R$. Recursos não investidos, disponíveis. WHERE net_financeiro > 100000 para oportunidades de investimento. Indica liquidez do cliente.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'net_financeiro'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio em previdência privada (PGBL/VGBL) em R$. Visão de longo prazo. Alta fidelização. WHERE net_previdencia > 0 para clientes com previdência. Benefício fiscal.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'net_previdencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio em outros produtos (COE, Debentures, etc) em R$. Produtos sofisticados ou específicos. Indica cliente qualificado quando > 0.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'net_outros'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Receita específica de aluguel de ações no mês em R$. Detalha origem da rentabilidade. WHERE receita_aluguel > 0 para clientes ativos no produto.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'receita_aluguel'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Timestamp de carga no DW. Controle de processamento. WHERE data_carga = MAX(data_carga) para último processamento. Auditoria de ETL.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Dados brutos do positivador XP. Contém informações consolidadas de patrimônio dos clientes, incluindo valores na XP e declarados em outras instituições' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_positivador'
GO
