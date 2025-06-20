-- ==============================================================================
-- QRY-POS-001-create_xp_positivador
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: dados@m7investimentos.com.br
-- Revisor: dados@m7investimentos.com.br
-- Tags: [ddl, bronze, xp, positivador, patrimonio, clientes]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.xp_positivador para armazenar dados brutos 
consolidados de patrimônio e movimentações dos clientes. 
Visão completa do cliente incluindo patrimônio, captações e perfil.

Casos de uso:
- Análise de evolução patrimonial mensal
- Cálculo de métricas de captação e evasão
- Segmentação de clientes por patrimônio e comportamento

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 100k registros/mês
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros necessários para execução:
N/A - Script DDL sem parâmetros

Exemplo de uso:
USE M7Medallion;
GO
-- Executar script completo
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela criada: bronze.xp_positivador

Colunas principais:
- data_ref: data de referencia (último dia)
- cod_xp: Cliente
- cod_aai: Assessor
- net_em_M: Patrimônio total atual
- captacao_liquida_em_M: Movimentação líquida
- Múltiplas métricas de receita e patrimônio por produto
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
N/A

Pré-requisitos:
- Schema bronze deve existir
- Permissões CREATE TABLE no schema bronze
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
USE M7Medallion;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- ==============================================================================
-- 6. VERIFICAÇÃO E LIMPEZA
-- ==============================================================================

-- Verificar se a tabela já existe
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[xp_positivador]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.xp_positivador já existe. Dropando...';
    DROP TABLE [bronze].[xp_positivador];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

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
    ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, 
            ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
GO

ALTER TABLE [bronze].[xp_positivador] ADD DEFAULT (getdate()) FOR [data_carga];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por assessor e período
CREATE NONCLUSTERED INDEX [IX_bronze_pos_assessor_data]
ON [bronze].[xp_positivador] ([cod_aai], [data_ref])
INCLUDE ([net_em_M], [captacao_liquida_em_M], [receita_mes]);
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Dados brutos do positivador XP. Contém informações consolidadas de patrimônio dos clientes, incluindo valores na XP e declarados em outras instituições', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador';
GO

-- Documentação das colunas (seleção das principais)
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data do snapshot mensal (último dia do mês). WHERE data_ref = ''2025-01-31'' para mês específico. MAX(data_ref) para posição atual. Base para séries históricas.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'data_ref';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID do cliente. Combine com data_ref para análise temporal por cliente. COUNT DISTINCT para base ativa. Chave primária composta com data_ref.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'cod_xp';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor. Chave para análises por carteira. JOIN com silver_dim_pessoas. GROUP BY para métricas de assessor. Base para rankings e metas individuais.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'cod_aai';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Profissão declarada texto livre. WHERE profissao LIKE ''%médico%'' para profissionais saúde. Muito variado, use LIKE. GROUP BY TOP 20 para profissões mais comuns. Base para ofertas direcionadas.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'profissao';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Gênero: M ou F. WHERE sexo = ''F'' para análises específicas. GROUP BY para distribuição de gênero. Combine com idade para segmentações demográficas.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'sexo';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Classificação de atendimento. Private (alta renda), Varejo, Empresas. Define nível de serviço e produtos elegíveis. WHERE segmento = ''Private'' para alto valor.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'segmento';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de abertura da conta na XP. DATEDIFF(month, data_cadastro, data_ref) para tempo de relacionamento. WHERE YEAR(data_cadastro) = 2024 para safra específica. Base para cohort analysis.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'data_cadastro';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag histórico se já fez 2º aporte: 1=sim, 0=não. Indica engajamento inicial. Clientes com 2º aporte têm maior lifetime value. Base para scoring.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'fez_segundo_aporte';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data nascimento do cliente. DATEDIFF(year, data_nascimento, GETDATE()) para idade. Segmente por faixa etária. NULL comum por privacidade. Importante para produtos específicos.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'data_nascimento';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Status ativo: 1=ativo (tem saldo), 0=inativo. WHERE status_cliente = 1 para base ativa. SUM(status_cliente) para contagem rápida de ativos. Fundamental para métricas.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'status_cliente';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag se cliente ativou no mês: 1=sim (primeira aplicação), 0=não. WHERE ativou_em_M = 1 para novos investidores. SUM para total de ativações. KPI importante.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'ativou_em_M';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag se cliente evadiu (zerou saldo): 1=sim, 0=não. WHERE evadiu_em_M = 1 para análise de churn. SUM para total de evasões. Alerta máximo para retenção.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'evadiu_em_M';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag se operou bolsa/ações no mês: 1=sim, 0=não. WHERE operou_bolsa = 1 para ativos no produto. Indica diversificação e atividade.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'operou_bolsa';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag se operou fundos de investimento no mês: 1=sim, 0=não. WHERE operou_fundo = 1 para ativos no produto. Indica diversificação e atividade.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'operou_fundo';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag se operou renda fixa no mês: 1=sim, 0=não. WHERE operou_renda_fixa = 1 para ativos no produto. Indica diversificação e atividade.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'operou_renda_fixa';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Patrimônio total declarado pelo cliente em R$. Pode incluir valores fora da XP. Compare com net_em_M para validar. WHERE aplicacao_financeira_declarada > 1000000 para HNW.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'aplicacao_financeira_declarada';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Receita total gerada pelo cliente no mês em R$. Soma de todas as receitas. WHERE receita_mes > 1000 para top revenue. Base para segmentação por rentabilidade.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'receita_mes';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Receita específica de corretagem de ações e taxas B3 no mês em R$. Detalha origem da rentabilidade. WHERE receita_bovespa > 0 para clientes ativos no produto.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'receita_bovespa';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Receita específica de operações no mercado futuro no mês em R$. Detalha origem da rentabilidade. WHERE receita_futuros > 0 para clientes ativos no produto.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'receita_futuros';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Receita específica de produtos bancários (CDB, LCI, LCA) no mês em R$. Detalha origem da rentabilidade. WHERE receita_rf_bancarios > 0 para clientes ativos no produto.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'receita_rf_bancarios';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Receita específica de títulos privados (debêntures, CRI, CRA) no mês em R$. Detalha origem da rentabilidade. WHERE receita_rf_privados > 0 para clientes ativos no produto.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'receita_rf_privados';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Receita específica de títulos públicos (Tesouro Direto) no mês em R$. Detalha origem da rentabilidade. WHERE receita_rf_publicos > 0 para clientes ativos no produto.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'receita_rf_publicos';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Total de entradas no mês em R$ (sem descontar resgates). WHERE captacao_bruta_em_M > 0 para clientes que aportaram. Base para análise de engajamento.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'captacao_bruta_em_M';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Total de resgates/saídas no mês em R$. WHERE resgate_em_M > net_em_M * 0.5 para grandes resgates proporcionais. Sinal de alerta para retenção.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'resgate_em_M';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Captação líquida no mês (entradas - saídas) em R$. Métrica chave de crescimento. WHERE captacao_liquida_em_M < 0 para clientes em evasão. SUM para net total.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'captacao_liquida_em_M';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Captação via TED (transferência bancária) no mês em R$. Canal tradicional, tickets maiores. Principal forma de entrada de recursos.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'captacao_TED';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Captação via ST no mês em R$. Verificar significado específico com XP. Menos comum que TED.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'captacao_ST';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Captação via OTA (Oferta Pública/IPO) no mês em R$. Participação em lançamentos. Cliente sofisticado e ativo.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'captacao_OTA';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Captação direta em renda fixa no mês em R$. Preferência por produtos conservadores. Base da pirâmide de investimentos.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'captacao_RF';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Captação via TD (Tesouro Direto) no mês em R$. Indica interesse em títulos públicos. Perfil conservador educado.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'captacao_TD';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Captação em previdência no mês em R$. Aportes recorrentes comuns. Alta fidelização e visão longo prazo.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'captacao_PREV';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Patrimônio no mês anterior (M-1) em R$. (net_em_M - net_em_M_1) para variação mensal. Identifique crescimento ou redução de patrimônio. Base para análise de tendência.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'net_em_M_1';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Patrimônio total na XP no mês atual em R$. Soma de todos produtos. SUM para AUM total. AVG para ticket médio. WHERE net_em_M > 0 para clientes com saldo. Métrica principal.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'net_em_M';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Patrimônio em renda fixa (CDB, LCI, LCA, Tesouro) em R$. Perfil conservador. WHERE net_renda_fixa/net_em_M > 0.8 para muito conservadores. Base para ofertas de diversificação.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'net_renda_fixa';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Patrimônio em FIIs (Fundos Imobiliários) em R$. Investidor de renda passiva. WHERE net_fundos_imobiliarios > 0 identifica esse perfil. Popular entre aposentados.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'net_fundos_imobiliarios';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Patrimônio em ações, ETFs, BDRs em R$. Perfil arrojado. WHERE net_renda_variavel > 0 para investidores em bolsa. Correlaciona com maior sofisticação e atividade.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'net_renda_variavel';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Patrimônio em fundos de investimento em R$. Inclui multimercados, ações, renda fixa. Produto de maior margem. WHERE net_fundos/net_em_M > 0.5 para fund lovers.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'net_fundos';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Saldo em conta corrente/financeiro em R$. Recursos não investidos, disponíveis. WHERE net_financeiro > 100000 para oportunidades de investimento. Indica liquidez do cliente.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'net_financeiro';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Patrimônio em previdência privada (PGBL/VGBL) em R$. Visão de longo prazo. Alta fidelização. WHERE net_previdencia > 0 para clientes com previdência. Benefício fiscal.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'net_previdencia';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Patrimônio em outros produtos (COE, Debentures, etc) em R$. Produtos sofisticados ou específicos. Indica cliente qualificado quando > 0.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'net_outros';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Receita específica de aluguel de ações no mês em R$. Detalha origem da rentabilidade. WHERE receita_aluguel > 0 para clientes ativos no produto.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'receita_aluguel';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Timestamp de carga no DW. Controle de processamento. WHERE data_carga = MAX(data_carga) para último processamento. Auditoria de ETL.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_positivador', 
    @level2type=N'COLUMN',@level2name=N'data_carga';
GO

-- ==============================================================================
-- 10. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                    | Descrição
--------|------------|--------------------------|--------------------------------------------
1.0.0   | 2025-01-17 | arquitetura.dados       | Criação inicial da tabela

*/

-- ==============================================================================
-- 11. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Snapshot mensal no último dia do mês
- Chave primária composta: data_ref + cod_xp + cod_aai
- Muitas métricas podem ser NULL/0 para clientes inativos
- Receitas são do mês, patrimônio é posição final

Troubleshooting comum:
1. Cliente duplicado: Verificar se tem múltiplos assessores
2. Patrimônio negativo: Possível em derivativos
3. Performance: Volume alto, considerar particionamento por data_ref

Queries úteis:
-- Evolução patrimonial por assessor
SELECT 
    cod_aai,
    data_ref,
    SUM(net_em_M) as aum_total,
    SUM(captacao_liquida_em_M) as captacao_liquida,
    COUNT(DISTINCT CASE WHEN status_cliente = 1 THEN cod_xp END) as clientes_ativos,
    SUM(receita_mes) as receita_total
FROM bronze.xp_positivador
WHERE data_ref >= DATEADD(MONTH, -12, GETDATE())
GROUP BY cod_aai, data_ref
ORDER BY cod_aai, data_ref;

-- Top clientes por patrimônio
SELECT TOP 100
    p.cod_xp,
    p.cod_aai,
    p.net_em_M,
    p.aplicacao_financeira_declarada,
    p.receita_mes,
    p.segmento,
    DATEDIFF(MONTH, p.data_cadastro, p.data_ref) as meses_relacionamento
FROM bronze.xp_positivador p
WHERE p.data_ref = (SELECT MAX(data_ref) FROM bronze.xp_positivador)
    AND p.status_cliente = 1
ORDER BY p.net_em_M DESC;

-- Análise de diversificação
SELECT 
    cod_aai,
    COUNT(DISTINCT cod_xp) as qtd_clientes,
    AVG(CASE WHEN net_em_M > 0 
        THEN (CASE WHEN net_renda_fixa > 0 THEN 1 ELSE 0 END +
              CASE WHEN net_renda_variavel > 0 THEN 1 ELSE 0 END +
              CASE WHEN net_fundos > 0 THEN 1 ELSE 0 END +
              CASE WHEN net_previdencia > 0 THEN 1 ELSE 0 END +
              CASE WHEN net_fundos_imobiliarios > 0 THEN 1 ELSE 0 END)
        ELSE 0 END) as media_produtos_por_cliente
FROM bronze.xp_positivador
WHERE data_ref = (SELECT MAX(data_ref) FROM bronze.xp_positivador)
    AND status_cliente = 1
GROUP BY cod_aai
HAVING COUNT(DISTINCT cod_xp) >= 10
ORDER BY media_produtos_por_cliente DESC;

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
PRINT 'Tabela bronze.xp_positivador criada com sucesso!';
GO
