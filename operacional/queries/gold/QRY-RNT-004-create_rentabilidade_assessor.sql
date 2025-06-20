-- ==============================================================================
-- QRY-RNT-004-create_rentabilidade_assessor
-- ==============================================================================
-- Tipo: CREATE TABLE
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [equipe.dados@m7investimentos.com.br]
-- Revisor: [revisor@m7investimentos.com.br]
-- Tags: [rentabilidade, assessor, performance, cdi, gold]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela física para armazenar métricas de rentabilidade por assessor,
comparando a performance das carteiras dos clientes (PF com patrimônio >= R$ 300k)
em relação ao CDI.

Casos de uso:
- Dashboard de performance de rentabilidade por assessor
- Análise comparativa de rentabilidade vs benchmark (CDI)
- Identificação de assessores com melhores resultados para clientes
- Base para políticas de remuneração variável
- Acompanhamento de metas de rentabilidade

Frequência de atualização: Diária (via procedure prc_gold_rentabilidade_assessor)
Volume esperado de linhas: ~2.000 registros/mês (1 por assessor ativo)
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Não aplicável - Script DDL de criação de tabela
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela criada: gold.rentabilidade_assessor

Chave lógica: ano_mes + cod_assessor (sem PK física definida)

Particionamento: Não aplicado
Índices: Recomenda-se criar índice clustered em (ano_mes, cod_assessor)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- gold.vw_rentabilidade_assessor: View que consolida os dados (populada via procedure)

Pré-requisitos:
- Schema gold deve existir
- Permissões CREATE TABLE no schema gold
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ==============================================================================
-- 6. CRIAÇÃO DA TABELA
-- ==============================================================================
CREATE TABLE [gold].[rentabilidade_assessor](
	[ano_mes] [int] NOT NULL,
	[ano] [int] NOT NULL,
	[mes] [int] NOT NULL,
	[nome_mes] [varchar](20) NOT NULL,
	[trimestre] [varchar](2) NOT NULL,
	[semestre] [varchar](2) NOT NULL,
	[cod_assessor] [varchar](20) NOT NULL,
	[codigo_crm_assessor] [varchar](20) NULL,
	[nome_assessor] [varchar](200) NOT NULL,
	[nivel_assessor] [varchar](50) NULL,
	[estrutura_id] [int] NULL,
	[estrutura_nome] [varchar](100) NULL,
	[qtd_clientes_300k_mais] [int] NOT NULL,
	[qtd_clientes_acima_cdi] [int] NOT NULL,
	[qtd_clientes_faixa_80_cdi] [int] NOT NULL,
	[qtd_clientes_faixa_50_cdi] [int] NOT NULL,
	[qtd_clientes_rentabilidade_positiva] [int] NOT NULL,
	[perc_clientes_acima_cdi] [decimal](12, 6) NOT NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT ((0)) FOR [qtd_clientes_300k_mais]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT ((0)) FOR [qtd_clientes_acima_cdi]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT ((0)) FOR [qtd_clientes_faixa_80_cdi]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT ((0)) FOR [qtd_clientes_faixa_50_cdi]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT ((0)) FOR [qtd_clientes_rentabilidade_positiva]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT ((0)) FOR [perc_clientes_acima_cdi]
GO
ALTER TABLE [gold].[rentabilidade_assessor] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
-- Extended properties para documentação das colunas
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Chave temporal no formato YYYYMM (ex: 202501 para janeiro/2025). Representa o ano e mês de referência.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'ano_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Ano de referência extraído do período (ex: 2025).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Mês de referência numérico (1-12). Janeiro = 1, Dezembro = 12.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome do mês por extenso em português (ex: Janeiro, Fevereiro).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'nome_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Trimestre do ano (Q1, Q2, Q3, Q4). Q1 = Jan-Mar, Q2 = Abr-Jun, Q3 = Jul-Set, Q4 = Out-Dez.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Semestre do ano (S1, S2). S1 = Janeiro a Junho, S2 = Julho a Dezembro.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código único do assessor no sistema XP (código AAI). Chave lógica junto com ano_mes.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ID do assessor no sistema CRM. Permite joins com outras tabelas que usam CRM ID.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'codigo_crm_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome completo do assessor comercial.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'nome_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nível hierárquico do assessor (Junior, Pleno, Senior).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'nivel_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ID da estrutura organizacional à qual o assessor pertence.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'estrutura_id'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome da estrutura organizacional (ex: Alta Renda-Fortaleza).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'estrutura_nome'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de clientes PF com patrimônio >= R$ 300.000 no período.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'qtd_clientes_300k_mais'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de clientes com rentabilidade mensal acima do CDI.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'qtd_clientes_acima_cdi'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de clientes com rentabilidade >= 80% do CDI mensal.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'qtd_clientes_faixa_80_cdi'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de clientes com rentabilidade >= 50% do CDI mensal.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'qtd_clientes_faixa_50_cdi'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de clientes com rentabilidade positiva (> 0%) no mês.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'qtd_clientes_rentabilidade_positiva'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Percentual de clientes com rentabilidade acima do CDI. Ex: 0.652100 = 65.21%' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'perc_clientes_acima_cdi'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data em que os dados foram carregados/atualizados na tabela.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela agregada mensal com métricas de rentabilidade por assessor. Compara a performance das carteiras dos clientes PF (patrimônio >= R$ 300k) em relação ao CDI. Granularidade: uma linha por assessor por mês.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'rentabilidade_assessor'
GO

-- ==============================================================================
-- 7. DEFAULTS E CONSTRAINTS
-- ==============================================================================
-- A tabela não possui chave primária física definida, o que pode impactar performance.
-- Recomenda-se adicionar:
-- ALTER TABLE [gold].[rentabilidade_assessor] 
-- ADD CONSTRAINT PK_rentabilidade_assessor PRIMARY KEY CLUSTERED (ano_mes, cod_assessor)
--
-- Defaults definidos:
-- - Todas as métricas de quantidade com default 0
-- - perc_clientes_acima_cdi com default 0
-- - data_carga com default GETDATE()

-- ==============================================================================
-- 8. EXTENDED PROPERTIES (DOCUMENTAÇÃO)
-- ==============================================================================
-- Todas as colunas possuem extended properties detalhadas descrevendo:
-- - Propósito e conteúdo da coluna
-- - Formato e valores esperados
-- - Relação com outras colunas/tabelas
-- - Comparações com benchmark CDI

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-20 | equipe.dados   | Criação inicial da tabela

*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Tabela não possui PK física definida - considerar adicionar para melhor performance
- Considera apenas clientes PF com patrimônio >= R$ 300.000
- Rentabilidade comparada com CDI do mesmo período
- Percentuais armazenados em decimal (0.65 = 65%, não 65)
- Estrutura do assessor é a vigente no período analisado

Faixas de rentabilidade:
- Acima CDI: rentabilidade > 100% do CDI
- Faixa 80%: rentabilidade >= 80% do CDI
- Faixa 50%: rentabilidade >= 50% do CDI
- Positiva: rentabilidade > 0%

Troubleshooting comum:
1. Valores zerados: Verificar se assessor tem clientes 300k+ com rentabilidade
2. Percentual baixo: Normal em períodos de alta volatilidade
3. Dados faltantes: Verificar processamento da silver.fact_rentabilidade_clientes

Contato para dúvidas: equipe-dados@m7investimentos.com.br
*/
