-- ==============================================================================
-- QRY-IEA-005-create_gold_indice_esforco_assessor
-- ==============================================================================
-- Tipo: CREATE TABLE
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [equipe.dados@m7investimentos.com.br]
-- Revisor: [revisor@m7investimentos.com.br]
-- Tags: [indice_esforco, assessor, performance, kpi, gold]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela física para armazenar o Índice de Esforço do Assessor (IEA)
e suas métricas componentes. O IEA é um indicador composto que mede o desempenho 
comercial dos assessores em duas dimensões: prospecção e relacionamento.

Casos de uso:
- Dashboard executivo de performance comercial
- Ranking e classificação de assessores por esforço
- Análise de correlação entre esforço e resultados
- Base para cálculo de remuneração variável
- Identificação de gaps de performance

Frequência de atualização: Diária (via procedure prc_gold_indice_esforco_assessor)
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
Tabela criada: gold.indice_esforco_assessor

Chave lógica: ano_mes + cod_assessor (sem PK física definida)

Particionamento: Não aplicado
Índices: Recomenda-se criar índice clustered em (ano_mes, cod_assessor)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- gold.vw_indice_esforco_assessor: View que consolida os dados (populada via procedure)

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
CREATE TABLE [gold].[indice_esforco_assessor](
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
	[indice_esforco_assessor] [decimal](18, 8) NOT NULL,
	[indice_esforco_assessor_trimestre] [decimal](18, 8) NOT NULL,
	[indice_esforco_assessor_semestre] [decimal](18, 8) NOT NULL,
	[indice_esforco_assessor_ano] [decimal](18, 8) NOT NULL,
	[indice_esforco_assessor_3_meses] [decimal](18, 8) NULL,
	[indice_esforco_assessor_6_meses] [decimal](18, 8) NULL,
	[indice_esforco_assessor_12_meses] [decimal](18, 8) NULL,
	[esforco_prospeccao] [decimal](18, 8) NOT NULL,
	[esforco_relacionamento] [decimal](18, 8) NOT NULL,
	[prospeccao_captacao_de_novos_clientes_por_aai] [decimal](16, 2) NOT NULL,
	[prospeccao_atingimento_lead_starts] [decimal](18, 8) NOT NULL,
	[prospeccao_atingimento_habilitacoes] [decimal](18, 8) NOT NULL,
	[prospeccao_atingimento_conversao] [decimal](18, 8) NOT NULL,
	[prospeccao_atingimento_carteiras_simuladas_novos] [decimal](18, 8) NOT NULL,
	[relacionamento_captacao_da_base] [decimal](18, 2) NOT NULL,
	[relacionamento_atingimento_contas_aportarem] [decimal](18, 8) NOT NULL,
	[relacionamento_atingimento_ordens_enviadas] [decimal](18, 8) NOT NULL,
	[relacionamento_atingimento_contas_acessadas_hub] [decimal](18, 8) NOT NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [gold].[indice_esforco_assessor] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Chave temporal no formato YYYYMM (ex: 202506 para junho/2025). Representa o ano e mês de referência para agregação dos dados.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'ano_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Ano de referência extraído do período (ex: 2025). Utilizado para filtros e agregações anuais.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Mês de referência numérico (1-12). Janeiro = 1, Dezembro = 12.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome do mês por extenso em inglês (ex: January, February). Facilita visualizações e relatórios.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'nome_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Trimestre do ano (Q1, Q2, Q3, Q4). Q1 = Jan-Mar, Q2 = Abr-Jun, Q3 = Jul-Set, Q4 = Out-Dez.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Semestre do ano (S1, S2). S1 = Janeiro a Junho, S2 = Julho a Dezembro.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código único do assessor no sistema XP (código AAI). Chave principal junto com ano_mes.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ID do assessor no sistema CRM. Permite joins com outras tabelas que usam CRM ID como referência.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'crm_id_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome completo do assessor. Obtido da dim_pessoas para facilitar identificação em relatórios.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'nome_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nível hierárquico do assessor (Junior, Pleno, Senior). Permite análises segmentadas por senioridade.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'nivel_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ID da estrutura organizacional à qual o assessor pertence. FK para dim_estruturas.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'estrutura_id'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome da estrutura organizacional (ex: Alta Renda-Fortaleza, Mesa Digital). Facilita análises por equipe.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'estrutura_nome'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Índice de Esforço do Assessor (IEA) no mês. Média entre esforço de prospecção e relacionamento. Valor entre 0 e 1, onde 1 representa máximo esforço.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'indice_esforco_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'IEA médio acumulado no trimestre corrente (desde o início do trimestre até o mês atual).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'indice_esforco_assessor_trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'IEA médio acumulado no semestre corrente (desde o início do semestre até o mês atual).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'indice_esforco_assessor_semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'IEA médio acumulado no ano corrente (desde janeiro até o mês atual).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'indice_esforco_assessor_ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'IEA médio dos últimos 3 meses móveis (janela móvel incluindo o mês atual).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'indice_esforco_assessor_3_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'IEA médio dos últimos 6 meses móveis (janela móvel incluindo o mês atual).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'indice_esforco_assessor_6_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'IEA médio dos últimos 12 meses móveis (janela móvel incluindo o mês atual).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'indice_esforco_assessor_12_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Índice de esforço em prospecção. Média ponderada dos indicadores de prospecção (lead starts, habilitações, conversão, carteiras simuladas). Valor entre 0 e 1.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'esforco_prospeccao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Índice de esforço em relacionamento. Média ponderada dos indicadores de relacionamento (contas acessadas, contas que aportaram, ordens enviadas). Valor entre 0 e 1.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'esforco_relacionamento'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Valor de captação de novos clientes por AAI no período. Valor monetário em reais.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'prospeccao_captacao_de_novos_clientes_por_aai'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Percentual de atingimento da meta de lead starts (novos leads iniciados). Valor entre 0 e 1, onde 1 = 100% da meta.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'prospeccao_atingimento_lead_starts'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Percentual de atingimento da meta de habilitações de novos clientes. Valor entre 0 e 1, onde 1 = 100% da meta.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'prospeccao_atingimento_habilitacoes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Percentual de atingimento da meta de conversão de leads em clientes. Valor entre 0 e 1, onde 1 = 100% da meta.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'prospeccao_atingimento_conversao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Percentual de atingimento da meta de carteiras simuladas para novos clientes. Valor entre 0 e 1, onde 1 = 100% da meta.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'prospeccao_atingimento_carteiras_simuladas_novos'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Valor de captação líquida da base de clientes existentes. Valor monetário em reais, pode ser negativo (resgate líquido).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'relacionamento_captacao_da_base'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Percentual de atingimento da meta de contas que realizaram aportes. Valor entre 0 e 1, onde 1 = 100% da meta.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'relacionamento_atingimento_contas_aportarem'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Percentual de atingimento da meta de ordens enviadas. Valor entre 0 e 1, onde 1 = 100% da meta.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'relacionamento_atingimento_ordens_enviadas'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Percentual de atingimento da meta de contas acessadas no Hub XP. Valor entre 0 e 1, onde 1 = 100% da meta.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'relacionamento_atingimento_contas_acessadas_hub'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de carga dos dados. Registra quando os dados foram inseridos ou atualizados na tabela.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela agregada mensal com métricas do Índice de Esforço do Assessor (IEA). Consolida indicadores de prospecção e relacionamento para medir o desempenho comercial dos assessores. Inclui métricas acumuladas em diferentes janelas temporais. Granularidade: uma linha por assessor por mês.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'indice_esforco_assessor'
GO

-- ==============================================================================
-- 7. DEFAULTS E CONSTRAINTS  
-- ==============================================================================
-- A tabela não possui chave primária definida, o que pode impactar performance.
-- Recomenda-se adicionar:
-- ALTER TABLE [gold].[indice_esforco_assessor] 
-- ADD CONSTRAINT PK_indice_esforco_assessor PRIMARY KEY CLUSTERED (ano_mes, cod_assessor)
--
-- O único default definido é para data_carga = GETDATE()

-- ==============================================================================
-- 8. EXTENDED PROPERTIES (DOCUMENTAÇÃO)
-- ==============================================================================
-- Todas as colunas possuem extended properties detalhadas descrevendo:
-- - Propósito e conteúdo da coluna
-- - Formato e valores esperados
-- - Relação com outras colunas/tabelas
-- - Regras de negócio aplicadas

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
- IEA varia de 0 a 1, onde 1 representa esforço máximo
- Métricas de atingimento também variam de 0 a 1 (percentual da meta)
- Valores monetários estão em reais (R$)
- Captação da base pode ser negativa (resgates líquidos)

Cálculo do IEA:
- IEA = (esforco_prospeccao + esforco_relacionamento) / 2
- Cada componente é uma média ponderada de seus indicadores

Troubleshooting comum:
1. Valores NULL em janelas móveis: Normal nos primeiros meses de dados
2. IEA zerado: Verificar se há dados fonte para o assessor no período
3. Duplicação de registros: Adicionar PK ou verificar procedure de carga

Contato para dúvidas: equipe-dados@m7investimentos.com.br
*/
