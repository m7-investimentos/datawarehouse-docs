-- ==============================================================================
-- QRY-NPS-006-create_gold_nps_assessor_aniversario
-- ==============================================================================
-- Tipo: CREATE TABLE
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [equipe.dados@m7investimentos.com.br]
-- Revisor: [revisor@m7investimentos.com.br]
-- Tags: [nps, assessor, aniversario, satisfacao, gold]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela física para armazenar métricas de Net Promoter Score (NPS)
por assessor, baseadas em pesquisas enviadas em aniversários de clientes.
Consolida dados mensais de satisfação e recomendação.

Casos de uso:
- Dashboard de NPS por assessor e estrutura
- Análise de evolução da satisfação dos clientes
- Identificação de assessores promotores vs detratores
- Base para planos de ação de melhoria de atendimento
- Correlação entre NPS e resultados comerciais

Frequência de atualização: Diária (via procedure prc_gold_nps_assessor_aniversario)
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
Tabela criada: gold.nps_assessor_aniversario

Chave lógica: ano_mes + cod_assessor (sem PK física definida)

Particionamento: Não aplicado
Índices: Recomenda-se criar índice clustered em (ano_mes, cod_assessor)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- gold.vw_nps_assessor_aniversario: View que consolida os dados (populada via procedure)

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
CREATE TABLE [gold].[nps_assessor_aniversario](
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
	[qtd_pesquisas_enviadas] [int] NOT NULL,
	[qtd_pesquisas_respondidas] [int] NOT NULL,
	[qtd_convites_abertos] [int] NOT NULL,
	[taxa_resposta] [decimal](10, 6) NULL,
	[taxa_abertura] [decimal](10, 6) NULL,
	[nps_score_assessor_mes] [decimal](10, 6) NULL,
	[nps_score_assessor_trimestre] [decimal](10, 6) NULL,
	[nps_score_assessor_semestre] [decimal](10, 6) NULL,
	[nps_score_assessor_ano] [decimal](10, 6) NULL,
	[nps_score_assessor_3_meses] [decimal](10, 6) NULL,
	[nps_score_assessor_6_meses] [decimal](10, 6) NULL,
	[nps_score_assessor_12_meses] [decimal](10, 6) NULL,
	[qtd_promotores] [int] NULL,
	[qtd_neutros] [int] NULL,
	[qtd_detratores] [int] NULL,
	[qtd_recomendaria_sim] [int] NULL,
	[perc_recomendaria] [decimal](10, 6) NULL,
	[razao_principal] [varchar](100) NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [gold].[nps_assessor_aniversario] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Chave temporal no formato YYYYMM (ex: 202506 para junho/2025). Representa o ano e mês de referência para agregação dos dados.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'ano_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Ano de referência extraído do período (ex: 2025). Utilizado para filtros e agregações anuais.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Mês de referência numérico (1-12). Janeiro = 1, Dezembro = 12.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome do mês por extenso em português (ex: Janeiro, Fevereiro). Facilita visualizações e relatórios.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'nome_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Trimestre do ano (Q1, Q2, Q3, Q4). Q1 = Jan-Mar, Q2 = Abr-Jun, Q3 = Jul-Set, Q4 = Out-Dez.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Semestre do ano (S1, S2). S1 = Janeiro a Junho, S2 = Julho a Dezembro.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código único do assessor no sistema XP (código AAI). Chave principal junto com ano_mes.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ID do assessor no sistema CRM. Permite joins com outras tabelas que usam CRM ID como referência.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'crm_id_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome completo do assessor. Obtido da dim_pessoas para facilitar identificação em relatórios.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'nome_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nível hierárquico do assessor (Junior, Pleno, Senior). Permite análises segmentadas por senioridade.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'nivel_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ID da estrutura organizacional à qual o assessor pertence. FK para dim_estruturas.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'estrutura_id'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome da estrutura organizacional (ex: Alta Renda, Mesa Digital). Facilita análises por equipe.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'estrutura_nome'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade total de pesquisas NPS enviadas para clientes do assessor no período mensal.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'qtd_pesquisas_enviadas'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de pesquisas efetivamente respondidas pelos clientes no período mensal.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'qtd_pesquisas_respondidas'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de convites de pesquisa que foram abertos pelos clientes (email foi visualizado).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'qtd_convites_abertos'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Taxa de resposta das pesquisas em decimal (qtd_respondidas/qtd_enviadas). Ex: 0.154320 = 15.4320%' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'taxa_resposta'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Taxa de abertura dos convites em decimal (qtd_abertos/qtd_enviados). Ex: 0.652100 = 65.21%' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'taxa_abertura'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Net Promoter Score do assessor no mês corrente. Fórmula: (% Promotores - % Detratores). Ex: 0.725000 = 72.5' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'nps_score_assessor_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'NPS Score médio do assessor no trimestre corrente acumulado (desde o início do trimestre até o mês atual).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'nps_score_assessor_trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'NPS Score médio do assessor no semestre corrente acumulado (desde o início do semestre até o mês atual).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'nps_score_assessor_semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'NPS Score médio do assessor no ano corrente acumulado (desde janeiro até o mês atual).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'nps_score_assessor_ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'NPS Score médio do assessor nos últimos 3 meses móveis (incluindo o mês atual).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'nps_score_assessor_3_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'NPS Score médio do assessor nos últimos 6 meses móveis (incluindo o mês atual).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'nps_score_assessor_6_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'NPS Score médio do assessor nos últimos 12 meses móveis (incluindo o mês atual).' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'nps_score_assessor_12_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de avaliações com notas 9 ou 10 (Promotores) no período mensal.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'qtd_promotores'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de avaliações com notas 7 ou 8 (Neutros) no período mensal.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'qtd_neutros'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de avaliações com notas de 0 a 6 (Detratores) no período mensal.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'qtd_detratores'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de clientes que responderam "sim" para a pergunta se recomendariam o assessor.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'qtd_recomendaria_sim'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Percentual de recomendação em decimal (qtd_sim/qtd_respondidas). Ex: 0.925000 = 92.50%' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'perc_recomendaria'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Principal razão citada pelos clientes para a nota atribuída ao assessor. Mais frequente no período.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario', @level2type=N'COLUMN',@level2name=N'razao_principal'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela agregada mensal com métricas de Net Promoter Score (NPS) por assessor. Consolida dados de pesquisas de satisfação enviadas em aniversários de clientes, incluindo taxas de resposta, scores NPS em diferentes janelas temporais e análise de razões. Granularidade: uma linha por assessor por mês.' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'nps_assessor_aniversario'
GO

-- ==============================================================================
-- 7. DEFAULTS E CONSTRAINTS
-- ==============================================================================
-- A tabela não possui chave primária física definida, o que pode impactar performance.
-- Recomenda-se adicionar:
-- ALTER TABLE [gold].[nps_assessor_aniversario] 
-- ADD CONSTRAINT PK_nps_assessor_aniversario PRIMARY KEY CLUSTERED (ano_mes, cod_assessor)
--
-- O único default definido é para data_carga = GETDATE()

-- ==============================================================================
-- 8. EXTENDED PROPERTIES (DOCUMENTAÇÃO)
-- ==============================================================================
-- Todas as colunas possuem extended properties detalhadas descrevendo:
-- - Propósito e conteúdo da coluna
-- - Formato e valores esperados  
-- - Fórmulas de cálculo quando aplicável
-- - Relação com outras colunas/tabelas

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
- NPS Score varia de -1 a 1 (-100 a +100 quando em percentual)
- Taxas são armazenadas em decimal (0.15 = 15%, não 15)
- Pesquisas são enviadas em aniversários de clientes
- Métricas de envio baseadas em data_entrega, resposta em data_resposta

Cálculo do NPS:
- NPS = (% Promotores - % Detratores)
- Promotores: notas 9-10
- Neutros: notas 7-8  
- Detratores: notas 0-6

Troubleshooting comum:
1. Valores NULL em scores: Normal para assessores sem respostas no período
2. Taxa resposta baixa: Verificar qualidade dos emails e engajamento
3. Divergência envio/resposta: Respostas podem vir em mês diferente do envio

Contato para dúvidas: equipe-dados@m7investimentos.com.br
*/
