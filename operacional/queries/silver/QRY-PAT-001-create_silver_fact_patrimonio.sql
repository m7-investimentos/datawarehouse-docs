-- ==============================================================================
-- QRY-PAT-001-CREATE_SILVER_FACT_PATRIMONIO
-- ==============================================================================
-- Tipo: DDL - Create Table
-- Versão: 1.0.0
-- Última atualização: 2025-01-13
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [ddl, silver, fact_table, patrimonio, cliente, open_investment, share_of_wallet]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela fato fact_patrimonio no schema silver para armazenar 
           informações consolidadas sobre o patrimônio dos clientes, incluindo:
           - Patrimônio custodiado na XP
           - Patrimônio total declarado pelo cliente
           - Share of wallet (participação da XP no patrimônio total)
           - Patrimônio em outras instituições (Open Investment)

Casos de uso:
- Análise de share of wallet para identificar potencial de crescimento
- Acompanhamento da evolução patrimonial dos clientes
- Identificação de oportunidades de captação e cross-sell
- Segmentação de clientes por patrimônio total vs custodiado
- Relatórios gerenciais de patrimônio consolidado
- Análise de competitividade (quanto está fora da XP)

Frequência de execução: Única (criação da tabela)
Tempo médio de execução: < 5 segundos
Volume esperado de linhas: ~1M registros/mês (snapshot mensal por cliente)
*/

-- ==============================================================================
-- 2. ESTRUTURA DA TABELA
-- ==============================================================================
/*
Colunas da tabela:

| Coluna                      | Tipo           | Nullable | Descrição                                                                        |
|-----------------------------|----------------|----------|-----------------------------------------------------------------------------------|
| data_ref                    | DATE           | NOT NULL | Data de referência do snapshot patrimonial (último dia do mês)                  |
| conta_xp_cliente            | INT            | NOT NULL | Número da conta do cliente na XP                                                |
| patrimonio_xp               | DECIMAL(18,2)  | NULL     | Valor total do patrimônio custodiado na XP em reais                            |
| patrimonio_declarado        | DECIMAL(18,2)  | NULL     | Valor total do patrimônio declarado pelo cliente em reais                      |
| share_of_wallet             | DECIMAL(18,2)  | NULL     | Percentual do patrimônio total que está na XP (0-100)                          |
| patrimonio_open_investment  | DECIMAL(18,2)  | NULL     | Valor estimado em outras instituições (declarado - XP)                         |

Chave primária: Não definida explicitamente
Índices recomendados: 
- Índice clustered em (data_ref, conta_xp_cliente)
- Índice non-clustered em conta_xp_cliente para consultas por cliente
*/

-- ==============================================================================
-- 3. DEPENDÊNCIAS
-- ==============================================================================
/*
Schemas necessários:
- silver: Schema onde a tabela será criada

Tabelas relacionadas (para carga):
- [bronze].[xp_patrimonio_cliente]: Dados de patrimônio na XP
  * Origem: Sistema de custódia XP
  * Atualização: Diária
  
- [bronze].[xp_cadastro_cliente]: Dados cadastrais incluindo patrimônio declarado
  * Origem: Sistema de cadastro XP
  * Atualização: Sob demanda (quando cliente atualiza)
  
- [bronze].[open_investment_patrimonio]: Dados de Open Investment (quando disponível)
  * Origem: APIs de Open Banking/Investment
  * Atualização: Mensal

Processo ETL:
- Procedure: [silver].[prc_load_silver_fact_patrimonio]
- Frequência: Mensal 
- Tipo: Incremental por data_ref

Pré-requisitos:
- Schema silver deve existir
- Usuário deve ter permissão CREATE TABLE no schema silver
- Tabelas bronze devem estar populadas
*/

-- ==============================================================================
-- 4. SCRIPT DE CRIAÇÃO
-- ==============================================================================

-- Configurações do SQL Server
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Criação da tabela

CREATE TABLE [silver].[fact_patrimonio](
	[data_ref] [date] NOT NULL,
	[conta_xp_cliente] [int] NOT NULL,
	[patrimonio_xp] [decimal](18, 2) NULL,
	[patrimonio_declarado] [decimal](18, 2) NULL,
	[share_of_wallet] [decimal](18, 2) NULL,
	[patrimonio_open_investment] [decimal](18, 2) NULL
) ON [PRIMARY]
GO

-- ==============================================================================
-- 5. DOCUMENTAÇÃO DAS COLUNAS (EXTENDED PROPERTIES)
-- ==============================================================================
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de referência para os dados de patrimônio' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Número da conta do cliente na XP' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio', @level2type=N'COLUMN',@level2name=N'conta_xp_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Valor do patrimônio do cliente na XP' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio', @level2type=N'COLUMN',@level2name=N'patrimonio_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Valor do patrimônio declarado pelo cliente' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio', @level2type=N'COLUMN',@level2name=N'patrimonio_declarado'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Percentual do patrimônio total do cliente que está custodiado na XP (0-100). Calculado como: (patrimonio_xp / patrimonio_declarado) * 100' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio', @level2type=N'COLUMN',@level2name=N'share_of_wallet'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Valor estimado do patrimônio do cliente em outras instituições financeiras. Pode vir de Open Investment ou ser calculado como: patrimonio_declarado - patrimonio_xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio', @level2type=N'COLUMN',@level2name=N'patrimonio_open_investment'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela fato contendo snapshots mensais do patrimônio dos clientes, incluindo valores na XP, patrimônio total declarado, share of wallet e estimativa de valores em outras instituições' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio'
GO

-- ==============================================================================
-- 6. ÍNDICES RECOMENDADOS
-- ==============================================================================
/*
-- Índice clustered para otimizar consultas por período e cliente
CREATE CLUSTERED INDEX [IX_fact_patrimonio_data_ref_conta] 
ON [silver].[fact_patrimonio] ([data_ref], [conta_xp_cliente])
GO

-- Índice para consultas por cliente ao longo do tempo
CREATE NONCLUSTERED INDEX [IX_fact_patrimonio_conta_data] 
ON [silver].[fact_patrimonio] ([conta_xp_cliente], [data_ref])
INCLUDE ([patrimonio_xp], [patrimonio_declarado], [share_of_wallet])
GO

-- Índice para análises de share of wallet
CREATE NONCLUSTERED INDEX [IX_fact_patrimonio_share_wallet] 
ON [silver].[fact_patrimonio] ([share_of_wallet])
WHERE [share_of_wallet] IS NOT NULL
GO
*/

-- ==============================================================================
-- 7. VALIDAÇÕES E CONSTRAINTS
-- ==============================================================================
/*
-- Constraint para garantir que share_of_wallet está entre 0 e 100
ALTER TABLE [silver].[fact_patrimonio]
ADD CONSTRAINT [CK_fact_patrimonio_share_wallet] 
CHECK ([share_of_wallet] >= 0 AND [share_of_wallet] <= 100)
GO

-- Constraint para garantir valores não negativos
ALTER TABLE [silver].[fact_patrimonio]
ADD CONSTRAINT [CK_fact_patrimonio_valores_positivos] 
CHECK (
    ([patrimonio_xp] IS NULL OR [patrimonio_xp] >= 0) AND
    ([patrimonio_declarado] IS NULL OR [patrimonio_declarado] >= 0) AND
    ([patrimonio_open_investment] IS NULL OR [patrimonio_open_investment] >= 0)
)
GO
*/

-- ==============================================================================
-- 8. QUERIES DE VALIDAÇÃO
-- ==============================================================================
/*
-- Query para verificar a criação da tabela
SELECT 
    t.name AS tabela,
    s.name AS schema_name,
    t.create_date,
    t.modify_date
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'silver' AND t.name = 'fact_patrimonio';

-- Query para verificar estrutura das colunas
SELECT 
    c.name AS coluna,
    t.name AS tipo_dado,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    ep.value AS descricao
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
LEFT JOIN sys.extended_properties ep ON 
    ep.major_id = c.object_id AND 
    ep.minor_id = c.column_id AND 
    ep.name = 'MS_Description'
WHERE c.object_id = OBJECT_ID('silver.fact_patrimonio')
ORDER BY c.column_id;
*/

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-13 | [Nome]         | Criação inicial da tabela

*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Tabela armazena snapshots mensais (não é SCD - Slowly Changing Dimension)
- data_ref sempre representa o último dia útil do mês
- Valores NULL são permitidos pois nem sempre temos todas as informações
- share_of_wallet só é calculado quando temos ambos valores (XP e declarado)
- patrimonio_open_investment pode vir de 3 fontes:
  1. APIs de Open Investment (mais preciso)
  2. Cálculo: patrimonio_declarado - patrimonio_xp (estimativa)
  3. NULL quando não temos informação suficiente

Regras de negócio:
- Cliente pode ter patrimônio XP zerado mas patrimônio declarado alto
- Share of wallet pode ser > 100% se cliente tem mais na XP do que declarou
- Patrimônio declarado é atualizado apenas quando cliente informa
- Alguns clientes podem não ter patrimônio declarado (NULL)

Cálculos importantes:
1. share_of_wallet = (patrimonio_xp / patrimonio_declarado) * 100
2. patrimonio_open_investment = patrimonio_declarado - patrimonio_xp

Oportunidades de melhoria:
1. Adicionar chave primária composta (data_ref, conta_xp_cliente)
2. Implementar particionamento por data_ref para grandes volumes
3. Adicionar coluna para fonte do patrimônio_open_investment
4. Incluir data da última atualização do patrimônio declarado
5. Adicionar flag indicando se cliente tem Open Investment ativo

Integração com outros processos:
- Alimenta dashboards de share of wallet
- Base para cálculo de metas de captação
- Entrada para modelos de propensão
- Usado em relatórios regulatórios

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
