-- ==============================================================================
-- QRY-AHE-002-CREATE_SILVER_FACT_ATIVACOES_HABILITACOES_EVASOES
-- ==============================================================================
-- Tipo: DDL - Create Table
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [ddl, silver, fact_table, ativacoes, habilitacoes, evasoes]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela fato fact_ativacoes_habilitacoes_evasoes no schema silver
           para armazenar registros de movimentações de clientes (ativações, 
           habilitações e evasões).

Casos de uso:
- Armazenar histórico de movimentações de clientes
- Base para análises de comportamento de clientes
- Cálculo de indicadores de ativação e evasão
- Suporte a relatórios gerenciais e dashboards

Frequência de execução: Única (criação da tabela)
Volume esperado de linhas: ~500.000 registros/ano
*/

-- ==============================================================================
-- 2. ESTRUTURA DA TABELA
-- ==============================================================================
/*
Colunas da tabela:

| Coluna              | Tipo         | Nullable | Descrição                                      |
|---------------------|--------------|----------|------------------------------------------------|
| data_ref            | DATE         | NOT NULL | Data de referência do registro                 |
| cod_xp              | INT          | NOT NULL | Código do cliente no sistema XP                |
| crm_id              | VARCHAR(20)  | NOT NULL | Identificador do assessor no CRM               |
| id_estrutura        | INT          | NOT NULL | Identificador da estrutura no sistema          |
| faixa_pl            | VARCHAR(50)  | NULL     | Faixa de patrimônio líquido do cliente         |
| tipo_movimentacao   | VARCHAR(50)  | NULL     | Tipo de movimentação (ativação/habilitação/evasão) |

Chave primária: Não definida (considerar adicionar)
Índices: A definir baseado em padrões de consulta
*/

-- ==============================================================================
-- 3. DEPENDÊNCIAS
-- ==============================================================================
/*
Schemas necessários:
- silver: Schema onde a tabela será criada
- bronze: Schema de origem dos dados

Tabelas de origem (bronze):
- [bronze].[xp_ativacoes_habilitacoes_evasoes]: Fonte principal dos dados de movimentações
  * Campos utilizados: data_ref, cod_xp, cod_aai, faixa_pl, tipo_movimentacao

Tabelas relacionadas (silver):
- [silver].[dim_pessoas]: Para obter crm_id através do cod_aai
- [silver].[fact_estrutura_pessoas]: Para obter id_estrutura através do crm_id

Pré-requisitos:
- Schema silver deve existir
- Usuário deve ter permissão CREATE TABLE no schema silver
- Tabelas bronze devem estar populadas antes da carga
*/

-- ==============================================================================
-- 4. SCRIPT DE CRIAÇÃO
-- ==============================================================================

-- Configurações iniciais
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Criação da tabela
CREATE TABLE [silver].[fact_ativacoes_habilitacoes_evasoes](
	[data_ref] [date] NOT NULL,
	[cod_xp] [int] NOT NULL,
	[crm_id] [varchar](20) NOT NULL,
	[id_estrutura] [int] NOT NULL,
	[faixa_pl] [varchar](50) NULL,
	[tipo_movimentacao] [varchar](50) NULL
) ON [PRIMARY]
GO

-- ==============================================================================
-- 5. DOCUMENTAÇÃO DAS COLUNAS (EXTENDED PROPERTIES)
-- ==============================================================================

-- Documentação da coluna data_ref
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'data de referência do registro', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes', 
    @level2type=N'COLUMN',@level2name=N'data_ref'
GO

-- Documentação da coluna cod_xp
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'código do cliente no sistema xp', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes', 
    @level2type=N'COLUMN',@level2name=N'cod_xp'
GO

-- Documentação da coluna crm_id
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'identificador do assessor no crm', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes', 
    @level2type=N'COLUMN',@level2name=N'crm_id'
GO

-- Documentação da coluna id_estrutura
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'identificador da estrutura no sistema', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes', 
    @level2type=N'COLUMN',@level2name=N'id_estrutura'
GO

-- Documentação da coluna faixa_pl
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'faixa de patrimônio líquido do cliente', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes', 
    @level2type=N'COLUMN',@level2name=N'faixa_pl'
GO

-- Documentação da coluna tipo_movimentacao
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'tipo de movimentação do registro (ativação, habilitação ou evasão)', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes', 
    @level2type=N'COLUMN',@level2name=N'tipo_movimentacao'
GO

-- Documentação da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'tabela fatos que contém os registros de ativação, habilitação e evasão de clientes', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes'
GO

-- ==============================================================================
-- 6. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-20 | [Nome]         | Criação inicial da tabela

*/

-- ==============================================================================
-- 7. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Tabela não possui chave primária definida - avaliar necessidade
- Considerar criação de índices para otimizar consultas frequentes
- Faixa_pl e tipo_movimentacao são nullable - validar regra de negócio
- Relaciona-se com dim_pessoas através de crm_id
- Relaciona-se com dim_estruturas através de id_estrutura

Recomendações:
1. Adicionar constraint de chave primária composta (data_ref, cod_xp, tipo_movimentacao)
2. Criar índice não clusterizado em crm_id para joins com dim_pessoas
3. Criar índice não clusterizado em id_estrutura para joins com dim_estruturas
4. Adicionar constraint CHECK para tipo_movimentacao IN ('ativacao', 'habilitacao', 'evasao')

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
