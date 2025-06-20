-- ==============================================================================
-- QRY-EST-001-CREATE_SILVER_DIM_ESTRUTURAS
-- ==============================================================================
-- Tipo: DDL - Create Table
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [ddl, silver, dimension_table, estruturas, hierarquia]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela dimensão dim_estruturas no schema silver.
           Armazena a hierarquia organizacional da empresa, permitindo
           representar estruturas em múltiplos níveis (árvore hierárquica)
           através de auto-relacionamento.

Casos de uso:
- Definição da estrutura organizacional da empresa
- Análise hierárquica de equipes e departamentos
- Identificação de líderes de cada estrutura
- Base para relatórios organizacionais
- Suporte a análises de performance por estrutura

Frequência de execução: Única (criação da tabela)
Volume esperado de linhas: ~100 registros
*/

-- ==============================================================================
-- 2. ESTRUTURA DA TABELA
-- ==============================================================================
/*
Colunas da tabela:

| Coluna         | Tipo         | Nullable | Descrição                                              |
|----------------|--------------|----------|--------------------------------------------------------|
| id_estrutura   | INT          | NOT NULL | Identificador único da estrutura (PK)                  |
| nome_estrutura | VARCHAR(100) | NOT NULL | Nome da estrutura organizacional                       |
| estrutura_pai  | INT          | NULL     | ID da estrutura pai (auto-relacionamento)              |
| observacoes    | VARCHAR(200) | NULL     | Observações adicionais sobre a estrutura               |
| crm_id_lider   | VARCHAR(20)  | NULL     | ID do líder da estrutura no CRM                        |

Chave primária: id_estrutura
Auto-relacionamento: estrutura_pai -> id_estrutura
*/

-- ==============================================================================
-- 3. DEPENDÊNCIAS
-- ==============================================================================
/*
Schemas necessários:
- silver: Schema onde a tabela será criada

Tabelas relacionadas:
- Auto-relacionamento com esta própria tabela (estrutura_pai)
- [silver].[dim_pessoas]: Para validar crm_id_lider (não enforced)
- [silver].[fact_estrutura_pessoas]: Tabela fato que usa esta dimensão

Processo de carga:
- Carga manual ou via sistema de RH/CRM
- Atualizações quando há mudanças organizacionais
- Deve respeitar hierarquia (pai deve existir antes do filho)

Pré-requisitos:
- Schema silver deve existir
- Usuário deve ter permissão CREATE TABLE no schema silver
- Dados devem ser carregados respeitando hierarquia
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
CREATE TABLE [silver].[dim_estruturas](
	[id_estrutura] [int] NOT NULL,
	[nome_estrutura] [varchar](100) NOT NULL,
	[estrutura_pai] [int] NULL,
	[observacoes] [varchar](200) NULL,
	[crm_id_lider] [varchar](20) NULL,
 CONSTRAINT [PK_dim_estruturas] PRIMARY KEY CLUSTERED 
(
	[id_estrutura] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- ==============================================================================
-- 5. DOCUMENTAÇÃO DAS COLUNAS (EXTENDED PROPERTIES)
-- ==============================================================================

-- Documentação da coluna id_estrutura
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'identificador único da estrutura organizacional. gerado manualmente ou importado do sistema de origem' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_estruturas', 
    @level2type=N'COLUMN',@level2name=N'id_estrutura'
GO

-- Documentação da coluna nome_estrutura
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'nome da estrutura organizacional. exemplos: Diretoria Comercial, Gerência Regional SP, Equipe AAI001' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_estruturas', 
    @level2type=N'COLUMN',@level2name=N'nome_estrutura'
GO

-- Documentação da coluna estrutura_pai
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'identificador da estrutura pai (auto-relacionamento). NULL para estrutura raiz. permite criar hierarquia em árvore' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_estruturas', 
    @level2type=N'COLUMN',@level2name=N'estrutura_pai'
GO

-- Documentação da coluna observacoes
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'observações adicionais sobre a estrutura organizacional. campo livre para anotações' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_estruturas', 
    @level2type=N'COLUMN',@level2name=N'observacoes'
GO

-- Documentação da coluna crm_id_lider
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'identificador do líder da estrutura no crm. relaciona com dim_pessoas.crm_id mas sem FK explícita' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_estruturas', 
    @level2type=N'COLUMN',@level2name=N'crm_id_lider'
GO

-- Documentação da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'tabela dimensão que contém as informações da estrutura organizacional da empresa. permite representar hierarquia em múltiplos níveis através de auto-relacionamento. cada estrutura pode ter um líder associado.' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_estruturas'
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
- Tabela suporta hierarquia ilimitada de níveis
- estrutura_pai NULL indica estrutura raiz (topo da hierarquia)
- Não há constraint de FK para estrutura_pai (permite flexibilidade)
- crm_id_lider não tem FK explícita para dim_pessoas
- Cuidado com referências circulares na hierarquia

Exemplo de hierarquia:
- Diretoria (id=1, pai=NULL)
  - Gerência Regional SP (id=2, pai=1)
    - Equipe SP-01 (id=3, pai=2)
    - Equipe SP-02 (id=4, pai=2)
  - Gerência Regional RJ (id=5, pai=1)
    - Equipe RJ-01 (id=6, pai=5)

Queries úteis:
-- Estruturas raiz (topo da hierarquia)
SELECT * FROM silver.dim_estruturas WHERE estrutura_pai IS NULL;

-- Estruturas filhas de uma estrutura específica
SELECT * FROM silver.dim_estruturas WHERE estrutura_pai = 1;

-- Caminho hierárquico completo (CTE recursivo)
WITH hierarquia AS (
    SELECT id_estrutura, nome_estrutura, estrutura_pai, 0 as nivel
    FROM silver.dim_estruturas WHERE estrutura_pai IS NULL
    UNION ALL
    SELECT e.id_estrutura, e.nome_estrutura, e.estrutura_pai, h.nivel + 1
    FROM silver.dim_estruturas e
    INNER JOIN hierarquia h ON e.estrutura_pai = h.id_estrutura
)
SELECT * FROM hierarquia ORDER BY nivel, id_estrutura;

Recomendações:
1. Criar índice em estrutura_pai para otimizar consultas hierárquicas
2. Implementar constraint CHECK para evitar auto-referência
3. Criar view materializada para hierarquia completa se necessário
4. Implementar trigger para validar hierarquia antes de insert/update
5. Considerar adicionar campos de audit (data_criacao, usuario_criacao)

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
