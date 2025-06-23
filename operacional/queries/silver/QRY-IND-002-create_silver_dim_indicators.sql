-- ==============================================================================
-- QRY-IND-002-create_silver_dim_indicators
-- ==============================================================================
-- Tipo: DDL
-- Versão: 1.0.0
-- Última atualização: 2025-06-23
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [ddl, silver, dimensão, indicadores, performance]
-- Status: desenvolvimento
-- Banco de Dados: SQL Server 2019
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da dimensão de indicadores na camada Silver do data warehouse.
Armazena metadados de todos os indicadores de performance (KPIs).

Origem dos dados: bronze.performance_indicators
Volume esperado: 50-100 registros
*/

-- ==============================================================================
-- 2. DEPENDÊNCIAS
-- ==============================================================================
/*
Schemas necessários:
- silver (deve existir)

Tabelas que referenciarão esta:
- silver.fact_performance_assignments
- silver.fact_performance_targets
*/

-- ==============================================================================
-- 3. CRIAÇÃO DA TABELA
-- ==============================================================================

-- Drop se existir
IF OBJECT_ID('silver.dim_indicators', 'U') IS NOT NULL
    DROP TABLE silver.dim_indicators;
GO

CREATE TABLE silver.dim_indicators (
    -- Chaves
    indicator_sk INT IDENTITY(1,1) NOT NULL,
    indicator_id NVARCHAR(50) NOT NULL,
    
    -- Atributos
    nome NVARCHAR(255) NOT NULL,
    tipo NVARCHAR(50) NOT NULL,
    grupo NVARCHAR(100) NULL,
    unidade_medida NVARCHAR(50) NULL,
    formula_calculo NVARCHAR(MAX) NOT NULL,
    aggregation_method NVARCHAR(50) NOT NULL DEFAULT 'SUM',
    tabela_origem NVARCHAR(255) NOT NULL,
    is_inverted BIT NOT NULL DEFAULT 0,
    is_active BIT NOT NULL DEFAULT 1,
    
    -- Auditoria
    created_date DATETIME NOT NULL DEFAULT GETDATE(),
    modified_date DATETIME NOT NULL DEFAULT GETDATE(),
    
    -- Constraints
    CONSTRAINT PK_dim_indicators PRIMARY KEY CLUSTERED (indicator_sk),
    CONSTRAINT UK_dim_indicators_id UNIQUE NONCLUSTERED (indicator_id),
    CONSTRAINT CHK_indicator_tipo CHECK (tipo IN ('CARD', 'GATILHO', 'KPI', 'PPI')),
    CONSTRAINT CHK_aggregation_method CHECK (aggregation_method IN ('SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'CUSTOM'))
);
GO

-- ==============================================================================
-- 4. ÍNDICES
-- ==============================================================================

CREATE NONCLUSTERED INDEX IX_dim_indicators_tipo
ON silver.dim_indicators (tipo, is_active)
INCLUDE (indicator_id, nome);
GO

-- ==============================================================================
-- 5. DOCUMENTAÇÃO - TABELA
-- ==============================================================================

EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Dimensão de indicadores de performance. Cada indicador tem uma fórmula SQL executável.',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators';
GO

-- ==============================================================================
-- 6. DOCUMENTAÇÃO - COLUNAS
-- ==============================================================================

-- indicator_sk
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Chave surrogate auto-incremento',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'COLUMN', @level2name = N'indicator_sk';

-- indicator_id
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Código único do indicador (ex: CAPT_LIQ, NPS_NOTA)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'COLUMN', @level2name = N'indicator_id';

-- nome
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Nome descritivo do indicador',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'COLUMN', @level2name = N'nome';

-- tipo
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Tipo: CARD (pondera score), GATILHO (pré-requisito), KPI (monitoramento), PPI (processo)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'COLUMN', @level2name = N'tipo';

-- grupo
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Categoria do indicador (CAPTACAO, QUALIDADE, FINANCEIRO, etc.)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'COLUMN', @level2name = N'grupo';

-- unidade_medida
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Unidade de medida (R$, %, Qtd, Score, etc.)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'COLUMN', @level2name = N'unidade_medida';

-- formula_calculo
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Fórmula SQL executável com placeholders @entity_id e @period',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'COLUMN', @level2name = N'formula_calculo';

-- aggregation_method
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Método de agregação (SUM, AVG, COUNT, MIN, MAX, CUSTOM)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'COLUMN', @level2name = N'aggregation_method';

-- tabela_origem
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Tabela principal de origem dos dados',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'COLUMN', @level2name = N'tabela_origem';

-- is_inverted
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'1 = menor é melhor (ex: taxa de churn)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'COLUMN', @level2name = N'is_inverted';

-- is_active
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'1 = ativo, 0 = inativo (soft delete)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'COLUMN', @level2name = N'is_active';

-- created_date
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Data de criação do registro',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'COLUMN', @level2name = N'created_date';

-- modified_date
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Data da última modificação',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'COLUMN', @level2name = N'modified_date';
GO

-- ==============================================================================
-- 7. DOCUMENTAÇÃO - CONSTRAINTS
-- ==============================================================================

-- PK_dim_indicators
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Chave primária da tabela dim_indicators',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'CONSTRAINT', @level2name = N'PK_dim_indicators';

-- UK_dim_indicators_id
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Garante unicidade do código do indicador',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'CONSTRAINT', @level2name = N'UK_dim_indicators_id';

-- CHK_indicator_tipo
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Valida tipos permitidos: CARD, GATILHO, KPI, PPI',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'CONSTRAINT', @level2name = N'CHK_indicator_tipo';

-- CHK_aggregation_method
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Valida métodos de agregação: SUM, AVG, COUNT, MIN, MAX, CUSTOM',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'CONSTRAINT', @level2name = N'CHK_aggregation_method';
GO

-- ==============================================================================
-- 8. DOCUMENTAÇÃO - ÍNDICES
-- ==============================================================================

-- IX_dim_indicators_tipo
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Índice para otimizar queries por tipo e status ativo',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'dim_indicators',
    @level2type = N'INDEX', @level2name = N'IX_dim_indicators_tipo';
GO

-- ==============================================================================
-- VALIDAÇÃO
-- ==============================================================================

IF OBJECT_ID('silver.dim_indicators', 'U') IS NOT NULL
    PRINT 'Tabela silver.dim_indicators criada com sucesso';
ELSE
    RAISERROR('ERRO: Tabela não foi criada', 16, 1);