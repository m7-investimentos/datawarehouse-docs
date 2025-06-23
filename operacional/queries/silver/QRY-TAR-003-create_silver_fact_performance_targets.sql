-- ==============================================================================
-- QRY-TAR-003-create_silver_fact_performance_targets
-- ==============================================================================
-- Tipo: DDL
-- Versão: 1.0.0
-- Última atualização: 2025-06-23
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [ddl, silver, fato, targets, metas, performance]
-- Status: desenvolvimento
-- Banco de Dados: SQL Server 2019
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela fato de targets (metas) na camada Silver.
Armazena metas mensais por pessoa/indicador com valores mínimo, target e superação.

Origem dos dados: bronze.performance_targets
Volume esperado: 5000-10000 registros por mês
*/

-- ==============================================================================
-- 2. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas necessárias:
- silver.dim_indicators (FK)
- silver.dim_pessoas (FK)
- silver.dim_calendario (FK)
- silver.dim_estruturas (FK)
*/

-- ==============================================================================
-- 3. CRIAÇÃO DA TABELA
-- ==============================================================================

-- Drop se existir
IF OBJECT_ID('silver.fact_performance_targets', 'U') IS NOT NULL
    DROP TABLE silver.fact_performance_targets;
GO

CREATE TABLE silver.fact_performance_targets (
    -- Chaves
    target_sk INT IDENTITY(1,1) NOT NULL,
    indicator_sk INT NOT NULL,
    crm_id VARCHAR(20) NOT NULL,
    data_ref DATE NOT NULL,
    id_estrutura INT NOT NULL,
    
    -- Valores
    valor_meta DECIMAL(18,4) NOT NULL,
    valor_minimo DECIMAL(18,4) NULL,
    valor_superacao DECIMAL(18,4) NULL,
    
    -- Período
    mes INT NOT NULL,
    ano INT NOT NULL,
    periodo_competencia NVARCHAR(7) NOT NULL,
    
    -- Workflow
    is_approved BIT NOT NULL DEFAULT 0,
    approved_by NVARCHAR(100) NULL,
    approved_date DATETIME NULL,
    
    -- Auditoria
    created_date DATETIME NOT NULL DEFAULT GETDATE(),
    created_by NVARCHAR(100) NULL,
    modified_date DATETIME NULL,
    
    -- Constraints
    CONSTRAINT PK_fact_targets PRIMARY KEY CLUSTERED (target_sk),
    CONSTRAINT FK_targets_indicator FOREIGN KEY (indicator_sk) REFERENCES silver.dim_indicators(indicator_sk),
    CONSTRAINT FK_targets_pessoa FOREIGN KEY (crm_id) REFERENCES silver.dim_pessoas(crm_id),
    CONSTRAINT FK_targets_calendario FOREIGN KEY (data_ref) REFERENCES silver.dim_calendario(data_ref),
    CONSTRAINT FK_targets_estrutura FOREIGN KEY (id_estrutura) REFERENCES silver.dim_estruturas(id_estrutura),
    CONSTRAINT UK_pessoa_indicator_periodo UNIQUE (crm_id, indicator_sk, periodo_competencia),
    CONSTRAINT CHK_valores_meta_coerentes CHECK ((valor_minimo IS NULL OR valor_minimo <= valor_meta) AND (valor_superacao IS NULL OR valor_superacao >= valor_meta)),
    CONSTRAINT CHK_mes_valido CHECK (mes BETWEEN 1 AND 12),
    CONSTRAINT CHK_periodo_formato CHECK (periodo_competencia LIKE '[0-9][0-9][0-9][0-9]-[0-9][0-9]')
);
GO

-- ==============================================================================
-- 4. ÍNDICES
-- ==============================================================================

CREATE NONCLUSTERED INDEX IX_fact_targets_periodo
ON silver.fact_performance_targets (periodo_competencia, is_approved)
INCLUDE (crm_id, indicator_sk, valor_meta);

CREATE NONCLUSTERED INDEX IX_fact_targets_pessoa
ON silver.fact_performance_targets (crm_id, ano, mes)
INCLUDE (indicator_sk, valor_meta);

CREATE NONCLUSTERED INDEX IX_fact_targets_indicator
ON silver.fact_performance_targets (indicator_sk, periodo_competencia)
INCLUDE (crm_id, valor_meta);

CREATE NONCLUSTERED INDEX IX_fact_targets_estrutura
ON silver.fact_performance_targets (id_estrutura, periodo_competencia)
INCLUDE (crm_id, indicator_sk, valor_meta);

CREATE NONCLUSTERED INDEX IX_fact_targets_pending_approval
ON silver.fact_performance_targets (periodo_competencia, crm_id)
WHERE is_approved = 0;
GO

-- ==============================================================================
-- 5. DOCUMENTAÇÃO - TABELA
-- ==============================================================================

EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Tabela fato que armazena metas mensais definidas para cada combinação pessoa/indicador',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets';
GO

-- ==============================================================================
-- 6. DOCUMENTAÇÃO - COLUNAS
-- ==============================================================================

-- target_sk
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Chave surrogate auto-incremento',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'target_sk';

-- indicator_sk
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_indicators',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'indicator_sk';

-- crm_id
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_pessoas - código do assessor no CRM',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'crm_id';

-- data_ref
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_calendario - primeiro dia do mês',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'data_ref';

-- id_estrutura
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_estruturas - estrutura organizacional',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'id_estrutura';

-- valor_meta
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Valor alvo principal da meta',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'valor_meta';

-- valor_minimo
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Valor mínimo aceitável (abaixo = não atingido)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'valor_minimo';

-- valor_superacao
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Valor de superação (acima = superado)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'valor_superacao';

-- mes
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Mês da meta (1-12)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'mes';

-- ano
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Ano da meta',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'ano';

-- periodo_competencia
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Período no formato YYYY-MM',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'periodo_competencia';

-- is_approved
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'1 = meta aprovada, 0 = pendente aprovação',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'is_approved';

-- approved_by
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Usuário que aprovou a meta',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'approved_by';

-- approved_date
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Data/hora da aprovação',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'approved_date';

-- created_date
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Data de criação do registro',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'created_date';

-- created_by
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Usuário/processo que criou o registro',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'created_by';

-- modified_date
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Data da última modificação',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'COLUMN', @level2name = N'modified_date';
GO

-- ==============================================================================
-- 7. DOCUMENTAÇÃO - CONSTRAINTS
-- ==============================================================================

-- PK_fact_targets
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Chave primária da tabela fact_performance_targets',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'CONSTRAINT', @level2name = N'PK_fact_targets';

-- FK_targets_indicator
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_indicators',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'CONSTRAINT', @level2name = N'FK_targets_indicator';

-- FK_targets_pessoa
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_pessoas',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'CONSTRAINT', @level2name = N'FK_targets_pessoa';

-- FK_targets_calendario
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_calendario',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'CONSTRAINT', @level2name = N'FK_targets_calendario';

-- FK_targets_estrutura
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_estruturas',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'CONSTRAINT', @level2name = N'FK_targets_estrutura';

-- UK_pessoa_indicator_periodo
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Garante apenas uma meta por pessoa/indicador/período',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'CONSTRAINT', @level2name = N'UK_pessoa_indicator_periodo';

-- CHK_valores_meta_coerentes
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Valida que minimo <= meta <= superacao',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'CONSTRAINT', @level2name = N'CHK_valores_meta_coerentes';

-- CHK_mes_valido
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Valida mês entre 1 e 12',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'CONSTRAINT', @level2name = N'CHK_mes_valido';

-- CHK_periodo_formato
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Valida formato do período (YYYY-MM)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'CONSTRAINT', @level2name = N'CHK_periodo_formato';
GO

-- ==============================================================================
-- 8. DOCUMENTAÇÃO - ÍNDICES
-- ==============================================================================

-- IX_fact_targets_periodo
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Índice para queries por período',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'INDEX', @level2name = N'IX_fact_targets_periodo';

-- IX_fact_targets_pessoa
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Índice para metas por pessoa',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'INDEX', @level2name = N'IX_fact_targets_pessoa';

-- IX_fact_targets_indicator
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Índice para análise por indicador',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'INDEX', @level2name = N'IX_fact_targets_indicator';

-- IX_fact_targets_estrutura
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Índice para análise por estrutura',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'INDEX', @level2name = N'IX_fact_targets_estrutura';

-- IX_fact_targets_pending_approval
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Índice filtrado para metas pendentes de aprovação',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_targets',
    @level2type = N'INDEX', @level2name = N'IX_fact_targets_pending_approval';
GO

-- ==============================================================================
-- VALIDAÇÃO
-- ==============================================================================

IF OBJECT_ID('silver.fact_performance_targets', 'U') IS NOT NULL
    PRINT 'Tabela silver.fact_performance_targets criada com sucesso';
ELSE
    RAISERROR('ERRO: Tabela não foi criada', 16, 1);