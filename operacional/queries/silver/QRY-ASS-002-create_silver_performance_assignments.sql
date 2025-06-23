-- ==============================================================================
-- QRY-ASS-002-create_silver_performance_assignments
-- ==============================================================================
-- Tipo: DDL
-- Versão: 1.0.0
-- Última atualização: 2025-06-23
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [ddl, silver, fato, assignments, performance]
-- Status: desenvolvimento
-- Banco de Dados: SQL Server 2019
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela fato de assignments (atribuições) na camada Silver.
Armazena as atribuições de indicadores por pessoa com pesos e vigência.

Origem dos dados: bronze.performance_assignments
Volume esperado: 500-1000 registros por trimestre
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
IF OBJECT_ID('silver.fact_performance_assignments', 'U') IS NOT NULL
    DROP TABLE silver.fact_performance_assignments;
GO

CREATE TABLE silver.fact_performance_assignments (
    -- Chaves
    assignment_sk INT IDENTITY(1,1) NOT NULL,
    indicator_sk INT NOT NULL,
    crm_id VARCHAR(20) NOT NULL,
    data_ref DATE NOT NULL,
    id_estrutura INT NOT NULL,
    
    -- Atributos
    peso DECIMAL(5,2) NOT NULL,
    trimestre NVARCHAR(10) NOT NULL,
    ano INT NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT '9999-12-31',
    is_current BIT NOT NULL DEFAULT 1,
    
    -- Auditoria
    created_date DATETIME NOT NULL DEFAULT GETDATE(),
    created_by NVARCHAR(100) NULL,
    
    -- Constraints
    CONSTRAINT PK_fact_assignments PRIMARY KEY CLUSTERED (assignment_sk),
    CONSTRAINT FK_assignments_indicator FOREIGN KEY (indicator_sk) REFERENCES silver.dim_indicators(indicator_sk),
    CONSTRAINT FK_assignments_pessoa FOREIGN KEY (crm_id) REFERENCES silver.dim_pessoas(crm_id),
    CONSTRAINT FK_assignments_calendario FOREIGN KEY (data_ref) REFERENCES silver.dim_calendario(data_ref),
    CONSTRAINT FK_assignments_estrutura FOREIGN KEY (id_estrutura) REFERENCES silver.dim_estruturas(id_estrutura),
    CONSTRAINT UK_pessoa_indicator_trimestre UNIQUE (crm_id, indicator_sk, trimestre),
    CONSTRAINT CHK_peso_valido CHECK (peso >= 0 AND peso <= 100),
    CONSTRAINT CHK_datas_validas CHECK (valid_from <= valid_to),
    CONSTRAINT CHK_trimestre_formato CHECK (trimestre LIKE '[0-9][0-9][0-9][0-9]-Q[1-4]')
);
GO

-- ==============================================================================
-- 4. ÍNDICES
-- ==============================================================================

CREATE NONCLUSTERED INDEX IX_fact_assignments_pessoa_periodo
ON silver.fact_performance_assignments (crm_id, trimestre, is_current)
INCLUDE (indicator_sk, peso);

CREATE NONCLUSTERED INDEX IX_fact_assignments_indicator
ON silver.fact_performance_assignments (indicator_sk, is_current)
INCLUDE (crm_id, peso);

CREATE NONCLUSTERED INDEX IX_fact_assignments_current
ON silver.fact_performance_assignments (is_current, valid_from, valid_to)
INCLUDE (crm_id, indicator_sk);

CREATE NONCLUSTERED INDEX IX_fact_assignments_estrutura
ON silver.fact_performance_assignments (id_estrutura, trimestre)
INCLUDE (crm_id, indicator_sk, peso);
GO

-- ==============================================================================
-- 5. DOCUMENTAÇÃO - TABELA
-- ==============================================================================

EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Tabela fato que registra atribuições de indicadores para cada pessoa, incluindo pesos e vigência trimestral',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments';
GO

-- ==============================================================================
-- 6. DOCUMENTAÇÃO - COLUNAS
-- ==============================================================================

-- assignment_sk
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Chave surrogate auto-incremento',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'COLUMN', @level2name = N'assignment_sk';

-- indicator_sk
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_indicators',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'COLUMN', @level2name = N'indicator_sk';

-- crm_id
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_pessoas - código do assessor no CRM',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'COLUMN', @level2name = N'crm_id';

-- data_ref
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_calendario - primeiro dia do trimestre',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'COLUMN', @level2name = N'data_ref';

-- id_estrutura
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_estruturas - estrutura organizacional',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'COLUMN', @level2name = N'id_estrutura';

-- peso
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Peso do indicador (0-100%). CARD deve somar 100% por pessoa/trimestre',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'COLUMN', @level2name = N'peso';

-- trimestre
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Trimestre de vigência no formato YYYY-QN',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'COLUMN', @level2name = N'trimestre';

-- ano
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Ano de vigência (redundante para performance)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'COLUMN', @level2name = N'ano';

-- valid_from
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Data início da vigência',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'COLUMN', @level2name = N'valid_from';

-- valid_to
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Data fim da vigência (9999-12-31 = vigente)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'COLUMN', @level2name = N'valid_to';

-- is_current
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'1 = registro vigente, 0 = histórico',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'COLUMN', @level2name = N'is_current';

-- created_date
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Data de criação do registro',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'COLUMN', @level2name = N'created_date';

-- created_by
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Usuário/processo que criou o registro',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'COLUMN', @level2name = N'created_by';
GO

-- ==============================================================================
-- 7. DOCUMENTAÇÃO - CONSTRAINTS
-- ==============================================================================

-- PK_fact_assignments
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Chave primária da tabela fact_performance_assignments',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'CONSTRAINT', @level2name = N'PK_fact_assignments';

-- FK_assignments_indicator
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_indicators',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'CONSTRAINT', @level2name = N'FK_assignments_indicator';

-- FK_assignments_pessoa
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_pessoas',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'CONSTRAINT', @level2name = N'FK_assignments_pessoa';

-- FK_assignments_calendario
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_calendario',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'CONSTRAINT', @level2name = N'FK_assignments_calendario';

-- FK_assignments_estrutura
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'FK para dim_estruturas',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'CONSTRAINT', @level2name = N'FK_assignments_estrutura';

-- UK_pessoa_indicator_trimestre
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Garante apenas uma atribuição por pessoa/indicador/trimestre',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'CONSTRAINT', @level2name = N'UK_pessoa_indicator_trimestre';

-- CHK_peso_valido
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Valida peso entre 0 e 100',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'CONSTRAINT', @level2name = N'CHK_peso_valido';

-- CHK_datas_validas
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Valida que valid_from <= valid_to',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'CONSTRAINT', @level2name = N'CHK_datas_validas';

-- CHK_trimestre_formato
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Valida formato do trimestre (YYYY-QN)',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'CONSTRAINT', @level2name = N'CHK_trimestre_formato';
GO

-- ==============================================================================
-- 8. DOCUMENTAÇÃO - ÍNDICES
-- ==============================================================================

-- IX_fact_assignments_pessoa_periodo
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Índice para queries por pessoa e período',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'INDEX', @level2name = N'IX_fact_assignments_pessoa_periodo';

-- IX_fact_assignments_indicator
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Índice para análise por indicador',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'INDEX', @level2name = N'IX_fact_assignments_indicator';

-- IX_fact_assignments_current
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Índice para registros vigentes',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'INDEX', @level2name = N'IX_fact_assignments_current';

-- IX_fact_assignments_estrutura
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Índice para análise por estrutura organizacional',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'fact_performance_assignments',
    @level2type = N'INDEX', @level2name = N'IX_fact_assignments_estrutura';
GO

-- ==============================================================================
-- VALIDAÇÃO
-- ==============================================================================

IF OBJECT_ID('silver.fact_performance_assignments', 'U') IS NOT NULL
    PRINT 'Tabela silver.fact_performance_assignments criada com sucesso';
ELSE
    RAISERROR('ERRO: Tabela não foi criada', 16, 1);