-- ==============================================================================
-- QRY-ASS-002-create_silver_performance_assignments
-- ==============================================================================
-- Tipo: DDL - Criação de Tabela
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [silver, ddl, performance, assignments, configuração]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: 
    Cria a tabela silver.performance_assignments para armazenar configurações 
    validadas de atribuições de indicadores de performance por assessor, com 
    tipos de dados adequados, constraints e relacionamentos.

Casos de uso:
    - Configuração de indicadores e pesos por assessor
    - Base para cálculo de performance e remuneração variável
    - Controle de vigência e histórico de atribuições
    - Integração com sistema de metas e cálculos

Frequência de execução: Uma única vez (criação) ou em caso de rebuild
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: ~200-500 registros ativos, histórico ilimitado
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros necessários para execução:
    Nenhum - Script DDL de criação de tabela
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Estrutura da tabela criada:

| Coluna                | Tipo           | Descrição                                   |
|-----------------------|----------------|---------------------------------------------|
| assignment_id         | INT            | ID único da atribuição (PK)                 |
| codigo_assessor_crm   | VARCHAR(20)    | Código do assessor no CRM                  |
| indicator_id          | INT            | FK para silver.performance_indicators     |
| indicator_weight      | DECIMAL(5,2)   | Peso do indicador (0.00-100.00)            |
| valid_from            | DATE           | Data início vigência                        |
| valid_to              | DATE           | Data fim vigência (NULL = ativo)           |
| created_date          | DATETIME       | Data de criação                            |
| created_by            | VARCHAR(100)   | Usuário que criou                          |
| modified_date         | DATETIME       | Data última modificação                    |
| modified_by           | VARCHAR(100)   | Usuário que modificou                      |
| approved_date         | DATETIME       | Data de aprovação                          |
| approved_by           | VARCHAR(100)   | Usuário que aprovou                        |
| is_active             | BIT            | Flag se está ativo                         |
| comments              | NVARCHAR(1000) | Comentários                                |
| bronze_load_id        | INT            | Referência ao load_id do bronze            |
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
    - silver.performance_indicators: Tabela de indicadores (FK)
    - dim.pessoas: Dimensão de pessoas para validar assessores
    
Pré-requisitos:
    - Schema silver deve existir
    - Tabela silver.performance_indicators deve existir
    - Permissão CREATE TABLE no schema silver
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- Configurações específicas do banco para otimização
USE [M7Medallion];
GO

-- ==============================================================================
-- 6. DDL PRINCIPAL
-- ==============================================================================

-- Drop tabela existente se necessário
IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'[silver].[performance_assignments]'))
BEGIN
    -- Drop constraints primeiro
    IF EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[silver].[FK_performance_assignments_indicators]'))
        ALTER TABLE [silver].[performance_assignments] DROP CONSTRAINT [FK_performance_assignments_indicators];
    
    DROP TABLE [silver].[performance_assignments];
    PRINT 'Tabela [silver].[performance_assignments] removida.';
END
GO

-- Criar tabela
CREATE TABLE [silver].[performance_assignments] (
    -- Chave primária
    [assignment_id] INT IDENTITY(1,1) NOT NULL,
    
    -- Dados principais
    [codigo_assessor_crm] VARCHAR(20) NOT NULL,
    [indicator_id] INT NOT NULL,
    [indicator_weight] DECIMAL(5,2) NOT NULL DEFAULT 0.00,
    
    -- Vigência
    [valid_from] DATE NOT NULL,
    [valid_to] DATE NULL,
    
    -- Auditoria
    [created_date] DATETIME NOT NULL DEFAULT GETDATE(),
    [created_by] VARCHAR(100) NOT NULL,
    [modified_date] DATETIME NULL,
    [modified_by] VARCHAR(100) NULL,
    [approved_date] DATETIME NULL,
    [approved_by] VARCHAR(100) NULL,
    
    -- Controle
    [is_active] BIT NOT NULL DEFAULT 1,
    [comments] NVARCHAR(1000) NULL,
    [bronze_load_id] INT NULL,
    
    -- Constraints
    CONSTRAINT [PK_silver_performance_assignments] PRIMARY KEY CLUSTERED ([assignment_id] ASC),
    CONSTRAINT [CK_performance_assignments_weight] CHECK ([indicator_weight] >= 0.00 AND [indicator_weight] <= 100.00),
    CONSTRAINT [CK_performance_assignments_dates] CHECK ([valid_to] IS NULL OR [valid_to] > [valid_from])
) ON [PRIMARY];
GO

-- ==============================================================================
-- 7. FOREIGN KEYS E CONSTRAINTS
-- ==============================================================================

-- FK para performance_indicators
ALTER TABLE [silver].[performance_assignments]
ADD CONSTRAINT [FK_performance_assignments_indicators]
FOREIGN KEY ([indicator_id])
REFERENCES [silver].[performance_indicators] ([indicator_id]);
GO

-- Unique constraint para evitar duplicatas ativas
CREATE UNIQUE NONCLUSTERED INDEX [UQ_performance_assignments_active]
ON [silver].[performance_assignments] (
    [codigo_assessor_crm] ASC,
    [indicator_id] ASC,
    [valid_from] ASC
)
WHERE [valid_to] IS NULL;
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por assessor
CREATE NONCLUSTERED INDEX [IX_performance_assignments_assessor]
ON [silver].[performance_assignments] (
    [codigo_assessor_crm] ASC,
    [valid_from] ASC,
    [valid_to] ASC
)
INCLUDE ([indicator_id], [indicator_weight])
WHERE [is_active] = 1;
GO

-- Índice para busca por indicador
CREATE NONCLUSTERED INDEX [IX_performance_assignments_indicator]
ON [silver].[performance_assignments] (
    [indicator_id] ASC,
    [is_active] ASC
)
INCLUDE ([codigo_assessor_crm], [indicator_weight], [valid_from], [valid_to]);
GO

-- Índice para vigência atual
CREATE NONCLUSTERED INDEX [IX_performance_assignments_current]
ON [silver].[performance_assignments] (
    [valid_from] ASC,
    [valid_to] ASC
)
INCLUDE ([codigo_assessor_crm], [indicator_id], [indicator_weight])
WHERE [is_active] = 1 AND [valid_to] IS NULL;
GO

-- ==============================================================================
-- 9. VIEWS AUXILIARES
-- ==============================================================================

-- View para atribuições vigentes
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[silver].[vw_performance_assignments_current]'))
    DROP VIEW [silver].[vw_performance_assignments_current];
GO

CREATE VIEW [silver].[vw_performance_assignments_current]
AS
WITH AssignmentsSummary AS (
    SELECT 
        a.codigo_assessor_crm,
        i.category,
        SUM(a.indicator_weight) as total_weight,
        COUNT(*) as indicator_count,
        STRING_AGG(CAST(i.indicator_code AS NVARCHAR(MAX)), ', ') 
            WITHIN GROUP (ORDER BY a.indicator_weight DESC) as indicators
    FROM silver.performance_assignments a
    INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
    WHERE a.is_active = 1
      AND a.valid_to IS NULL
      AND GETDATE() >= a.valid_from
    GROUP BY a.codigo_assessor_crm, i.category
)
SELECT 
    codigo_assessor_crm,
    category,
    total_weight,
    indicator_count,
    indicators,
    CASE 
        WHEN ABS(total_weight - 100.00) < 0.01 THEN 'VÁLIDO'
        ELSE 'INVÁLIDO - Soma: ' + CAST(total_weight AS VARCHAR(10))
    END as weight_validation
FROM AssignmentsSummary;
GO

-- ==============================================================================
-- 10. DOCUMENTAÇÃO E COMENTÁRIOS
-- ==============================================================================

-- Adicionar descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela de metadados para configuração de atribuições de indicadores de performance por assessor', 
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'performance_assignments';
GO

-- Adicionar descrições das colunas principais
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Identificador único da atribuição', 
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'assignment_id';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor (formato AAI###)', 
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'codigo_assessor_crm';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID do indicador de performance (FK)', 
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'indicator_id';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Peso do indicador (0.00-100.00). Para tipo CARD, soma deve ser 100.00 por assessor', 
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'indicator_weight';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de início da vigência da atribuição', 
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'valid_from';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de fim da vigência (NULL = vigente)', 
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'valid_to';
GO

-- ==============================================================================
-- 11. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================

/*
-- Query para verificar soma de pesos por assessor
SELECT 
    a.codigo_assessor_crm,
    i.indicator_type,
    COUNT(*) as qtd_indicadores,
    SUM(a.indicator_weight) as soma_pesos,
    CASE 
        WHEN i.indicator_type = 'CARD' AND ABS(SUM(a.indicator_weight) - 100.00) < 0.01 THEN 'OK'
        WHEN i.indicator_type = 'CARD' THEN 'ERRO'
        ELSE 'N/A'
    END as validacao
FROM silver.performance_assignments a
INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
WHERE a.is_active = 1 AND a.valid_to IS NULL
GROUP BY a.codigo_assessor_crm, i.indicator_type
ORDER BY a.codigo_assessor_crm, i.indicator_type;

-- Query para verificar histórico de mudanças
SELECT 
    codigo_assessor_crm,
    valid_from,
    valid_to,
    COUNT(*) as qtd_indicadores,
    created_date,
    created_by,
    approved_date,
    approved_by
FROM silver.performance_assignments
WHERE codigo_assessor_crm = 'AAI001'  -- Exemplo
ORDER BY valid_from DESC;
*/

-- ==============================================================================
-- 12. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                | Descrição
--------|------------|----------------------|--------------------------------------------
1.0.0   | 2025-01-17 | bruno.chiaramonti   | Criação inicial da tabela

*/

-- ==============================================================================
-- 13. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
    - indicator_weight usa DECIMAL(5,2) para precisão em cálculos financeiros
    - Constraint única previne duplicatas de atribuições ativas
    - View auxiliar facilita validação de somas de pesos
    - Índices otimizados para queries de vigência e cálculo
    - Histórico completo mantido com valid_to

Troubleshooting comum:
    1. Erro "duplicate key": Verificar se já existe atribuição ativa
    2. Erro FK: Verificar se indicator_id existe em performance_indicators
    3. Soma pesos ≠ 100: Usar view vw_performance_assignments_current

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Verificar criação da tabela
SELECT 
    t.name AS TableName,
    s.name AS SchemaName,
    t.create_date,
    t.modify_date,
    (SELECT COUNT(*) FROM sys.indexes WHERE object_id = t.object_id) as IndexCount,
    (SELECT COUNT(*) FROM sys.foreign_keys WHERE parent_object_id = t.object_id) as FKCount
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.name = 'performance_assignments' AND s.name = 'silver';
GO