-- ==============================================================================
-- QRY-IND-002-create_metadata_performance_indicators
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [metadata, performance, indicadores, dimensão, ddl]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: metadata
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela metadata.performance_indicators para armazenar a configuração
           validada e processada dos indicadores de performance. Esta é a fonte
           oficial para cálculos e relatórios.

Casos de uso:
- Armazenamento definitivo de indicadores validados
- Fonte para cálculo de performance de assessores
- Referência para dashboards e relatórios
- Configuração de metas e atribuições

Frequência de execução: Uma única vez (criação) ou em recriações
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 10-50 registros
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros necessários para execução:
Nenhum - Script DDL de criação de objeto
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela criada: metadata.performance_indicators

Colunas principais:
- indicator_id: Chave primária
- indicator_code: Código único do indicador
- indicator_name: Nome do indicador
- category: Categoria validada
- unit: Unidade de medida
- aggregation_method: Método de agregação
- calculation_formula: Fórmula SQL validada
- is_inverted: Flag booleano
- is_active: Flag booleano
- description: Descrição completa

Colunas de auditoria:
- created_date: Data de criação
- created_by: Usuário criador
- modified_date: Data de modificação
- modified_by: Usuário modificador
- version: Versão do registro
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Objetos necessários:
- Schema metadata deve existir

Permissões requeridas:
- CREATE TABLE no schema metadata
- Procedures de ETL devem ter INSERT/UPDATE na tabela
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- Tabela pequena, não requer particionamento
-- Usar compressão de página para economia de espaço

USE [M7Medallion];
GO

-- ==============================================================================
-- 6. CRIAÇÃO DA TABELA
-- ==============================================================================

-- Criar schema se não existir
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'metadata')
BEGIN
    EXEC('CREATE SCHEMA [metadata]');
    PRINT 'Schema metadata criado';
END
GO

-- Drop tabela se existir (cuidado em produção!)
IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'[metadata].[performance_indicators]'))
BEGIN
    DROP TABLE [metadata].[performance_indicators];
    PRINT 'Tabela metadata.performance_indicators removida';
END
GO

-- Criar tabela
CREATE TABLE [metadata].[performance_indicators] (
    -- Chave primária
    [indicator_id] INT IDENTITY(1,1) NOT NULL,
    
    -- Dados principais do indicador
    [indicator_code] VARCHAR(50) NOT NULL,
    [indicator_name] VARCHAR(200) NOT NULL,
    [category] VARCHAR(50) NOT NULL,
    [unit] VARCHAR(20) NOT NULL,
    [aggregation_method] VARCHAR(20) NOT NULL DEFAULT 'SUM',
    
    -- Configuração de cálculo
    [calculation_formula] VARCHAR(MAX) NULL,
    [is_inverted] BIT NOT NULL DEFAULT 0,
    [is_active] BIT NOT NULL DEFAULT 1,
    
    -- Informações adicionais
    [description] VARCHAR(2000) NULL,
    [business_rules] VARCHAR(MAX) NULL,
    [notes] VARCHAR(MAX) NULL,
    
    -- Metadados de versionamento
    [version] INT NOT NULL DEFAULT 1,
    [valid_from] DATETIME NOT NULL DEFAULT GETDATE(),
    [valid_to] DATETIME NULL,
    
    -- Auditoria
    [created_date] DATETIME NOT NULL DEFAULT GETDATE(),
    [created_by] VARCHAR(100) NOT NULL DEFAULT SYSTEM_USER,
    [modified_date] DATETIME NULL,
    [modified_by] VARCHAR(100) NULL,
    
    -- Constraints
    CONSTRAINT [PK_metadata_performance_indicators] PRIMARY KEY CLUSTERED ([indicator_id] ASC),
    CONSTRAINT [UQ_metadata_performance_indicators_code] UNIQUE NONCLUSTERED ([indicator_code] ASC),
    CONSTRAINT [CK_metadata_performance_indicators_category] CHECK (
        [category] IN ('FINANCEIRO', 'QUALIDADE', 'VOLUME', 'COMPORTAMENTAL', 'PROCESSO', 'GATILHO')
    ),
    CONSTRAINT [CK_metadata_performance_indicators_unit] CHECK (
        [unit] IN ('R$', '%', 'QTD', 'SCORE', 'HORAS', 'DIAS', 'RATIO')
    ),
    CONSTRAINT [CK_metadata_performance_indicators_aggregation] CHECK (
        [aggregation_method] IN ('SUM', 'AVG', 'COUNT', 'MAX', 'MIN', 'LAST', 'CUSTOM')
    )
) ON [PRIMARY]
WITH (DATA_COMPRESSION = PAGE);
GO

-- ==============================================================================
-- 7. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por categoria
CREATE NONCLUSTERED INDEX [IX_metadata_performance_indicators_category]
ON [metadata].[performance_indicators] ([category])
INCLUDE ([indicator_code], [indicator_name], [is_active])
WHERE [is_active] = 1;
GO

-- Índice para busca por status ativo
CREATE NONCLUSTERED INDEX [IX_metadata_performance_indicators_active]
ON [metadata].[performance_indicators] ([is_active])
INCLUDE ([indicator_code], [indicator_name], [category]);
GO

-- Índice para versionamento temporal
CREATE NONCLUSTERED INDEX [IX_metadata_performance_indicators_temporal]
ON [metadata].[performance_indicators] ([valid_from], [valid_to])
INCLUDE ([indicator_code], [version]);
GO

-- ==============================================================================
-- 8. DOCUMENTAÇÃO E PROPRIEDADES ESTENDIDAS
-- ==============================================================================

-- Documentação da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela de metadados contendo a configuração oficial e validada dos indicadores de performance. Fonte autoritativa para todos os cálculos de performance.',
    @level0type=N'SCHEMA', @level0name=N'metadata',
    @level1type=N'TABLE', @level1name=N'performance_indicators';
GO

-- Documentação das colunas principais
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Identificador único do indicador',
    @level0type=N'SCHEMA', @level0name=N'metadata',
    @level1type=N'TABLE', @level1name=N'performance_indicators',
    @level2type=N'COLUMN', @level2name=N'indicator_id';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código único do indicador (ex: CAPTACAO_LIQUIDA). Usado como referência em todo o sistema.',
    @level0type=N'SCHEMA', @level0name=N'metadata',
    @level1type=N'TABLE', @level1name=N'performance_indicators',
    @level2type=N'COLUMN', @level2name=N'indicator_code';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Categoria do indicador: FINANCEIRO (métricas monetárias), QUALIDADE (satisfação/qualidade), VOLUME (quantidades), COMPORTAMENTAL (ações/comportamentos), PROCESSO (métricas de processo), GATILHO (triggers/alertas)',
    @level0type=N'SCHEMA', @level0name=N'metadata',
    @level1type=N'TABLE', @level1name=N'performance_indicators',
    @level2type=N'COLUMN', @level2name=N'category';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Fórmula SQL validada para cálculo do indicador. Deve referenciar tabelas do DW.',
    @level0type=N'SCHEMA', @level0name=N'metadata',
    @level1type=N'TABLE', @level1name=N'performance_indicators',
    @level2type=N'COLUMN', @level2name=N'calculation_formula';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Indica se o indicador é invertido (1 = menor valor é melhor, 0 = maior valor é melhor)',
    @level0type=N'SCHEMA', @level0name=N'metadata',
    @level1type=N'TABLE', @level1name=N'performance_indicators',
    @level2type=N'COLUMN', @level2name=N'is_inverted';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Versão do registro para controle de histórico. Incrementa a cada mudança.',
    @level0type=N'SCHEMA', @level0name=N'metadata',
    @level1type=N'TABLE', @level1name=N'performance_indicators',
    @level2type=N'COLUMN', @level2name=N'version';
GO

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                   | Descrição
--------|------------|-------------------------|--------------------------------------------
1.0.0   | 2025-01-17 | bruno.chiaramonti      | Criação inicial da tabela
*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Esta é a fonte oficial de indicadores - todas as aplicações devem referenciar esta tabela
- Mudanças em indicadores criam nova versão, mantendo histórico
- Fórmulas SQL são validadas antes de serem armazenadas
- Indicadores inativos são mantidos para histórico mas não usados em cálculos

Troubleshooting comum:
1. Código duplicado: Verificar constraint UQ_metadata_performance_indicators_code
2. Categoria inválida: Verificar valores permitidos no CHECK constraint
3. Fórmula com erro: Validar sintaxe SQL antes de inserir

Integrações:
- performance_assignments: Referencia indicator_id desta tabela
- performance_targets: Usa indicator_code para definir metas
- Cálculos de performance: Executam calculation_formula

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Criar view auxiliar para indicadores ativos
GO
CREATE VIEW [metadata].[vw_active_indicators]
AS
SELECT 
    indicator_id,
    indicator_code,
    indicator_name,
    category,
    unit,
    aggregation_method,
    calculation_formula,
    is_inverted,
    description
FROM [metadata].[performance_indicators]
WHERE is_active = 1
  AND valid_to IS NULL;
GO

-- Confirmar criação
SELECT 
    t.name AS TableName,
    s.name AS SchemaName,
    t.create_date,
    t.modify_date
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'metadata' AND t.name = 'performance_indicators';
GO

PRINT 'Tabela metadata.performance_indicators criada com sucesso!';