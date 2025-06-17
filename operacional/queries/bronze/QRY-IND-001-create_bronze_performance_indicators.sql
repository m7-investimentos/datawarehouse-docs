-- ==============================================================================
-- QRY-IND-001-create_bronze_performance_indicators
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [bronze, performance, indicadores, staging, ddl]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.performance_indicators para receber dados brutos
           de indicadores de performance extraídos do Google Sheets.

Casos de uso:
- Staging inicial dos dados de indicadores de performance
- Armazenamento temporário antes da validação e transformação
- Auditoria e rastreabilidade de mudanças nos indicadores

Frequência de execução: Uma única vez (criação) ou em recriações
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 10-50 registros por carga
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
Tabela criada: bronze.performance_indicators

Colunas de controle:
- load_id: Identificador único da carga
- load_timestamp: Momento da carga
- load_source: Origem dos dados

Colunas de dados (todas VARCHAR para aceitar qualquer formato):
- indicator_code: Código do indicador
- indicator_name: Nome do indicador
- category: Categoria do indicador
- unit: Unidade de medida
- aggregation: Método de agregação
- formula: Fórmula SQL do indicador
- is_inverted: Indicador invertido (menor é melhor)
- is_active: Indicador ativo
- description: Descrição do indicador
- created_date: Data de criação
- notes: Observações

Colunas de processamento:
- row_number: Número da linha na planilha original
- row_hash: Hash MD5 do registro para detectar mudanças
- is_processed: Flag de processamento
- processing_date: Data do processamento
- processing_status: Status do processamento
- processing_notes: Notas do processamento
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Objetos necessários:
- Schema bronze deve existir

Permissões requeridas:
- CREATE TABLE no schema bronze
- Usuário ETL deve ter INSERT/UPDATE/DELETE na tabela
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- Usar READ_COMMITTED_SNAPSHOT para evitar locks durante leitura
-- A tabela é pequena, não requer particionamento

USE [M7Medallion];
GO

-- ==============================================================================
-- 6. CRIAÇÃO DA TABELA
-- ==============================================================================

-- Drop tabela se existir (cuidado em produção!)
IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'[bronze].[performance_indicators]'))
BEGIN
    DROP TABLE [bronze].[performance_indicators];
    PRINT 'Tabela bronze.performance_indicators removida';
END
GO

-- Criar tabela
CREATE TABLE [bronze].[performance_indicators] (
    -- Campos de controle de carga
    [load_id] INT IDENTITY(1,1) NOT NULL,
    [load_timestamp] DATETIME NOT NULL DEFAULT GETDATE(),
    [load_source] VARCHAR(200) NOT NULL DEFAULT 'GoogleSheets:m7_performance_indicators',
    
    -- Campos da planilha (todos VARCHAR para aceitar qualquer entrada)
    [indicator_code] VARCHAR(MAX) NULL,
    [indicator_name] VARCHAR(MAX) NULL,
    [category] VARCHAR(MAX) NULL,
    [unit] VARCHAR(MAX) NULL,
    [aggregation] VARCHAR(MAX) NULL,
    [formula] VARCHAR(MAX) NULL,
    [is_inverted] VARCHAR(MAX) NULL,
    [is_active] VARCHAR(MAX) NULL,
    [description] VARCHAR(MAX) NULL,
    [created_date] VARCHAR(MAX) NULL,
    [notes] VARCHAR(MAX) NULL,
    
    -- Metadados de controle
    [row_number] INT NULL,
    [row_hash] VARCHAR(32) NULL,
    [is_processed] BIT NOT NULL DEFAULT 0,
    [processing_date] DATETIME NULL,
    [processing_status] VARCHAR(50) NULL,
    [processing_notes] VARCHAR(MAX) NULL,
    
    -- Constraints
    CONSTRAINT [PK_bronze_performance_indicators] PRIMARY KEY CLUSTERED ([load_id] ASC)
) ON [PRIMARY];
GO

-- ==============================================================================
-- 7. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por timestamp de carga
CREATE NONCLUSTERED INDEX [IX_bronze_performance_indicators_load_timestamp]
ON [bronze].[performance_indicators] ([load_timestamp] DESC)
INCLUDE ([load_source], [is_processed]);
GO

-- Índice para processamento pendente
CREATE NONCLUSTERED INDEX [IX_bronze_performance_indicators_pending]
ON [bronze].[performance_indicators] ([is_processed])
WHERE [is_processed] = 0;
GO

-- Índice para busca por código do indicador
CREATE NONCLUSTERED INDEX [IX_bronze_performance_indicators_code]
ON [bronze].[performance_indicators] ([indicator_code])
WHERE [indicator_code] IS NOT NULL;
GO

-- ==============================================================================
-- 8. DOCUMENTAÇÃO E PROPRIEDADES ESTENDIDAS
-- ==============================================================================

-- Documentação da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela bronze para staging de indicadores de performance extraídos do Google Sheets. Armazena dados brutos antes da validação e transformação.',
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_indicators';
GO

-- Documentação das colunas principais
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Identificador único e incremental da carga',
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_indicators',
    @level2type=N'COLUMN', @level2name=N'load_id';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código único do indicador de performance (ex: CAPTACAO_LIQUIDA)',
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_indicators',
    @level2type=N'COLUMN', @level2name=N'indicator_code';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Categoria do indicador: FINANCEIRO, QUALIDADE, VOLUME, COMPORTAMENTAL, PROCESSO, GATILHO',
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_indicators',
    @level2type=N'COLUMN', @level2name=N'category';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Fórmula SQL para cálculo do indicador',
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_indicators',
    @level2type=N'COLUMN', @level2name=N'formula';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Hash MD5 do registro para detectar mudanças',
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_indicators',
    @level2type=N'COLUMN', @level2name=N'row_hash';
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
- Tabela bronze aceita todos os campos como VARCHAR para máxima flexibilidade
- Validações e conversões de tipo são feitas na transformação Bronze → Metadata
- Hash é calculado para detectar mudanças nos registros
- Manter apenas últimas 10 cargas (limpeza via job)

Troubleshooting comum:
1. Campos truncados: Aumentar tamanho se necessário (VARCHAR(MAX) já é o máximo)
2. Performance lenta: Volume é baixo, não deve ocorrer
3. Locks durante carga: Usar TABLOCK hint no INSERT

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
SELECT 
    t.name AS TableName,
    s.name AS SchemaName,
    t.create_date,
    t.modify_date,
    p.rows AS RowCounts
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
WHERE s.name = 'bronze' AND t.name = 'performance_indicators';
GO

PRINT 'Tabela bronze.performance_indicators criada com sucesso!';