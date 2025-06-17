-- ==============================================================================
-- QRY-ASS-001-create_bronze_performance_assignments
-- ==============================================================================
-- Tipo: DDL - Criação de Tabela
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [bronze, ddl, performance, assignments, etl]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: 
    Cria a tabela bronze.performance_assignments para armazenar dados brutos de 
    atribuições de indicadores de performance por assessor, extraídos da planilha 
    Google Sheets m7_performance_assignments.

Casos de uso:
    - Armazenamento temporário de dados extraídos do Google Sheets
    - Validação e processamento de atribuições antes da carga em metadata
    - Auditoria de mudanças nas atribuições ao longo do tempo

Frequência de execução: Uma única vez (criação) ou em caso de rebuild
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: ~200-500 registros por carga
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

NOTA IMPORTANTE: 
- Campos VARCHAR(MAX) não podem ser usados como chave em índices no SQL Server
- Índices foram criados apenas em colunas de tamanho fixo (INT, BIT, DATETIME, VARCHAR(32))
- Para buscar por campos VARCHAR(MAX), use queries diretas sem depender de índices

| Coluna                | Tipo         | Descrição                                   |
|-----------------------|--------------|---------------------------------------------|
| load_id               | INT          | ID único da carga (auto-incremento)        |
| load_timestamp        | DATETIME     | Data/hora da carga                          |
| load_source           | VARCHAR(200) | Fonte dos dados (planilha)                  |
| cod_assessor          | VARCHAR(MAX) | Código do assessor                          |
| nome_assessor         | VARCHAR(MAX) | Nome do assessor                            |
| indicator_code        | VARCHAR(MAX) | Código do indicador                         |
| indicator_type        | VARCHAR(MAX) | Tipo (CARD, GATILHO, KPI, etc.)            |
| weight                | VARCHAR(MAX) | Peso do indicador (0-100)                   |
| valid_from            | VARCHAR(MAX) | Data início vigência                        |
| valid_to              | VARCHAR(MAX) | Data fim vigência                           |
| created_by            | VARCHAR(MAX) | Usuário que criou                           |
| approved_by           | VARCHAR(MAX) | Usuário que aprovou                         |
| comments              | VARCHAR(MAX) | Comentários                                 |
| row_number            | INT          | Número da linha na planilha                 |
| row_hash              | VARCHAR(32)  | Hash MD5 da linha                           |
| is_current            | BIT          | Flag se está vigente                        |
| is_processed          | BIT          | Flag se foi processado                      |
| processing_date       | DATETIME     | Data do processamento                       |
| processing_status     | VARCHAR(50)  | Status do processamento                     |
| processing_notes      | VARCHAR(MAX) | Notas do processamento                      |
| weight_sum_valid      | BIT          | Flag se soma de pesos é válida              |
| indicator_exists      | BIT          | Flag se indicador existe                    |
| validation_errors     | VARCHAR(MAX) | Erros de validação em JSON                  |
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Objetos do banco utilizados:
    - Schema: bronze (deve existir)
    
Pré-requisitos:
    - Permissão CREATE TABLE no schema bronze
    - SQL Server 2016 ou superior (para suporte a JSON)
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
IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'[bronze].[performance_assignments]'))
BEGIN
    DROP TABLE [bronze].[performance_assignments];
    PRINT 'Tabela [bronze].[performance_assignments] removida.';
END
GO

-- Criar tabela
CREATE TABLE [bronze].[performance_assignments] (
    -- Campos de controle de carga
    [load_id] INT IDENTITY(1,1) NOT NULL,
    [load_timestamp] DATETIME NOT NULL DEFAULT GETDATE(),
    [load_source] VARCHAR(200) NOT NULL DEFAULT 'GoogleSheets:m7_performance_assignments',
    
    -- Campos da planilha (todos VARCHAR para Bronze)
    [cod_assessor] VARCHAR(MAX),
    [nome_assessor] VARCHAR(MAX),
    [indicator_code] VARCHAR(MAX),
    [indicator_type] VARCHAR(MAX),
    [weight] VARCHAR(MAX),
    [valid_from] VARCHAR(MAX),
    [valid_to] VARCHAR(MAX),
    [created_by] VARCHAR(MAX),
    [approved_by] VARCHAR(MAX),
    [comments] VARCHAR(MAX),
    
    -- Metadados de controle
    [row_number] INT,
    [row_hash] VARCHAR(32),
    [is_current] BIT,
    [is_processed] BIT DEFAULT 0,
    [processing_date] DATETIME NULL,
    [processing_status] VARCHAR(50) NULL,
    [processing_notes] VARCHAR(MAX) NULL,
    
    -- Validações
    [weight_sum_valid] BIT,
    [indicator_exists] BIT,
    [validation_errors] VARCHAR(MAX),
    
    -- Constraints
    CONSTRAINT [PK_bronze_performance_assignments] PRIMARY KEY CLUSTERED ([load_id] ASC)
) ON [PRIMARY];
GO

-- ==============================================================================
-- 7. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por load_id e processamento
CREATE NONCLUSTERED INDEX [IX_bronze_assignments_load] 
ON [bronze].[performance_assignments] (
    [load_id] ASC,
    [is_processed] ASC
)
INCLUDE ([row_number], [is_current]);
GO

-- Índice para processamento pendente
CREATE NONCLUSTERED INDEX [IX_bronze_assignments_pending] 
ON [bronze].[performance_assignments] ([is_processed] ASC, [load_timestamp] ASC)
WHERE [is_processed] = 0;
GO

-- Índice para validações com erro de peso
CREATE NONCLUSTERED INDEX [IX_bronze_assignments_weight_errors] 
ON [bronze].[performance_assignments] ([weight_sum_valid] ASC, [load_id] ASC)
WHERE [weight_sum_valid] = 0;
GO

-- Índice para validações com erro de indicador
CREATE NONCLUSTERED INDEX [IX_bronze_assignments_indicator_errors] 
ON [bronze].[performance_assignments] ([indicator_exists] ASC, [load_id] ASC)
WHERE [indicator_exists] = 0;
GO

-- Índice para row_hash (busca de duplicatas)
CREATE NONCLUSTERED INDEX [IX_bronze_assignments_hash] 
ON [bronze].[performance_assignments] ([row_hash] ASC)
INCLUDE ([load_id], [is_processed]);
GO

-- ==============================================================================
-- 8. DOCUMENTAÇÃO E COMENTÁRIOS
-- ==============================================================================

-- Adicionar descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela Bronze para armazenamento de atribuições de indicadores de performance por assessor, extraídas do Google Sheets', 
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_assignments';
GO

-- Adicionar descrições das colunas principais
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Identificador único da carga (auto-incremento)', 
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'load_id';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor (formato AAI###)', 
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'cod_assessor';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do indicador de performance', 
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'indicator_code';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tipo do indicador: CARD, GATILHO, KPI, PPI, METRICA', 
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'indicator_type';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Peso do indicador (0-100, apenas para tipo CARD)', 
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'weight';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag indicando se a soma dos pesos CARD = 100% para o assessor/período', 
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'weight_sum_valid';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag indicando se o indicator_code existe em performance_indicators', 
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'indicator_exists';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Erros de validação em formato JSON', 
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'performance_assignments',
    @level2type=N'COLUMN', @level2name=N'validation_errors';
GO

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                | Descrição
--------|------------|----------------------|--------------------------------------------
1.0.0   | 2025-01-17 | bruno.chiaramonti   | Criação inicial da tabela

*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
    - Todos os campos de dados são VARCHAR(MAX) seguindo padrão Bronze
    - A validação de tipos de dados ocorre na transformação para metadata
    - Índices otimizados para queries de processamento pendente
    - Campo validation_errors armazena JSON com detalhes de validação
    - Tabela deve ser limpa periodicamente (retenção de 30 dias)

Troubleshooting comum:
    1. Erro "Cannot insert duplicate key": Verificar se load_id está como IDENTITY
    2. Performance lenta: Verificar índices e estatísticas
    3. JSON inválido em validation_errors: Verificar encoding UTF-8

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Verificar criação da tabela
SELECT 
    t.name AS TableName,
    s.name AS SchemaName,
    t.create_date,
    t.modify_date
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.name = 'performance_assignments' AND s.name = 'bronze';
GO