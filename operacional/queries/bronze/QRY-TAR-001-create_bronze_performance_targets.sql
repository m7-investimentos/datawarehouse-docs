-- ==============================================================================
-- QRY-TAR-001-create_bronze_performance_targets
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [ddl, bronze, performance, targets, metas]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.performance_targets para armazenar metas mensais 
de performance extraídas do Google Sheets, mantendo dados brutos com validações 
e metadados de controle.

Casos de uso:
- Armazenamento de metas anuais de performance por assessor e indicador
- Tracking de ajustes mensais em metas
- Base para cálculos de atingimento e comissionamento

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 2500-3000 registros por ano
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros necessários para execução:
N/A - Script DDL sem parâmetros

Exemplo de uso:
USE M7Medallion;
GO
-- Executar script completo
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela criada: bronze.performance_targets

Colunas principais:
- load_id: Identificador único do registro
- codigo_assessor_crm: Código do assessor no CRM
- indicator_code: Código do indicador
- period_start/end: Período da meta
- target_value: Valor da meta
- stretch_value: Meta stretch (desafio)
- minimum_value: Meta mínima
- Metadados de controle e validação
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
N/A - Criação inicial

Pré-requisitos:
- Schema bronze deve existir
- Permissões CREATE TABLE no schema bronze
- SQL Server 2016+ (para JSON support)
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
USE M7Medallion;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- ==============================================================================
-- 6. VERIFICAÇÃO E LIMPEZA
-- ==============================================================================

-- Verificar se a tabela já existe
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[performance_targets]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.performance_targets já existe. Dropando...';
    DROP TABLE [bronze].[performance_targets];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[performance_targets](
    -- Metadados de carga
    [load_id] [int] IDENTITY(1,1) NOT NULL,
    [load_timestamp] [datetime] NOT NULL DEFAULT (GETDATE()),
    [load_source] [varchar](200) NOT NULL DEFAULT ('GoogleSheets:performance_targets'),
    
    -- Campos da planilha (todos VARCHAR(MAX) para Bronze)
    [codigo_assessor_crm] [varchar](max) NULL,
    [nome_assessor] [varchar](max) NULL,
    [indicator_code] [varchar](max) NULL,
    [period_type] [varchar](max) NULL,
    [period_start] [varchar](max) NULL,
    [period_end] [varchar](max) NULL,
    [target_value] [varchar](max) NULL,
    [stretch_value] [varchar](max) NULL,
    [minimum_value] [varchar](max) NULL,
    
    -- Metadados de controle
    [row_number] [int] NULL,
    [row_hash] [varchar](32) NULL,
    [target_year] [int] NULL,
    [target_quarter] [int] NULL,
    [is_processed] [bit] NULL DEFAULT ((0)),
    [processing_date] [datetime] NULL,
    [processing_status] [varchar](50) NULL,
    [processing_notes] [varchar](max) NULL,
    
    -- Validações
    [target_logic_valid] [bit] NULL,
    [is_inverted] [bit] NULL,
    [validation_errors] [varchar](max) NULL,
    
    CONSTRAINT [PK_bronze_performance_targets] PRIMARY KEY CLUSTERED 
    (
        [load_id] ASC
    ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por ano e processamento
CREATE NONCLUSTERED INDEX [IX_bronze_targets_year_processing] 
ON [bronze].[performance_targets] ([target_year], [is_processed])
WHERE [is_processed] = 0;
GO

-- Índice para validações
CREATE NONCLUSTERED INDEX [IX_bronze_targets_validation]
ON [bronze].[performance_targets] ([target_logic_valid], [is_inverted])
WHERE [is_processed] = 0;
GO

-- Índice para controle de carga
CREATE NONCLUSTERED INDEX [IX_bronze_targets_load]
ON [bronze].[performance_targets] ([load_id], [load_timestamp])
INCLUDE ([target_year]);
GO

-- NOTA: Não é possível criar índices em colunas VARCHAR(MAX)
-- Para queries de busca por crm_id/indicador, considerar:
-- 1. Usar queries com CAST/CONVERT para tipos menores
-- 2. Criar view materializada com tipos apropriados
-- 3. Processar para metadata onde os tipos são otimizados

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Metadados de carga
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID único do registro de carga (auto-incremento)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'load_id';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Timestamp da carga do registro', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'load_timestamp';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Fonte dos dados (planilha Google Sheets)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'load_source';
GO

-- Campos de negócio
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor no CRM', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'codigo_assessor_crm';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Nome completo do assessor', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'nome_assessor';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do indicador de performance', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'indicator_code';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tipo do período (sempre MENSAL)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'period_type';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de início do período (primeiro dia do mês)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'period_start';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de fim do período (último dia do mês)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'period_end';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Valor da meta padrão', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'target_value';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Valor da meta stretch (desafio)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'stretch_value';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Valor da meta mínima', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'minimum_value';
GO

-- Metadados de controle
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Número da linha na planilha original', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'row_number';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Hash MD5 do registro para controle de mudanças', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'row_hash';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Ano da meta', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'target_year';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Trimestre da meta', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'target_quarter';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag indicando se o registro foi processado', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'is_processed';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data do processamento', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'processing_date';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Status do processamento (SUCCESS, ERROR)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'processing_status';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Notas sobre o processamento', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'processing_notes';
GO

-- Validações
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag indicando se a lógica stretch/target/minimum está válida', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'target_logic_valid';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag indicando se o indicador é invertido (menor é melhor)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'is_inverted';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'JSON com erros de validação encontrados', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'validation_errors';
GO

-- ==============================================================================
-- 10. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================

/*
-- Query para verificar estrutura da tabela
SELECT 
    c.name AS column_name,
    t.name AS data_type,
    c.max_length,
    c.is_nullable,
    c.is_identity
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('bronze.performance_targets')
ORDER BY c.column_id;

-- Query para verificar índices
SELECT 
    i.name AS index_name,
    i.type_desc,
    i.is_unique,
    i.is_primary_key
FROM sys.indexes i
WHERE i.object_id = OBJECT_ID('bronze.performance_targets')
    AND i.type > 0
ORDER BY i.name;

-- Query para verificar volume após carga
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT codigo_assessor_crm) as unique_assessors,
    COUNT(DISTINCT indicator_code) as unique_indicators,
    MIN(CAST(period_start AS DATE)) as min_date,
    MAX(CAST(period_start AS DATE)) as max_date
FROM bronze.performance_targets
WHERE is_processed = 0;
*/

-- ==============================================================================
-- 11. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                    | Descrição
--------|------------|--------------------------|--------------------------------------------
1.0.0   | 2025-01-17 | bruno.chiaramonti       | Criação inicial da tabela

*/

-- ==============================================================================
-- 12. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Esta tabela armazena dados brutos do Google Sheets (camada Bronze)
- Todos os campos de dados são VARCHAR(MAX) para flexibilidade
- Volume esperado: ~2500-3000 registros por ano (12 meses x ~200 assessores x indicadores)
- Índices otimizados para queries de processamento e validação
- Campos numéricos são armazenados como VARCHAR e convertidos no processamento

Troubleshooting comum:
1. Erro de conversão de data: Verificar formato no Google Sheets
2. Performance lenta: Verificar índices e estatísticas
3. Validation_errors não vazio: Verificar completude anual e lógica de metas

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/

-- Confirmar criação
PRINT 'Tabela bronze.performance_targets criada com sucesso!';
GO