-- ==============================================================================
-- QRY-IND-001-create_bronze_performance_indicators
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [ddl, bronze, performance, indicators, kpis]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.performance_indicators para armazenar indicadores 
de performance extraídos do Google Sheets de forma bruta, preservando todos os 
dados originais sem transformações.

Casos de uso:
- Staging inicial de indicadores de performance
- Histórico de todas as cargas realizadas
- Base para transformações para camada Silver
- Auditoria de mudanças nos indicadores

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 10-50 indicadores
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
Tabela criada: bronze.performance_indicators

Colunas principais:
- load_id: ID único da carga (IDENTITY)
- indicator_code: Código do indicador
- indicator_name: Nome do indicador
- category: Categoria do indicador
- Campos de controle ETL
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
N/A

Pré-requisitos:
- Schema bronze deve existir
- Permissões CREATE TABLE no schema bronze
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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[performance_indicators]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.performance_indicators já existe. Dropando...';
    DROP TABLE [bronze].[performance_indicators];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[performance_indicators](
    -- Metadados de carga
    [load_id] [int] IDENTITY(1,1) NOT NULL,
    [load_timestamp] [datetime] NOT NULL DEFAULT (GETDATE()),
    [load_source] [varchar](100) NOT NULL,
    
    -- Dados do indicador (todos VARCHAR para preservar formato original)
    [indicator_code] [varchar](50) NOT NULL,
    [indicator_name] [varchar](200) NOT NULL,
    [category] [varchar](100) NULL,
    [subcategory] [varchar](100) NULL,
    [indicator_type] [varchar](50) NULL,
    [unit] [varchar](50) NULL,
    [aggregation] [varchar](50) NULL,
    [formula] [varchar](500) NULL,
    [is_inverted] [varchar](10) NULL,
    [is_active] [varchar](10) NULL,
    [description] [varchar](1000) NULL,
    [notes] [varchar](1000) NULL,
    
    -- Controle de linha original
    [row_number] [int] NULL,
    [row_hash] [varchar](64) NULL,
    
    -- Controle de processamento
    [is_processed] [bit] NOT NULL DEFAULT ((0)),
    [processing_date] [datetime] NULL,
    [processing_status] [varchar](50) NULL,
    [processing_notes] [varchar](500) NULL,
    
    CONSTRAINT [PK_bronze_performance_indicators] PRIMARY KEY CLUSTERED 
    (
        [load_id] ASC
    ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por código do indicador
CREATE NONCLUSTERED INDEX [IX_bronze_indicators_code]
ON [bronze].[performance_indicators] ([indicator_code])
INCLUDE ([indicator_name], [is_processed]);
GO

-- Índice para processamento ETL
CREATE NONCLUSTERED INDEX [IX_bronze_indicators_processing]
ON [bronze].[performance_indicators] ([is_processed], [load_timestamp])
WHERE [is_processed] = 0;
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela bronze para staging de indicadores de performance do Google Sheets', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators';
GO

-- Metadados de carga
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID único da carga (auto-incremento)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'load_id';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Timestamp da carga dos dados', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'load_timestamp';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Fonte dos dados (Google Sheets ID)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'load_source';
GO

-- Dados do indicador
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código único do indicador', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'indicator_code';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Nome descritivo do indicador', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'indicator_name';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Categoria do indicador', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'category';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Subcategoria do indicador', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'subcategory';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tipo do indicador (CARD/RANKING)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'indicator_type';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Unidade de medida', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'unit';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Método de agregação (SUM/AVG/MAX/MIN)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'aggregation';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Fórmula de cálculo', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'formula';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Indicador invertido (1=menor é melhor)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'is_inverted';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Indicador ativo (1=ativo)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'is_active';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Descrição detalhada do indicador', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'description';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Notas e observações', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'notes';
GO

-- Controle
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Número da linha no arquivo original', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'row_number';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Hash MD5 da linha para detecção de mudanças', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'row_hash';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag indicando se o registro foi processado', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'is_processed';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data do processamento', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'processing_date';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Status do processamento', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'processing_status';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Notas sobre o processamento', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_indicators', 
    @level2type=N'COLUMN',@level2name=N'processing_notes';
GO

-- ==============================================================================
-- 10. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                    | Descrição
--------|------------|--------------------------|--------------------------------------------
1.0.0   | 2025-01-17 | bruno.chiaramonti       | Criação inicial da tabela

*/

-- ==============================================================================
-- 11. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Esta tabela armazena dados brutos do Google Sheets sem transformações
- Todos os campos são VARCHAR para preservar o formato original
- Processamento para Silver deve validar e converter tipos de dados
- Hash de linha usado para detectar mudanças nos indicadores

Troubleshooting comum:
1. Erro de duplicação: Verificar indicator_code duplicado na mesma carga
2. Performance: Criar estatísticas nos índices após grandes cargas

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/

-- Confirmar criação
PRINT 'Tabela bronze.performance_indicators criada com sucesso!';
GO