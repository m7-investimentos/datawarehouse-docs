-- ==============================================================================
-- QRY-ASS-001-create_bronze_performance_assignments
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [ddl, bronze, performance, assignments, atribuicoes]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.performance_assignments para armazenar atribuições 
de indicadores de performance por assessor extraídas do Google Sheets de forma 
bruta, preservando todos os dados originais sem transformações.

Casos de uso:
- Staging inicial de atribuições de indicadores
- Histórico de todas as cargas realizadas
- Base para transformações para camada Silver
- Auditoria de mudanças nas atribuições

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 200-500 atribuições (assessores x indicadores)
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
Tabela criada: bronze.performance_assignments

Colunas principais:
- load_id: ID único da carga (IDENTITY)
- cod_assessor: Código do assessor
- indicator_code: Código do indicador
- weight: Peso do indicador
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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[performance_assignments]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.performance_assignments já existe. Dropando...';
    DROP TABLE [bronze].[performance_assignments];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[performance_assignments](
    -- Metadados de carga
    [load_id] [int] IDENTITY(1,1) NOT NULL,
    [load_timestamp] [datetime] NOT NULL DEFAULT (GETDATE()),
    [load_source] [varchar](100) NOT NULL,
    
    -- Dados da atribuição (todos VARCHAR para preservar formato original)
    [crm_id] [varchar](20) NOT NULL,
    [nome_assessor] [varchar](200) NULL,
    [indicator_code] [varchar](50) NOT NULL,
    [weight] [varchar](20) NULL,
    [is_active] [varchar](10) NULL,
    [valid_from] [varchar](30) NULL,
    [valid_to] [varchar](30) NULL,
    [indicator_type] [varchar](50) NULL,
    [notes] [varchar](1000) NULL,
    
    -- Validações
    [weight_validation] [varchar](10) NULL,
    [total_weight] [varchar](20) NULL,
    
    -- Controle de linha original
    [row_number] [int] NULL,
    [row_hash] [varchar](64) NULL,
    
    -- Controle de processamento
    [is_processed] [bit] NOT NULL DEFAULT ((0)),
    [processing_date] [datetime] NULL,
    [processing_status] [varchar](50) NULL,
    [processing_notes] [varchar](500) NULL,
    
    CONSTRAINT [PK_bronze_performance_assignments] PRIMARY KEY CLUSTERED 
    (
        [load_id] ASC
    ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por assessor
CREATE NONCLUSTERED INDEX [IX_bronze_assignments_assessor]
ON [bronze].[performance_assignments] ([crm_id])
INCLUDE ([indicator_code], [weight], [is_processed]);
GO

-- Índice para busca por indicador
CREATE NONCLUSTERED INDEX [IX_bronze_assignments_indicator]
ON [bronze].[performance_assignments] ([indicator_code])
INCLUDE ([crm_id], [weight]);
GO

-- Índice para processamento ETL
CREATE NONCLUSTERED INDEX [IX_bronze_assignments_processing]
ON [bronze].[performance_assignments] ([is_processed], [load_timestamp])
WHERE [is_processed] = 0;
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela bronze para staging de atribuições de indicadores de performance do Google Sheets', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments';
GO

-- Metadados de carga
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID único da carga (auto-incremento)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'load_id';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Timestamp da carga dos dados', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'load_timestamp';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Fonte dos dados (Google Sheets ID)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'load_source';
GO

-- Dados da atribuição
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID do CRM do assessor', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'crm_id';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Nome do assessor', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'nome_assessor';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do indicador', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'indicator_code';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Peso do indicador (%)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'weight';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Atribuição ativa (1=ativo)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'is_active';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de início da vigência', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'valid_from';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de fim da vigência', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'valid_to';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tipo do indicador (CARD/RANKING)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'indicator_type';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Notas e observações', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'notes';
GO

-- Validações
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Validação do peso (1=válido, 0=inválido)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'weight_validation';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Soma total dos pesos do assessor', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'total_weight';
GO

-- Controle
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Número da linha no arquivo original', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'row_number';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Hash MD5 da linha para detecção de mudanças', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'row_hash';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag indicando se o registro foi processado', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'is_processed';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data do processamento', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'processing_date';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Status do processamento', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
    @level2type=N'COLUMN',@level2name=N'processing_status';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Notas sobre o processamento', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'performance_assignments', 
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
- Validação de pesos deve ser feita na camada Silver
- Hash de linha usado para detectar mudanças nas atribuições

Troubleshooting comum:
1. Erro de duplicação: Verificar combinação crm_id/indicador duplicada
2. Validação de pesos: Soma deve ser 100% para indicadores tipo CARD
3. Performance: Criar estatísticas nos índices após grandes cargas

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/

-- Confirmar criação
PRINT 'Tabela bronze.performance_assignments criada com sucesso!';
GO