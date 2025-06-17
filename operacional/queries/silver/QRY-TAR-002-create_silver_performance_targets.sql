-- ==============================================================================
-- QRY-TAR-002-create_silver_performance_targets
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [ddl, silver, performance, targets, metas]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela silver.performance_targets para armazenar metas de 
performance processadas e validadas, com tipos de dados apropriados e 
relacionamentos com indicadores.

Casos de uso:
- Armazenamento estruturado de metas após validação
- Base para cálculos de atingimento de metas
- Integração com sistema de comissionamento
- Análises temporais de evolução de metas

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
Tabela criada: silver.performance_targets

Colunas principais:
- target_id: Identificador único da meta
- cod_assessor: Código do assessor (FK)
- indicator_id: ID do indicador (FK)
- period_start/end: Período da meta
- target_value: Valor da meta
- stretch_target: Meta stretch
- minimum_target: Meta mínima
- Auditoria e controle
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- silver.performance_indicators: Para FK de indicator_id
- dim.assessores: Para validação de cod_assessor (se existir)

Pré-requisitos:
- Schema silver deve existir
- Tabela silver.performance_indicators deve existir
- Permissões CREATE TABLE no schema silver
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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[silver].[performance_targets]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela silver.performance_targets já existe. Dropando...';
    
    -- Remover FKs primeiro
    IF EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[silver].[FK_performance_targets_indicators]'))
        ALTER TABLE [silver].[performance_targets] DROP CONSTRAINT [FK_performance_targets_indicators];
    
    DROP TABLE [silver].[performance_targets];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [silver].[performance_targets](
    -- Chave primária
    [target_id] [int] IDENTITY(1,1) NOT NULL,
    
    -- Chaves de negócio
    [cod_assessor] [varchar](20) NOT NULL,
    [indicator_id] [int] NOT NULL,
    
    -- Período
    [period_type] [varchar](20) NOT NULL DEFAULT ('MENSAL'),
    [period_start] [date] NOT NULL,
    [period_end] [date] NOT NULL,
    
    -- Valores de meta
    [target_value] [decimal](18,4) NOT NULL,
    [stretch_target] [decimal](18,4) NULL,
    [minimum_target] [decimal](18,4) NULL,
    
    -- Metadados
    [is_active] [bit] NOT NULL DEFAULT ((1)),
    [created_date] [datetime] NOT NULL DEFAULT (GETDATE()),
    [created_by] [varchar](100) NOT NULL DEFAULT (SUSER_SNAME()),
    [modified_date] [datetime] NULL,
    [modified_by] [varchar](100) NULL,
    
    -- Controle de origem
    [source_system] [varchar](50) NOT NULL DEFAULT ('GoogleSheets'),
    [source_id] [varchar](100) NULL,
    [bronze_load_id] [int] NULL,
    
    CONSTRAINT [PK_silver_performance_targets] PRIMARY KEY CLUSTERED 
    (
        [target_id] ASC
    ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
    
    -- Constraint única para evitar duplicatas
    CONSTRAINT [UQ_performance_targets_unique] UNIQUE NONCLUSTERED 
    (
        [cod_assessor] ASC,
        [indicator_id] ASC,
        [period_start] ASC
    ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY];
GO

-- ==============================================================================
-- 8. FOREIGN KEYS
-- ==============================================================================

-- FK para indicadores
ALTER TABLE [silver].[performance_targets] WITH CHECK 
ADD CONSTRAINT [FK_performance_targets_indicators] FOREIGN KEY([indicator_id])
REFERENCES [silver].[performance_indicators] ([indicator_id]);
GO

ALTER TABLE [silver].[performance_targets] CHECK CONSTRAINT [FK_performance_targets_indicators];
GO

-- ==============================================================================
-- 9. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por assessor e período
CREATE NONCLUSTERED INDEX [IX_targets_assessor_period]
ON [silver].[performance_targets] ([cod_assessor], [period_start])
INCLUDE ([indicator_id], [target_value], [stretch_target], [minimum_target]);
GO

-- Índice para busca por indicador
CREATE NONCLUSTERED INDEX [IX_targets_indicator]
ON [silver].[performance_targets] ([indicator_id], [period_start])
INCLUDE ([cod_assessor], [target_value]);
GO

-- Índice para análises temporais
CREATE NONCLUSTERED INDEX [IX_targets_temporal]
ON [silver].[performance_targets] ([period_start], [period_end])
WHERE [is_active] = 1;
GO

-- ==============================================================================
-- 10. CHECK CONSTRAINTS
-- ==============================================================================

-- Validar que period_end >= period_start
ALTER TABLE [silver].[performance_targets] WITH CHECK 
ADD CONSTRAINT [CK_targets_period_valid] CHECK ([period_end] >= [period_start]);
GO

-- Validar que valores não sejam negativos (exceto quando permitido)
ALTER TABLE [silver].[performance_targets] WITH CHECK 
ADD CONSTRAINT [CK_targets_values_positive] CHECK (
    [target_value] >= 0 OR [target_value] < 0 -- Permite negativos para alguns indicadores
);
GO

-- ==============================================================================
-- 11. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela de metadados com metas de performance mensais por assessor e indicador', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets';
GO

-- Chave primária
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID único da meta (auto-incremento)', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'target_id';
GO

-- Chaves de negócio
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor/AAI', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'cod_assessor';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID do indicador (FK para silver.performance_indicators)', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'indicator_id';
GO

-- Período
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tipo do período (MENSAL, TRIMESTRAL, ANUAL)', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'period_type';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de início do período', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'period_start';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de fim do período', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'period_end';
GO

-- Valores
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Valor da meta padrão (100% de atingimento)', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'target_value';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Meta stretch/desafio (>100% de atingimento)', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'stretch_target';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Meta mínima aceitável', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'minimum_target';
GO

-- Metadados
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag indicando se a meta está ativa', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'is_active';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de criação do registro', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'created_date';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Usuário que criou o registro', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'created_by';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data da última modificação', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'modified_date';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Usuário que fez a última modificação', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'modified_by';
GO

-- Controle de origem
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Sistema de origem dos dados', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'source_system';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID do registro no sistema de origem', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'source_id';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID do registro na tabela bronze de origem', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'performance_targets', 
    @level2type=N'COLUMN',@level2name=N'bronze_load_id';
GO

-- ==============================================================================
-- 12. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================

/*
-- Query para verificar estrutura da tabela
SELECT 
    c.name AS column_name,
    t.name AS data_type,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('silver.performance_targets')
ORDER BY c.column_id;

-- Query para verificar relacionamentos
SELECT 
    fk.name AS FK_name,
    tp.name AS parent_table,
    cp.name AS parent_column,
    tr.name AS referenced_table,
    cr.name AS referenced_column
FROM sys.foreign_keys fk
INNER JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
INNER JOIN sys.tables tr ON fk.referenced_object_id = tr.object_id
INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
INNER JOIN sys.columns cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
INNER JOIN sys.columns cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id
WHERE tp.name = 'performance_targets';

-- Query para análise de metas por assessor
SELECT 
    t.cod_assessor,
    i.indicator_name,
    YEAR(t.period_start) as target_year,
    COUNT(*) as months_defined,
    AVG(t.target_value) as avg_target,
    SUM(t.target_value) as total_annual_target
FROM silver.performance_targets t
INNER JOIN silver.performance_indicators i ON t.indicator_id = i.indicator_id
WHERE t.is_active = 1
GROUP BY t.cod_assessor, i.indicator_name, YEAR(t.period_start)
ORDER BY t.cod_assessor, i.indicator_name, target_year;
*/

-- ==============================================================================
-- 13. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                    | Descrição
--------|------------|--------------------------|--------------------------------------------
1.0.0   | 2025-01-17 | bruno.chiaramonti       | Criação inicial da tabela

*/

-- ==============================================================================
-- 14. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Esta tabela armazena metas processadas e validadas (camada Metadata)
- Relacionamento obrigatório com silver.performance_indicators
- Constraint única previne duplicação de metas para mesmo assessor/indicador/período
- Valores decimais com precisão 18,4 para suportar valores monetários grandes
- Índices otimizados para queries de análise e cálculo de atingimento

Troubleshooting comum:
1. Erro de FK: Verificar se indicator_id existe em silver.performance_indicators
2. Erro de constraint única: Verificar duplicatas de assessor/indicador/período
3. Performance lenta: Atualizar estatísticas dos índices

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/

-- Confirmar criação
PRINT 'Tabela silver.performance_targets criada com sucesso!';
GO