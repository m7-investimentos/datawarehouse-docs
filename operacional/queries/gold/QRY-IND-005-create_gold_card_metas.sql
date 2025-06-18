-- ==============================================================================
-- QRY-IND-005-create_gold_card_metas
-- ==============================================================================
-- Tipo: DDL - Criação de Tabela
-- Versão: 1.0.0
-- Última atualização: 2025-01-18
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [gold, performance, eav, indicadores, metas, card]
-- Status: aprovado
-- Banco de Dados: SQL Server 2019+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criar a tabela principal da camada Gold para armazenamento de 
indicadores de performance em modelo EAV (Entity-Attribute-Value). Esta tabela
suporta cálculos dinâmicos de performance individualizados com indicadores e 
pesos personalizados por pessoa.

Casos de uso:
- Armazenar resultados calculados de performance mensal por assessor
- Suportar N indicadores dinâmicos sem alteração de estrutura
- Permitir análises de atingimento e rankings
- Base para dashboards Power BI e consumo ML

Frequência de execução: Única (criação inicial)
Volume esperado de linhas: ~10.000 registros/mês (500 pessoas × 20 indicadores)
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Não aplicável - Script DDL
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela criada: gold.card_metas

Modelo EAV com campos:
- Identificação: meta_id, period_start, period_end
- Entidade: entity_type, entity_id (assessor)
- Atributo: attribute_type, attribute_code, attribute_name
- Valores: target_value, realized_value, achievement_percentage
- Metadados: indicator_type, indicator_category, indicator_weight
- Cálculo: weighted_achievement, achievement_status
- Auditoria: processing_date, processing_notes
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Schemas necessários:
- gold: Schema da camada Gold deve existir

Tabelas relacionadas (não são FKs físicas):
- silver.performance_indicators: Origem dos metadados de indicadores
- silver.performance_assignments: Origem das atribuições pessoa/indicador
- silver.performance_targets: Origem das metas por período

Pré-requisitos:
- Permissões CREATE TABLE no schema gold
- Processamento Silver deve estar completo
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- Usar schema gold
USE M7Medallion;
GO

-- ==============================================================================
-- 6. DROP E CREATE DO SCHEMA (SE NECESSÁRIO)
-- ==============================================================================
-- Criar schema gold se não existir
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold AUTHORIZATION dbo');
    PRINT 'Schema gold criado com sucesso';
END
GO

-- Drop da tabela se existir (cuidado em produção!)
IF OBJECT_ID('gold.card_metas', 'U') IS NOT NULL
BEGIN
    DROP TABLE gold.card_metas;
    PRINT 'Tabela gold.card_metas removida';
END
GO

-- ==============================================================================
-- 7. CREATE TABLE
-- ==============================================================================
CREATE TABLE gold.card_metas (
    -- Identificação única
    meta_id INT IDENTITY(1,1) NOT NULL,
    
    -- Período de referência
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    
    -- Entity (Assessor/Pessoa)
    entity_type VARCHAR(50) NOT NULL DEFAULT 'ASSESSOR',
    entity_id VARCHAR(20) NOT NULL, -- codigo_assessor_crm
    
    -- Attribute (Indicador)
    attribute_type VARCHAR(50) NOT NULL DEFAULT 'INDICATOR',
    attribute_code VARCHAR(50) NOT NULL,
    attribute_name VARCHAR(200) NOT NULL,
    
    -- Classificação do indicador
    indicator_type VARCHAR(20) NOT NULL, -- CARD, GATILHO, KPI, PPI
    indicator_category VARCHAR(50) NULL,
    
    -- Valores de meta
    target_value DECIMAL(18,4) NULL,
    stretch_value DECIMAL(18,4) NULL,
    minimum_value DECIMAL(18,4) NULL,
    
    -- Valor realizado (calculado dinamicamente)
    realized_value DECIMAL(18,4) NULL,
    
    -- Cálculos de atingimento
    achievement_percentage DECIMAL(5,2) NULL, -- % de atingimento
    indicator_weight DECIMAL(5,2) NOT NULL DEFAULT 0, -- Peso do indicador
    weighted_achievement DECIMAL(5,2) NULL, -- Atingimento ponderado
    
    -- Status de atingimento
    achievement_status VARCHAR(20) NULL, -- SUPERADO, ATINGIDO, PARCIAL, NAO_ATINGIDO
    
    -- Flags de controle
    is_inverted BIT NOT NULL DEFAULT 0, -- 1 = indicador invertido
    is_calculated BIT NOT NULL DEFAULT 0, -- 1 = cálculo realizado
    has_error BIT NOT NULL DEFAULT 0, -- 1 = erro no cálculo
    
    -- Metadados do cálculo
    calculation_formula VARCHAR(MAX) NULL, -- Fórmula SQL usada
    calculation_method VARCHAR(20) NULL, -- SUM, AVG, COUNT, CUSTOM
    data_source VARCHAR(100) NULL, -- Tabela origem dos dados
    
    -- Auditoria e controle
    processing_date DATETIME NOT NULL DEFAULT GETDATE(),
    processing_id INT NULL, -- ID da execução da procedure
    processing_duration_ms INT NULL, -- Tempo de cálculo em ms
    processing_notes VARCHAR(MAX) NULL, -- Notas/erros do processamento
    
    -- Controle de versão
    created_date DATETIME NOT NULL DEFAULT GETDATE(),
    created_by VARCHAR(100) NOT NULL DEFAULT SYSTEM_USER,
    modified_date DATETIME NULL,
    modified_by VARCHAR(100) NULL,
    
    -- Constraints
    CONSTRAINT PK_gold_card_metas PRIMARY KEY CLUSTERED (meta_id),
    CONSTRAINT UQ_gold_card_metas_unique UNIQUE (
        entity_id, 
        attribute_code, 
        period_start
    ),
    CONSTRAINT CK_gold_card_metas_period CHECK (period_end >= period_start),
    CONSTRAINT CK_gold_card_metas_achievement CHECK (
        achievement_percentage IS NULL OR 
        (achievement_percentage >= -999.99 AND achievement_percentage <= 999.99)
    ),
    CONSTRAINT CK_gold_card_metas_weight CHECK (
        indicator_weight >= 0 AND indicator_weight <= 100
    ),
    CONSTRAINT CK_gold_card_metas_status CHECK (
        achievement_status IS NULL OR
        achievement_status IN ('SUPERADO', 'ATINGIDO', 'PARCIAL', 'NAO_ATINGIDO', 'NAO_APLICAVEL')
    ),
    CONSTRAINT CK_gold_card_metas_indicator_type CHECK (
        indicator_type IN ('CARD', 'GATILHO', 'KPI', 'PPI')
    )
);
GO

-- ==============================================================================
-- 8. ÍNDICES
-- ==============================================================================
-- Índice para consultas por entity (assessor)
CREATE NONCLUSTERED INDEX IX_gold_card_metas_entity 
ON gold.card_metas (entity_id, period_start)
INCLUDE (attribute_code, realized_value, achievement_percentage);
GO

-- Índice para consultas por período
CREATE NONCLUSTERED INDEX IX_gold_card_metas_period 
ON gold.card_metas (period_start, period_end)
INCLUDE (entity_id, achievement_status);
GO

-- Índice para consultas por indicador
CREATE NONCLUSTERED INDEX IX_gold_card_metas_attribute 
ON gold.card_metas (attribute_code)
INCLUDE (entity_id, period_start, realized_value);
GO

-- Índice para filtrar apenas indicadores CARD (com peso)
CREATE NONCLUSTERED INDEX IX_gold_card_metas_card_type 
ON gold.card_metas (indicator_type, period_start)
WHERE indicator_type = 'CARD';
GO

-- Índice para identificar processamentos com erro
CREATE NONCLUSTERED INDEX IX_gold_card_metas_errors 
ON gold.card_metas (has_error, processing_date)
WHERE has_error = 1;
GO

-- ==============================================================================
-- 9. EXTENDED PROPERTIES
-- ==============================================================================
-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela EAV (Entity-Attribute-Value) para armazenar resultados de performance calculados dinamicamente. Suporta N indicadores por pessoa sem alteração de estrutura.', 
    @level0type=N'SCHEMA', @level0name=N'gold',
    @level1type=N'TABLE', @level1name=N'card_metas';

-- Descrições das colunas principais
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID único do registro de meta/realizado', 
    @level0type=N'SCHEMA', @level0name=N'gold',
    @level1type=N'TABLE', @level1name=N'card_metas',
    @level2type=N'COLUMN', @level2name=N'meta_id';

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor CRM (entity_id no modelo EAV)', 
    @level0type=N'SCHEMA', @level0name=N'gold',
    @level1type=N'TABLE', @level1name=N'card_metas',
    @level2type=N'COLUMN', @level2name=N'entity_id';

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do indicador (attribute_code no modelo EAV)', 
    @level0type=N'SCHEMA', @level0name=N'gold',
    @level1type=N'TABLE', @level1name=N'card_metas',
    @level2type=N'COLUMN', @level2name=N'attribute_code';

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Valor calculado dinamicamente através da fórmula SQL do indicador', 
    @level0type=N'SCHEMA', @level0name=N'gold',
    @level1type=N'TABLE', @level1name=N'card_metas',
    @level2type=N'COLUMN', @level2name=N'realized_value';

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Percentual de atingimento: (realized/target)*100 ou (2-realized/target)*100 se invertido', 
    @level0type=N'SCHEMA', @level0name=N'gold',
    @level1type=N'TABLE', @level1name=N'card_metas',
    @level2type=N'COLUMN', @level2name=N'achievement_percentage';

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Atingimento ponderado: achievement_percentage * indicator_weight (apenas para CARD)', 
    @level0type=N'SCHEMA', @level0name=N'gold',
    @level1type=N'TABLE', @level1name=N'card_metas',
    @level2type=N'COLUMN', @level2name=N'weighted_achievement';

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'1 = indicador invertido (menor valor é melhor)', 
    @level0type=N'SCHEMA', @level0name=N'gold',
    @level1type=N'TABLE', @level1name=N'card_metas',
    @level2type=N'COLUMN', @level2name=N'is_inverted';

GO

-- ==============================================================================
-- 10. TABELA DE LOG DE PROCESSAMENTO
-- ==============================================================================
-- Drop da tabela de log se existir
IF OBJECT_ID('gold.processing_log', 'U') IS NOT NULL
BEGIN
    DROP TABLE gold.processing_log;
    PRINT 'Tabela gold.processing_log removida';
END
GO

-- Criar tabela de log para rastreabilidade
CREATE TABLE gold.processing_log (
    log_id INT IDENTITY(1,1) NOT NULL,
    processing_id INT NOT NULL,
    processing_type VARCHAR(50) NOT NULL, -- 'FULL', 'INCREMENTAL', 'REPROCESSING'
    
    -- Parâmetros de execução
    period_start DATE NULL,
    period_end DATE NULL,
    entity_id VARCHAR(20) NULL, -- Se processar específico
    
    -- Controle de execução
    start_time DATETIME NOT NULL DEFAULT GETDATE(),
    end_time DATETIME NULL,
    duration_seconds INT NULL,
    
    -- Estatísticas
    total_entities INT NULL,
    total_indicators INT NULL,
    total_calculations INT NULL,
    successful_calculations INT NULL,
    failed_calculations INT NULL,
    
    -- Status e mensagens
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING', -- RUNNING, SUCCESS, ERROR, WARNING
    error_message VARCHAR(MAX) NULL,
    warning_messages VARCHAR(MAX) NULL,
    
    -- Auditoria
    executed_by VARCHAR(100) NOT NULL DEFAULT SYSTEM_USER,
    execution_notes VARCHAR(MAX) NULL,
    
    CONSTRAINT PK_gold_processing_log PRIMARY KEY CLUSTERED (log_id),
    CONSTRAINT CK_gold_processing_log_status CHECK (
        status IN ('RUNNING', 'SUCCESS', 'ERROR', 'WARNING', 'CANCELLED')
    )
);
GO

-- Índice para buscar execuções recentes
CREATE NONCLUSTERED INDEX IX_gold_processing_log_recent 
ON gold.processing_log (start_time DESC)
INCLUDE (status, total_calculations);
GO

-- Descrição da tabela de log
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Log de execuções do processamento Gold para rastreabilidade e auditoria', 
    @level0type=N'SCHEMA', @level0name=N'gold',
    @level1type=N'TABLE', @level1name=N'processing_log';
GO

-- ==============================================================================
-- 11. PERMISSÕES
-- ==============================================================================
-- Conceder permissões de leitura
GRANT SELECT ON gold.card_metas TO db_datareader;
GRANT SELECT ON gold.processing_log TO db_datareader;
GO

-- Conceder permissões de escrita apenas para processos ETL
IF EXISTS (SELECT * FROM sys.database_principals WHERE name = 'db_etl_writer')
BEGIN
    GRANT INSERT, UPDATE ON gold.card_metas TO db_etl_writer;
    GRANT INSERT, UPDATE ON gold.processing_log TO db_etl_writer;
END
GO

-- ==============================================================================
-- 12. DADOS DE EXEMPLO/TESTE
-- ==============================================================================
/*
-- Inserir registro de exemplo (comentado para produção)
INSERT INTO gold.card_metas (
    period_start, period_end, 
    entity_id, 
    attribute_code, attribute_name,
    indicator_type, indicator_category,
    target_value, realized_value,
    achievement_percentage, indicator_weight, weighted_achievement,
    achievement_status, calculation_formula
)
VALUES (
    '2025-01-01', '2025-01-31',
    'AAI001',
    'CAPT_LIQ', 'Captação Líquida',
    'CARD', 'FINANCEIRO',
    500000.00, 450000.00,
    90.00, 40.00, 36.00,
    'ATINGIDO', 'SELECT captacao_liquida_total FROM gold.captacao_liquida_assessor WHERE codigo_assessor_crm = @entity_id'
);
*/

-- ==============================================================================
-- 13. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                  | Descrição
--------|------------|------------------------|--------------------------------------------
1.0.0   | 2025-01-18 | bruno.chiaramonti     | Criação inicial da tabela EAV Gold
*/

-- ==============================================================================
-- 14. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Modelo EAV permite flexibilidade total para N indicadores
- Fórmulas SQL são armazenadas e executadas dinamicamente
- Performance adequada para volume esperado (10K registros/mês)
- Índices otimizados para padrões de consulta mais comuns
- Tabela processing_log essencial para troubleshooting

Padrões de nomenclatura:
- entity_id = codigo_assessor_crm (do modelo Silver)
- attribute_code = indicator_code (do modelo Silver)
- Mantém compatibilidade com nomenclatura EAV padrão

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Fim do script
PRINT 'Tabelas gold.card_metas e gold.processing_log criadas com sucesso!';
GO