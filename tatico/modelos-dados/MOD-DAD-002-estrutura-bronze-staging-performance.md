# MOD-002-estrutura-bronze-performance

---
título: Modelo de Dados Bronze - Sistema de Performance Tracking
tipo: MOD - Modelo de Dados
versão: 1.0.0
última_atualização: 2025-01-16
autor: arquitetura.dados@m7investimentos.com.br
aprovador: diretoria.ti@m7investimentos.com.br
tags: [modelo, bronze, staging, etl, performance, medallion]
status: aprovado
dependências:
  - tipo: modelo
    ref: [MOD-001]
    repo: datawarehouse-docs
  - tipo: etl
    ref: [ETL-001, ETL-002, ETL-003]
    repo: datawarehouse-docs
---

## 1. Objetivo

Documentar a estrutura das tabelas Bronze (staging) do sistema de Performance Tracking, que recebem dados brutos das planilhas Google Sheets. Estas tabelas são projetadas para aceitar qualquer formato de entrada, permitindo validação e transformação posterior para as camadas superiores da arquitetura Medallion.

## 2. Escopo e Contexto

### 2.1 Domínio de Negócio
- **Área**: Staging/ETL para Performance Tracking
- **Processos suportados**: 
  - Recepção de dados não estruturados
  - Validação inicial de dados
  - Quarentena de registros problemáticos
  - Auditoria de cargas
- **Stakeholders**: 
  - Equipe de Dados
  - Administradores ETL

### 2.2 Contexto Técnico
- **Tipo de modelo**: Staging tables (permissivas)
- **Plataforma**: SQL Server 2019+
- **Database**: M7Medallion
- **Schema**: bronze
- **Layer**: Bronze (Raw/Staging)
- **Característica principal**: Aceita VARCHAR(MAX) para todos os campos de negócio

## 3. Visão Geral do Modelo

### 3.1 Diagrama de Fluxo Bronze

```
FONTES EXTERNAS                    CAMADA BRONZE                         PRÓXIMAS CAMADAS
================                   ==============                        ================

┌─────────────────────┐           ┌─────────────────────────┐          ┌──────────────┐
│ Google Sheets:      │           │ bronze.performance_     │          │              │
│ m7_performance_     │ ─ETL-001─>│ indicators              │─MOD-003─>│  metadados.  │
│ indicators          │           │ (staging indicators)    │          │ performance_ │
└─────────────────────┘           └─────────────────────────┘          │ indicators   │
                                            │                           └──────────────┘
                                            ▼
┌─────────────────────┐           ┌─────────────────────────┐
│ Google Sheets:      │           │ bronze.performance_     │          ┌──────────────┐
│ m7_performance_     │ ─ETL-002─>│ assignments             │─MOD-003─>│  metadados.  │
│ assignments         │           │ (staging assignments)   │          │ performance_ │
└─────────────────────┘           └─────────────────────────┘          │ assignments  │
                                            │                           └──────────────┘
                                            ▼
┌─────────────────────┐           ┌─────────────────────────┐
│ Google Sheets:      │           │ bronze.performance_     │          ┌──────────────┐
│ m7_performance_     │ ─ETL-003─>│ targets                 │─MOD-003─>│  metadados.  │
│ targets             │           │ (staging targets)       │          │ performance_ │
└─────────────────────┘           └─────────────────────────┘          │ targets      │
                                            │                           └──────────────┘
                                            ▼
                                  ┌─────────────────────────┐
                                  │ bronze.etl_control      │
                                  │ (log de execuções)     │
                                  └─────────────────────────┘
```

### 3.2 Principais Características Bronze
| Característica | Descrição | Justificativa |
|----------------|-----------|---------------|
| Campos VARCHAR(MAX) | Todos os campos de negócio | Aceita qualquer formato |
| Sem constraints de negócio | Apenas NOT NULL em campos de controle | Não rejeitar dados |
| Metadados de carga | load_id, load_timestamp, etc | Rastreabilidade |
| Controle de processamento | is_processed, processing_status | Gestão do pipeline |
| Row hash | Hash MD5 do registro | Detectar mudanças |
| Validação posterior | validation_errors em JSON | Não perder informação |

## 4. Dicionário de Dados Detalhado

### 4.1 Tabela: bronze.performance_indicators

**Descrição**: Tabela staging para receber dados brutos do catálogo de indicadores de performance vindos do Google Sheets.

| Campo | Tipo | Constraint | Descrição | Exemplo |
|-------|------|------------|-----------|---------|
| **Campos de Controle ETL** |
| load_id | INT | PK, IDENTITY | ID único da carga | 1 |
| load_timestamp | DATETIME | NOT NULL, DEFAULT | Momento da carga | 2025-01-16 10:30:00 |
| load_source | VARCHAR(200) | NOT NULL, DEFAULT | Origem dos dados | 'GoogleSheets:m7_performance_indicators' |
| **Campos da Planilha (todos VARCHAR(MAX))** |
| indicator_code | VARCHAR(MAX) | NULL | Código do indicador | "CAPTACAO_LIQUIDA" |
| indicator_name | VARCHAR(MAX) | NULL | Nome do indicador | "Captação Líquida Mensal" |
| category | VARCHAR(MAX) | NULL | Categoria | "FINANCEIRO" |
| unit | VARCHAR(MAX) | NULL | Unidade de medida | "R$" |
| aggregation | VARCHAR(MAX) | NULL | Método agregação | "SUM" |
| formula | VARCHAR(MAX) | NULL | Fórmula SQL | "SELECT SUM(valor)..." |
| is_inverted | VARCHAR(MAX) | NULL | Se invertido | "TRUE" ou "FALSE" |
| is_active | VARCHAR(MAX) | NULL | Se ativo | "TRUE" ou "FALSE" |
| description | VARCHAR(MAX) | NULL | Descrição | "Soma de aplicações..." |
| created_date | VARCHAR(MAX) | NULL | Data criação | "2024-01-15" ou "15/01/2024" |
| notes | VARCHAR(MAX) | NULL | Observações | "Ajustado em jan/24" |
| **Metadados de Controle** |
| row_number | INT | NULL | Linha na planilha | 5 |
| row_hash | VARCHAR(32) | NULL | Hash MD5 do registro | "a1b2c3d4..." |
| is_processed | BIT | DEFAULT 0 | Se já foi processado | 0 |
| processing_date | DATETIME | NULL | Quando foi processado | NULL |
| processing_status | VARCHAR(50) | NULL | Status processamento | NULL, 'SUCCESS', 'ERROR' |
| processing_notes | VARCHAR(MAX) | NULL | Notas/erros | NULL |

**Índices**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_bronze_indicators | CLUSTERED | load_id | Chave primária |
| IX_bronze_indicators_unprocessed | FILTERED | load_timestamp | WHERE is_processed = 0 |

**Estrutura DDL**:
```sql
CREATE TABLE bronze.performance_indicators (
    -- Controle ETL
    load_id INT IDENTITY(1,1) NOT NULL,
    load_timestamp DATETIME NOT NULL DEFAULT GETDATE(),
    load_source VARCHAR(200) NOT NULL DEFAULT 'GoogleSheets:m7_performance_indicators',
    
    -- Campos da planilha (permissivos)
    indicator_code VARCHAR(MAX) NULL,
    indicator_name VARCHAR(MAX) NULL,
    category VARCHAR(MAX) NULL,
    unit VARCHAR(MAX) NULL,
    aggregation VARCHAR(MAX) NULL,
    formula VARCHAR(MAX) NULL,
    is_inverted VARCHAR(MAX) NULL,
    is_active VARCHAR(MAX) NULL,
    description VARCHAR(MAX) NULL,
    created_date VARCHAR(MAX) NULL,
    notes VARCHAR(MAX) NULL,
    
    -- Metadados de controle
    row_number INT NULL,
    row_hash VARCHAR(32) NULL,
    is_processed BIT NOT NULL DEFAULT 0,
    processing_date DATETIME NULL,
    processing_status VARCHAR(50) NULL,
    processing_notes VARCHAR(MAX) NULL,
    
    CONSTRAINT PK_bronze_indicators PRIMARY KEY CLUSTERED (load_id)
);

-- Índice para processamento
CREATE INDEX IX_bronze_indicators_unprocessed 
ON bronze.performance_indicators (load_timestamp)
WHERE is_processed = 0;
```

### 4.2 Tabela: bronze.performance_assignments

**Descrição**: Tabela staging para receber dados brutos de atribuições de indicadores por assessor vindos do Google Sheets.

| Campo | Tipo | Constraint | Descrição | Exemplo |
|-------|------|------------|-----------|---------|
| **Campos de Controle ETL** |
| load_id | INT | PK, IDENTITY | ID único da carga | 1 |
| load_timestamp | DATETIME | NOT NULL, DEFAULT | Momento da carga | 2025-01-16 11:00:00 |
| load_source | VARCHAR(200) | NOT NULL, DEFAULT | Origem dos dados | 'GoogleSheets:m7_performance_assignments' |
| **Campos da Planilha (todos VARCHAR(MAX))** |
| cod_assessor | VARCHAR(MAX) | NULL | Código assessor | "AAI001" |
| nome_assessor | VARCHAR(MAX) | NULL | Nome assessor | "João Silva" |
| indicator_code | VARCHAR(MAX) | NULL | Código indicador | "CAPTACAO_LIQUIDA" |
| indicator_type | VARCHAR(MAX) | NULL | Tipo indicador | "CARD" |
| weight | VARCHAR(MAX) | NULL | Peso do indicador | "25.00" ou "25" |
| valid_from | VARCHAR(MAX) | NULL | Início vigência | "2024-01-01" ou "01/01/2024" |
| valid_to | VARCHAR(MAX) | NULL | Fim vigência | NULL ou data |
| created_by | VARCHAR(MAX) | NULL | Criado por | "gestor@m7.com" |
| approved_by | VARCHAR(MAX) | NULL | Aprovado por | "diretor@m7.com" |
| comments | VARCHAR(MAX) | NULL | Comentários | "Ajuste trimestral" |
| **Metadados de Controle** |
| row_number | INT | NULL | Linha na planilha | 10 |
| row_hash | VARCHAR(32) | NULL | Hash do registro | "e5f6g7h8..." |
| is_current | BIT | NULL | Se vigente | 1 |
| is_processed | BIT | DEFAULT 0 | Se processado | 0 |
| processing_date | DATETIME | NULL | Data processamento | NULL |
| processing_status | VARCHAR(50) | NULL | Status | NULL, 'SUCCESS', 'ERROR' |
| processing_notes | VARCHAR(MAX) | NULL | Notas/erros | NULL |
| **Validações** |
| weight_sum_valid | BIT | NULL | Se soma=100% | 1 |
| indicator_exists | BIT | NULL | Se indicador existe | 1 |
| validation_errors | VARCHAR(MAX) | NULL | Erros em JSON | '{"error": "peso invalido"}' |

**Índices**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_bronze_assignments | CLUSTERED | load_id | Chave primária |
| IX_bronze_assignments_lookup | NONCLUSTERED | cod_assessor, indicator_code, valid_from | Busca rápida |
| IX_bronze_assignments_unprocessed | FILTERED | load_timestamp | WHERE is_processed = 0 |

**Estrutura DDL**:
```sql
CREATE TABLE bronze.performance_assignments (
    -- Controle ETL
    load_id INT IDENTITY(1,1) NOT NULL,
    load_timestamp DATETIME NOT NULL DEFAULT GETDATE(),
    load_source VARCHAR(200) NOT NULL DEFAULT 'GoogleSheets:m7_performance_assignments',
    
    -- Campos da planilha
    cod_assessor VARCHAR(MAX) NULL,
    nome_assessor VARCHAR(MAX) NULL,
    indicator_code VARCHAR(MAX) NULL,
    indicator_type VARCHAR(MAX) NULL,
    weight VARCHAR(MAX) NULL,
    valid_from VARCHAR(MAX) NULL,
    valid_to VARCHAR(MAX) NULL,
    created_by VARCHAR(MAX) NULL,
    approved_by VARCHAR(MAX) NULL,
    comments VARCHAR(MAX) NULL,
    
    -- Metadados de controle
    row_number INT NULL,
    row_hash VARCHAR(32) NULL,
    is_current BIT NULL,
    is_processed BIT NOT NULL DEFAULT 0,
    processing_date DATETIME NULL,
    processing_status VARCHAR(50) NULL,
    processing_notes VARCHAR(MAX) NULL,
    
    -- Validações
    weight_sum_valid BIT NULL,
    indicator_exists BIT NULL,
    validation_errors VARCHAR(MAX) NULL,
    
    CONSTRAINT PK_bronze_assignments PRIMARY KEY CLUSTERED (load_id)
);

-- Índices para performance
CREATE INDEX IX_bronze_assignments_lookup 
ON bronze.performance_assignments (cod_assessor, indicator_code, valid_from)
WHERE is_processed = 0;

CREATE INDEX IX_bronze_assignments_unprocessed 
ON bronze.performance_assignments (load_timestamp)
WHERE is_processed = 0;
```

### 4.3 Tabela: bronze.performance_targets

**Descrição**: Tabela staging para receber dados brutos de metas mensais vindos do Google Sheets. Projetada para alto volume (2500+ registros).

| Campo | Tipo | Constraint | Descrição | Exemplo |
|-------|------|------------|-----------|---------|
| **Campos de Controle ETL** |
| load_id | INT | PK, IDENTITY | ID único da carga | 1 |
| load_timestamp | DATETIME | NOT NULL, DEFAULT | Momento da carga | 2025-01-16 12:00:00 |
| load_source | VARCHAR(200) | NOT NULL, DEFAULT | Origem | 'GoogleSheets:m7_performance_targets' |
| **Campos da Planilha (todos VARCHAR(MAX))** |
| cod_assessor | VARCHAR(MAX) | NULL | Código assessor | "AAI001" |
| nome_assessor | VARCHAR(MAX) | NULL | Nome assessor | "João Silva" |
| indicator_code | VARCHAR(MAX) | NULL | Código indicador | "CAPTACAO_LIQUIDA" |
| period_type | VARCHAR(MAX) | NULL | Tipo período | "MENSAL" |
| period_start | VARCHAR(MAX) | NULL | Início período | "2024-01-01" |
| period_end | VARCHAR(MAX) | NULL | Fim período | "2024-01-31" |
| target_value | VARCHAR(MAX) | NULL | Valor meta | "1000000.00" |
| stretch_value | VARCHAR(MAX) | NULL | Meta stretch | "1200000.00" |
| minimum_value | VARCHAR(MAX) | NULL | Meta mínima | "800000.00" |
| **Metadados de Controle** |
| row_number | INT | NULL | Linha planilha | 100 |
| row_hash | VARCHAR(32) | NULL | Hash registro | "i9j0k1l2..." |
| target_year | INT | NULL | Ano da meta | 2024 |
| target_quarter | INT | NULL | Trimestre | 1 |
| is_processed | BIT | DEFAULT 0 | Se processado | 0 |
| processing_date | DATETIME | NULL | Data proc | NULL |
| processing_status | VARCHAR(50) | NULL | Status | NULL |
| processing_notes | VARCHAR(MAX) | NULL | Notas | NULL |
| **Validações** |
| target_logic_valid | BIT | NULL | Lógica válida | 1 |
| is_inverted | BIT | NULL | Se invertido | 0 |
| validation_errors | VARCHAR(MAX) | NULL | Erros JSON | NULL |

**Índices otimizados para volume**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_bronze_targets | CLUSTERED | load_id | Chave primária |
| IX_bronze_targets_year | NONCLUSTERED | target_year, cod_assessor, indicator_code | Busca por ano |
| IX_bronze_targets_lookup | NONCLUSTERED | cod_assessor, indicator_code, period_start | Busca específica |
| IX_bronze_targets_unprocessed | FILTERED | target_year, load_timestamp | WHERE is_processed = 0 |

**Estrutura DDL**:
```sql
CREATE TABLE bronze.performance_targets (
    -- Controle ETL
    load_id INT IDENTITY(1,1) NOT NULL,
    load_timestamp DATETIME NOT NULL DEFAULT GETDATE(),
    load_source VARCHAR(200) NOT NULL DEFAULT 'GoogleSheets:m7_performance_targets',
    
    -- Campos da planilha
    cod_assessor VARCHAR(MAX) NULL,
    nome_assessor VARCHAR(MAX) NULL,
    indicator_code VARCHAR(MAX) NULL,
    period_type VARCHAR(MAX) NULL,
    period_start VARCHAR(MAX) NULL,
    period_end VARCHAR(MAX) NULL,
    target_value VARCHAR(MAX) NULL,
    stretch_value VARCHAR(MAX) NULL,
    minimum_value VARCHAR(MAX) NULL,
    
    -- Metadados de controle
    row_number INT NULL,
    row_hash VARCHAR(32) NULL,
    target_year INT NULL,
    target_quarter INT NULL,
    is_processed BIT NOT NULL DEFAULT 0,
    processing_date DATETIME NULL,
    processing_status VARCHAR(50) NULL,
    processing_notes VARCHAR(MAX) NULL,
    
    -- Validações
    target_logic_valid BIT NULL,
    is_inverted BIT NULL,
    validation_errors VARCHAR(MAX) NULL,
    
    CONSTRAINT PK_bronze_targets PRIMARY KEY CLUSTERED (load_id)
);

-- Índices para performance com volume alto
CREATE INDEX IX_bronze_targets_year 
ON bronze.performance_targets (target_year, cod_assessor, indicator_code)
WHERE is_processed = 0;

CREATE INDEX IX_bronze_targets_lookup
ON bronze.performance_targets (cod_assessor, indicator_code, period_start)
INCLUDE (target_value, stretch_value, minimum_value);

CREATE INDEX IX_bronze_targets_unprocessed
ON bronze.performance_targets (target_year, load_timestamp)
WHERE is_processed = 0;
```

### 4.4 Tabela: bronze.etl_control

**Descrição**: Tabela de controle e auditoria das execuções de ETL no schema Bronze.

| Campo | Tipo | Constraint | Descrição | Exemplo |
|-------|------|------------|-----------|---------|
| execution_id | INT | PK, IDENTITY | ID da execução | 1 |
| etl_name | VARCHAR(100) | NOT NULL | Nome do ETL | 'ETL-001-indicators' |
| execution_start | DATETIME | NOT NULL | Início execução | 2025-01-16 10:00:00 |
| execution_end | DATETIME | NULL | Fim execução | 2025-01-16 10:01:30 |
| execution_status | VARCHAR(50) | NOT NULL | Status final | 'SUCCESS', 'ERROR', 'RUNNING' |
| source_system | VARCHAR(200) | NOT NULL | Sistema origem | 'GoogleSheets' |
| source_details | VARCHAR(MAX) | NULL | Detalhes origem | '{"spreadsheet_id": "1WUN..."}' |
| target_table | VARCHAR(200) | NOT NULL | Tabela destino | 'bronze.performance_indicators' |
| records_read | INT | NULL | Registros lidos | 50 |
| records_written | INT | NULL | Registros gravados | 48 |
| records_error | INT | NULL | Registros com erro | 2 |
| error_message | VARCHAR(MAX) | NULL | Mensagem erro | NULL |
| execution_log | VARCHAR(MAX) | NULL | Log completo | '{"steps": [...]}' |
| created_by | VARCHAR(100) | NOT NULL | Usuário/sistema | 'ETL_SERVICE' |

**Estrutura DDL**:
```sql
CREATE TABLE bronze.etl_control (
    execution_id INT IDENTITY(1,1) NOT NULL,
    etl_name VARCHAR(100) NOT NULL,
    execution_start DATETIME NOT NULL,
    execution_end DATETIME NULL,
    execution_status VARCHAR(50) NOT NULL,
    source_system VARCHAR(200) NOT NULL,
    source_details VARCHAR(MAX) NULL,
    target_table VARCHAR(200) NOT NULL,
    records_read INT NULL,
    records_written INT NULL,
    records_error INT NULL,
    error_message VARCHAR(MAX) NULL,
    execution_log VARCHAR(MAX) NULL,
    created_by VARCHAR(100) NOT NULL DEFAULT SYSTEM_USER,
    
    CONSTRAINT PK_etl_control PRIMARY KEY CLUSTERED (execution_id),
    CONSTRAINT CHK_execution_status CHECK (execution_status IN ('RUNNING', 'SUCCESS', 'ERROR', 'WARNING'))
);

-- Índices para consultas
CREATE INDEX IX_etl_control_status 
ON bronze.etl_control (execution_status, execution_start DESC);

CREATE INDEX IX_etl_control_table 
ON bronze.etl_control (target_table, execution_start DESC);
```

## 5. Views de Monitoramento

### 5.1 View: bronze.vw_pending_processing

**Descrição**: Visão consolidada de todos os registros pendentes de processamento.

```sql
CREATE VIEW bronze.vw_pending_processing AS
-- Indicadores pendentes
SELECT 
    'indicators' as table_name,
    load_id,
    load_timestamp,
    indicator_code as primary_key,
    processing_status,
    validation_errors
FROM bronze.performance_indicators
WHERE is_processed = 0

UNION ALL

-- Assignments pendentes
SELECT 
    'assignments' as table_name,
    load_id,
    load_timestamp,
    cod_assessor + '_' + indicator_code as primary_key,
    processing_status,
    validation_errors
FROM bronze.performance_assignments
WHERE is_processed = 0

UNION ALL

-- Targets pendentes
SELECT 
    'targets' as table_name,
    load_id,
    load_timestamp,
    cod_assessor + '_' + indicator_code + '_' + period_start as primary_key,
    processing_status,
    validation_errors
FROM bronze.performance_targets
WHERE is_processed = 0;
```

### 5.2 View: bronze.vw_etl_status

**Descrição**: Status das últimas execuções de ETL.

```sql
CREATE VIEW bronze.vw_etl_status AS
WITH last_executions AS (
    SELECT 
        etl_name,
        MAX(execution_start) as last_execution
    FROM bronze.etl_control
    GROUP BY etl_name
)
SELECT 
    e.etl_name,
    e.execution_start,
    e.execution_end,
    e.execution_status,
    e.records_written,
    e.records_error,
    DATEDIFF(SECOND, e.execution_start, e.execution_end) as duration_seconds,
    CASE 
        WHEN e.execution_status = 'ERROR' THEN 'CRITICAL'
        WHEN e.records_error > 0 THEN 'WARNING'
        WHEN DATEDIFF(HOUR, e.execution_start, GETDATE()) > 24 THEN 'STALE'
        ELSE 'OK'
    END as health_status
FROM bronze.etl_control e
JOIN last_executions le 
    ON e.etl_name = le.etl_name 
    AND e.execution_start = le.last_execution;
```

## 6. Procedimentos de Manutenção

### 6.1 Procedure: bronze.prc_cleanup_processed

**Descrição**: Limpa registros já processados com mais de X dias.

```sql
CREATE PROCEDURE bronze.prc_cleanup_processed
    @days_to_keep INT = 30,
    @batch_size INT = 1000
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @deleted_total INT = 0;
    DECLARE @deleted_batch INT;
    
    -- Limpar indicators
    WHILE 1 = 1
    BEGIN
        DELETE TOP (@batch_size)
        FROM bronze.performance_indicators
        WHERE is_processed = 1
          AND processing_date < DATEADD(DAY, -@days_to_keep, GETDATE());
        
        SET @deleted_batch = @@ROWCOUNT;
        SET @deleted_total += @deleted_batch;
        
        IF @deleted_batch < @batch_size BREAK;
    END
    
    -- Repetir para assignments e targets...
    
    -- Registrar limpeza
    INSERT INTO bronze.etl_control (
        etl_name, execution_start, execution_end, execution_status,
        source_system, target_table, records_written
    )
    VALUES (
        'CLEANUP_PROCESSED',
        GETDATE(),
        GETDATE(),
        'SUCCESS',
        'INTERNAL',
        'bronze.*',
        @deleted_total
    );
    
    RETURN @deleted_total;
END;
```

### 6.2 Procedure: bronze.prc_reset_failed_processing

**Descrição**: Reseta registros com erro para reprocessamento.

```sql
CREATE PROCEDURE bronze.prc_reset_failed_processing
    @table_name VARCHAR(100) = NULL,
    @error_pattern VARCHAR(200) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Reset indicators
    IF @table_name IS NULL OR @table_name = 'indicators'
    BEGIN
        UPDATE bronze.performance_indicators
        SET is_processed = 0,
            processing_status = NULL,
            processing_notes = 'Reset for reprocessing'
        WHERE processing_status = 'ERROR'
          AND (@error_pattern IS NULL OR processing_notes LIKE '%' + @error_pattern + '%');
    END
    
    -- Reset assignments
    IF @table_name IS NULL OR @table_name = 'assignments'
    BEGIN
        UPDATE bronze.performance_assignments
        SET is_processed = 0,
            processing_status = NULL,
            processing_notes = 'Reset for reprocessing'
        WHERE processing_status = 'ERROR'
          AND (@error_pattern IS NULL OR processing_notes LIKE '%' + @error_pattern + '%');
    END
    
    -- Reset targets
    IF @table_name IS NULL OR @table_name = 'targets'
    BEGIN
        UPDATE bronze.performance_targets
        SET is_processed = 0,
            processing_status = NULL,
            processing_notes = 'Reset for reprocessing'
        WHERE processing_status = 'ERROR'
          AND (@error_pattern IS NULL OR processing_notes LIKE '%' + @error_pattern + '%');
    END
END;
```

## 7. Monitoramento e Alertas

### 7.1 Queries de Monitoramento

```sql
-- Verificar registros não processados há mais de 1 hora
SELECT 
    table_name,
    COUNT(*) as pending_count,
    MIN(load_timestamp) as oldest_pending,
    DATEDIFF(MINUTE, MIN(load_timestamp), GETDATE()) as minutes_waiting
FROM bronze.vw_pending_processing
GROUP BY table_name
HAVING DATEDIFF(MINUTE, MIN(load_timestamp), GETDATE()) > 60;

-- Verificar taxa de erro por tabela
SELECT 
    target_table,
    COUNT(*) as total_loads,
    SUM(CASE WHEN execution_status = 'ERROR' THEN 1 ELSE 0 END) as error_count,
    CAST(SUM(CASE WHEN execution_status = 'ERROR' THEN 1 ELSE 0 END) AS FLOAT) / 
        COUNT(*) * 100 as error_rate
FROM bronze.etl_control
WHERE execution_start >= DATEADD(DAY, -7, GETDATE())
GROUP BY target_table
ORDER BY error_rate DESC;
```

### 7.2 Alertas Sugeridos

| Alerta | Condição | Ação |
|--------|----------|------|
| Processamento atrasado | Registros pendentes > 1 hora | Verificar ETL travado |
| Taxa de erro alta | > 10% de erro em 24h | Investigar fonte dados |
| Volume anormal | Variação > 50% do esperado | Validar completude |
| Duplicatas detectadas | Row hash repetido | Verificar extração |

## 8. Considerações de Performance

### 8.1 Estratégias para Alto Volume

```sql
-- Estatísticas para tabela de targets (alto volume)
UPDATE STATISTICS bronze.performance_targets WITH FULLSCAN;

-- Compressão de página para targets
ALTER TABLE bronze.performance_targets 
REBUILD WITH (DATA_COMPRESSION = PAGE);

-- Particionamento mensal (opcional para > 10M registros)
-- Criar função e esquema de partição por target_year
```

### 8.2 Manutenção de Índices

```sql
-- Script de manutenção semanal
ALTER INDEX ALL ON bronze.performance_indicators REORGANIZE;
ALTER INDEX ALL ON bronze.performance_assignments REORGANIZE;
ALTER INDEX ALL ON bronze.performance_targets REORGANIZE;

-- Rebuild mensal para fragmentação > 30%
SELECT 
    OBJECT_NAME(object_id) as table_name,
    name as index_name,
    avg_fragmentation_in_percent
FROM sys.dm_db_index_physical_stats(
    DB_ID('M7Medallion'), 
    OBJECT_ID('bronze.performance_targets'), 
    NULL, NULL, 'DETAILED'
)
WHERE avg_fragmentation_in_percent > 30;
```

## 9. Segurança

### 9.1 Permissões Recomendadas

```sql
-- Role para ETL
CREATE ROLE db_bronze_etl_writer;
GRANT INSERT, UPDATE ON SCHEMA::bronze TO db_bronze_etl_writer;
GRANT EXECUTE ON SCHEMA::bronze TO db_bronze_etl_writer;

-- Role para leitura/monitoramento
CREATE ROLE db_bronze_reader;
GRANT SELECT ON SCHEMA::bronze TO db_bronze_reader;

-- Role para administração
CREATE ROLE db_bronze_admin;
GRANT CONTROL ON SCHEMA::bronze TO db_bronze_admin;
```

### 9.2 Mascaramento de Dados

```sql
-- Mascarar nomes em ambientes não-produção
CREATE FUNCTION bronze.fn_mask_nome(@nome VARCHAR(MAX))
RETURNS VARCHAR(MAX)
AS
BEGIN
    IF DB_NAME() != 'M7Medallion_PROD'
        RETURN LEFT(@nome, 3) + REPLICATE('*', LEN(@nome) - 3)
    RETURN @nome
END;
```

## 10. Troubleshooting

### 10.1 Problemas Comuns

| Problema | Diagnóstico | Solução |
|----------|-------------|---------|
| Carga duplicada | Row hash repetido | Verificar execução ETL |
| Processamento travado | is_processed = 0 antigo | Reset e reprocessar |
| Validação falhando | validation_errors preenchido | Analisar JSON de erros |
| Performance degradada | Queries lentas | Rebuild índices |

### 10.2 Scripts de Diagnóstico

```sql
-- Identificar cargas duplicadas
WITH duplicates AS (
    SELECT 
        row_hash,
        COUNT(*) as duplicate_count,
        STRING_AGG(CAST(load_id AS VARCHAR), ', ') as load_ids
    FROM bronze.performance_indicators
    WHERE is_processed = 0
    GROUP BY row_hash
    HAVING COUNT(*) > 1
)
SELECT * FROM duplicates;

-- Analisar erros de validação
SELECT 
    JSON_VALUE(validation_errors, '$.error_type') as error_type,
    COUNT(*) as error_count,
    JSON_VALUE(validation_errors, '$.message') as sample_message
FROM bronze.performance_assignments
WHERE validation_errors IS NOT NULL
GROUP BY 
    JSON_VALUE(validation_errors, '$.error_type'),
    JSON_VALUE(validation_errors, '$.message')
ORDER BY error_count DESC;
```

## 11. Referências

- [MOD-001 - Modelo Performance Tracking (Metadados/Platinum)]
- [MOD-003 - Transformação Bronze → Metadados]
- [ETL-001 - Extração Performance Indicators]
- [ETL-002 - Extração Performance Assignments]
- [ETL-003 - Extração Performance Targets]
- [ARQ-002 - Arquitetura Medallion M7]

---

**Documento criado por**: Arquitetura de Dados M7  
**Data**: 2025-01-16  
**Próxima revisão**: 2025-04-16