# MOD-003-transformacao-bronze-metadados

---
título: Transformação Bronze para Metadados - Sistema Performance Tracking
tipo: MOD - Modelo de Dados
versão: 1.0.0
última_atualização: 2025-01-16
autor: arquitetura.dados@m7investimentos.com.br
aprovador: diretoria.ti@m7investimentos.com.br
tags: [modelo, transformação, bronze, metadados, etl, medallion]
status: aprovado
dependências:
  - tipo: modelo
    ref: [MOD-001, MOD-002]
    repo: datawarehouse-docs
  - tipo: etl
    ref: [ETL-001, ETL-002, ETL-003]
    repo: datawarehouse-docs
---

## 1. Objetivo

Documentar as regras de transformação, validação e processamento dos dados da camada Bronze (staging) para a camada de Metadados (dados validados e estruturados) do sistema de Performance Tracking. Este documento define como dados não estruturados são limpos, validados, transformados e carregados nas tabelas finais.

## 2. Escopo e Contexto

### 2.1 Domínio de Negócio
- **Área**: ETL/Transformação de Dados
- **Processos suportados**: 
  - Limpeza e padronização de dados
  - Validação de regras de negócio
  - Transformação de tipos de dados
  - Detecção e tratamento de anomalias
  - Merge incremental de dados
- **Stakeholders**: 
  - Equipe de Dados
  - Administradores do Sistema

### 2.2 Contexto Técnico
- **Tipo**: Transformação ETL (Extract-Transform-Load)
- **Origem**: Schema bronze (staging)
- **Destino**: Schema metadados
- **Tecnologia**: SQL Server Stored Procedures + SSIS (opcional)
- **Frequência**: Sob demanda ou agendada

## 3. Visão Geral do Processo

### 3.1 Fluxo de Transformação

```
BRONZE (Staging)                 TRANSFORMAÇÃO                    METADADOS (Validado)
================                 ==============                   ====================

┌─────────────────┐             ┌─────────────────┐             ┌─────────────────┐
│ bronze.         │             │ Validações:     │             │ metadados.      │
│ performance_    │────────────>│ - Tipos dados   │────────────>│ performance_    │
│ indicators      │             │ - Valores null  │             │ indicators      │
│                 │             │ - Duplicatas    │             │                 │
│ VARCHAR(MAX)    │             │ - Referências   │             │ Tipos corretos  │
└─────────────────┘             └─────────────────┘             └─────────────────┘
         │                               │                                │
         ▼                               ▼                                ▼
┌─────────────────┐             ┌─────────────────┐             ┌─────────────────┐
│ Registros com   │             │ Quarentena:     │             │ Audit Trail:    │
│ is_processed=0  │             │ - Log erros     │             │ - Mudanças      │
│                 │             │ - Notificações  │             │ - Histórico     │
└─────────────────┘             └─────────────────┘             └─────────────────┘
```

### 3.2 Princípios de Transformação

| Princípio | Descrição | Implementação |
|-----------|-----------|---------------|
| **Idempotência** | Executar N vezes produz mesmo resultado | MERGE com chaves únicas |
| **Atomicidade** | Tudo ou nada | Transações com ROLLBACK |
| **Rastreabilidade** | Auditoria completa | Log de todas as operações |
| **Tolerância a falhas** | Continuar com erros parciais | Quarentena + notificação |
| **Performance** | Otimizado para volume | Batch processing |

## 4. Transformações por Entidade

### 4.1 Transformação: performance_indicators

**Origem**: bronze.performance_indicators  
**Destino**: metadados.performance_indicators

#### 4.1.1 Mapeamento de Campos

| Campo Bronze | Tipo Bronze | Campo Metadados | Tipo Metadados | Transformação |
|--------------|-------------|-----------------|----------------|---------------|
| indicator_code | VARCHAR(MAX) | indicator_code | VARCHAR(50) | UPPER, TRIM, REPLACE(' ', '_') |
| indicator_name | VARCHAR(MAX) | indicator_name | VARCHAR(200) | TRIM, validar NOT NULL |
| category | VARCHAR(MAX) | category | VARCHAR(50) | UPPER, validar lista |
| unit | VARCHAR(MAX) | unit | VARCHAR(20) | UPPER, validar lista |
| aggregation | VARCHAR(MAX) | aggregation_method | VARCHAR(20) | UPPER, default 'CUSTOM' |
| formula | VARCHAR(MAX) | formula | VARCHAR(MAX) | Validar sintaxe SQL |
| is_inverted | VARCHAR(MAX) | is_inverted | BIT | Converter TRUE/FALSE → 1/0 |
| is_active | VARCHAR(MAX) | is_active | BIT | Converter, default 1 |
| description | VARCHAR(MAX) | description | VARCHAR(MAX) | TRIM |
| created_date | VARCHAR(MAX) | created_date | DATETIME | Parse múltiplos formatos |
| notes | VARCHAR(MAX) | - | - | Adicionar em description |
| - | - | created_by | VARCHAR(100) | Default 'ETL_IMPORT' |
| - | - | modified_date | DATETIME | NULL inicialmente |
| - | - | modified_by | VARCHAR(100) | NULL inicialmente |

#### 4.1.2 Validações

```sql
-- Validações aplicadas durante transformação
CREATE FUNCTION bronze.fn_validate_indicator(@indicator_code VARCHAR(MAX))
RETURNS TABLE
AS
RETURN
SELECT 
    CASE 
        WHEN @indicator_code IS NULL OR LTRIM(RTRIM(@indicator_code)) = '' 
        THEN 'ERROR: Código do indicador não pode ser vazio'
        
        WHEN LEN(@indicator_code) > 50 
        THEN 'ERROR: Código do indicador muito longo (max 50)'
        
        WHEN @indicator_code LIKE '% %' 
        THEN 'WARNING: Código contém espaços, serão substituídos por _'
        
        WHEN @indicator_code != UPPER(@indicator_code) 
        THEN 'WARNING: Código será convertido para maiúsculas'
        
        ELSE 'OK'
    END as validation_result;
```

#### 4.1.3 Procedure de Transformação

```sql
CREATE PROCEDURE bronze.prc_process_indicators_to_metadata
    @batch_size INT = 100,
    @force_reprocess BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    BEGIN TRANSACTION;
    
    BEGIN TRY
        -- 1. Identificar registros para processar
        CREATE TABLE #to_process (
            load_id INT PRIMARY KEY,
            indicator_code_clean VARCHAR(50),
            validation_status VARCHAR(20)
        );
        
        INSERT INTO #to_process
        SELECT TOP (@batch_size)
            load_id,
            UPPER(REPLACE(LTRIM(RTRIM(indicator_code)), ' ', '_')) as indicator_code_clean,
            CASE 
                WHEN indicator_code IS NULL OR LTRIM(RTRIM(indicator_code)) = '' THEN 'ERROR'
                WHEN LEN(indicator_code) > 50 THEN 'ERROR'
                ELSE 'OK'
            END as validation_status
        FROM bronze.performance_indicators
        WHERE is_processed = 0 OR @force_reprocess = 1
        ORDER BY load_id;
        
        -- 2. Processar registros válidos
        MERGE metadados.performance_indicators AS target
        USING (
            SELECT 
                b.load_id,
                tp.indicator_code_clean as indicator_code,
                LTRIM(RTRIM(b.indicator_name)) as indicator_name,
                UPPER(LTRIM(RTRIM(b.category))) as category,
                UPPER(LTRIM(RTRIM(b.unit))) as unit,
                ISNULL(UPPER(LTRIM(RTRIM(b.aggregation))), 'CUSTOM') as aggregation_method,
                b.formula,
                CASE 
                    WHEN UPPER(b.is_inverted) IN ('TRUE', '1', 'SIM', 'S', 'YES', 'Y') THEN 1
                    ELSE 0
                END as is_inverted,
                CASE 
                    WHEN UPPER(b.is_active) IN ('FALSE', '0', 'NAO', 'N', 'NO') THEN 0
                    ELSE 1
                END as is_active,
                ISNULL(LTRIM(RTRIM(b.description)), '') + 
                    CASE WHEN b.notes IS NOT NULL 
                         THEN CHAR(13) + CHAR(10) + '--- Notes: ' + b.notes 
                         ELSE '' 
                    END as description,
                TRY_CONVERT(DATETIME, b.created_date, 103) as created_date -- DD/MM/YYYY
            FROM bronze.performance_indicators b
            INNER JOIN #to_process tp ON b.load_id = tp.load_id
            WHERE tp.validation_status = 'OK'
        ) AS source
        ON target.indicator_code = source.indicator_code
        
        WHEN MATCHED THEN
            UPDATE SET
                indicator_name = source.indicator_name,
                category = source.category,
                unit = source.unit,
                aggregation_method = source.aggregation_method,
                formula = source.formula,
                is_inverted = source.is_inverted,
                is_active = source.is_active,
                description = source.description,
                modified_date = GETDATE(),
                modified_by = 'ETL_UPDATE'
                
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (indicator_code, indicator_name, category, unit, aggregation_method,
                   formula, is_inverted, is_active, description, created_date, created_by)
            VALUES (source.indicator_code, source.indicator_name, source.category, 
                   source.unit, source.aggregation_method, source.formula,
                   source.is_inverted, source.is_active, source.description,
                   ISNULL(source.created_date, GETDATE()), 'ETL_IMPORT');
        
        -- 3. Marcar como processados
        UPDATE b
        SET is_processed = 1,
            processing_date = GETDATE(),
            processing_status = tp.validation_status,
            processing_notes = CASE 
                WHEN tp.validation_status = 'ERROR' THEN 'Validation failed'
                ELSE 'Processed successfully'
            END
        FROM bronze.performance_indicators b
        INNER JOIN #to_process tp ON b.load_id = tp.load_id;
        
        -- 4. Log de auditoria
        INSERT INTO bronze.etl_control (
            etl_name, execution_start, execution_end, execution_status,
            source_system, target_table, records_read, records_written, records_error
        )
        SELECT 
            'TRANSFORM_INDICATORS',
            GETDATE(),
            GETDATE(),
            'SUCCESS',
            'bronze.performance_indicators',
            'metadados.performance_indicators',
            COUNT(*),
            SUM(CASE WHEN validation_status = 'OK' THEN 1 ELSE 0 END),
            SUM(CASE WHEN validation_status = 'ERROR' THEN 1 ELSE 0 END)
        FROM #to_process;
        
        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        
        -- Marcar batch com erro
        UPDATE bronze.performance_indicators
        SET processing_status = 'ERROR',
            processing_notes = ERROR_MESSAGE()
        WHERE load_id IN (SELECT load_id FROM #to_process);
        
        -- Re-throw error
        THROW;
    END CATCH
END;
```

### 4.2 Transformação: performance_assignments

**Origem**: bronze.performance_assignments  
**Destino**: metadados.performance_assignments

#### 4.2.1 Validações Complexas

```sql
-- Validação de soma de pesos por assessor
CREATE FUNCTION bronze.fn_validate_assignment_weights(@cod_assessor VARCHAR(20), @valid_from DATE)
RETURNS @result TABLE (
    is_valid BIT,
    total_weight DECIMAL(5,2),
    message VARCHAR(500)
)
AS
BEGIN
    DECLARE @total DECIMAL(5,2);
    
    SELECT @total = SUM(TRY_CAST(weight AS DECIMAL(5,2)))
    FROM bronze.performance_assignments
    WHERE cod_assessor = @cod_assessor
      AND TRY_CAST(valid_from AS DATE) = @valid_from
      AND indicator_type = 'CARD'
      AND is_processed = 0;
    
    INSERT INTO @result
    SELECT 
        CASE WHEN ABS(@total - 100.0) < 0.01 THEN 1 ELSE 0 END,
        @total,
        CASE 
            WHEN @total IS NULL THEN 'Erro ao converter pesos para numérico'
            WHEN ABS(@total - 100.0) < 0.01 THEN 'Soma dos pesos OK'
            ELSE CONCAT('Soma dos pesos = ', @total, ', esperado 100.00')
        END;
    
    RETURN;
END;
```

#### 4.2.2 Procedure de Transformação

```sql
CREATE PROCEDURE bronze.prc_process_assignments_to_metadata
    @validate_weights BIT = 1,
    @validate_references BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    BEGIN TRANSACTION;
    
    BEGIN TRY
        -- 1. Criar tabela temporária com validações
        CREATE TABLE #assignments_validated (
            load_id INT PRIMARY KEY,
            cod_assessor VARCHAR(20),
            indicator_code VARCHAR(50),
            indicator_id INT,
            indicator_weight DECIMAL(5,2),
            indicator_type VARCHAR(20),
            valid_from DATE,
            valid_to DATE,
            weight_validation VARCHAR(100),
            reference_validation VARCHAR(100),
            can_process BIT
        );
        
        -- 2. Validar e preparar dados
        INSERT INTO #assignments_validated
        SELECT 
            a.load_id,
            UPPER(LTRIM(RTRIM(a.cod_assessor))) as cod_assessor,
            UPPER(REPLACE(LTRIM(RTRIM(a.indicator_code)), ' ', '_')) as indicator_code,
            i.indicator_id,
            CASE 
                WHEN a.indicator_type = 'CARD' THEN TRY_CAST(a.weight AS DECIMAL(5,2))
                ELSE 0.00
            END as indicator_weight,
            UPPER(LTRIM(RTRIM(a.indicator_type))) as indicator_type,
            TRY_CAST(a.valid_from AS DATE) as valid_from,
            TRY_CAST(a.valid_to AS DATE) as valid_to,
            'PENDING' as weight_validation,
            CASE 
                WHEN i.indicator_id IS NULL THEN 'Indicador não encontrado'
                ELSE 'OK'
            END as reference_validation,
            CASE 
                WHEN i.indicator_id IS NULL THEN 0
                WHEN TRY_CAST(a.valid_from AS DATE) IS NULL THEN 0
                ELSE 1
            END as can_process
        FROM bronze.performance_assignments a
        LEFT JOIN metadados.performance_indicators i 
            ON UPPER(REPLACE(LTRIM(RTRIM(a.indicator_code)), ' ', '_')) = i.indicator_code
        WHERE a.is_processed = 0;
        
        -- 3. Validar pesos se requisitado
        IF @validate_weights = 1
        BEGIN
            WITH weight_check AS (
                SELECT 
                    cod_assessor,
                    valid_from,
                    SUM(CASE WHEN indicator_type = 'CARD' THEN indicator_weight ELSE 0 END) as total_weight
                FROM #assignments_validated
                WHERE can_process = 1
                GROUP BY cod_assessor, valid_from
            )
            UPDATE av
            SET weight_validation = CASE 
                WHEN wc.total_weight IS NULL THEN 'Sem indicadores CARD'
                WHEN ABS(wc.total_weight - 100.0) < 0.01 THEN 'OK'
                ELSE CONCAT('Total = ', wc.total_weight)
            END
            FROM #assignments_validated av
            INNER JOIN weight_check wc 
                ON av.cod_assessor = wc.cod_assessor 
                AND av.valid_from = wc.valid_from;
        END
        
        -- 4. Processar registros válidos
        MERGE metadados.performance_assignments AS target
        USING (
            SELECT 
                av.*,
                ba.created_by,
                ba.approved_by,
                ba.comments
            FROM #assignments_validated av
            INNER JOIN bronze.performance_assignments ba ON av.load_id = ba.load_id
            WHERE av.can_process = 1
              AND (@validate_weights = 0 OR av.weight_validation IN ('OK', 'Sem indicadores CARD'))
        ) AS source
        ON target.cod_assessor = source.cod_assessor
           AND target.indicator_id = source.indicator_id
           AND target.valid_from = source.valid_from
           
        WHEN MATCHED AND target.valid_to IS NULL THEN
            UPDATE SET
                indicator_weight = source.indicator_weight,
                indicator_type = source.indicator_type,
                valid_to = source.valid_to,
                modified_date = GETDATE(),
                modified_by = ISNULL(source.created_by, 'ETL_UPDATE')
                
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (indicator_id, cod_assessor, indicator_weight, indicator_type,
                   valid_from, valid_to, created_date, created_by, approved_by, comments)
            VALUES (source.indicator_id, source.cod_assessor, source.indicator_weight,
                   source.indicator_type, source.valid_from, source.valid_to,
                   GETDATE(), ISNULL(source.created_by, 'ETL_IMPORT'),
                   source.approved_by, source.comments);
        
        -- 5. Marcar como processados
        UPDATE ba
        SET is_processed = 1,
            processing_date = GETDATE(),
            processing_status = CASE 
                WHEN av.can_process = 0 THEN 'ERROR'
                WHEN av.weight_validation NOT IN ('OK', 'Sem indicadores CARD') THEN 'WARNING'
                ELSE 'SUCCESS'
            END,
            processing_notes = CASE
                WHEN av.reference_validation != 'OK' THEN av.reference_validation
                WHEN av.weight_validation NOT IN ('OK', 'Sem indicadores CARD') THEN av.weight_validation
                ELSE 'Processed successfully'
            END,
            weight_sum_valid = CASE 
                WHEN av.weight_validation = 'OK' THEN 1 
                ELSE 0 
            END,
            indicator_exists = CASE 
                WHEN av.reference_validation = 'OK' THEN 1 
                ELSE 0 
            END
        FROM bronze.performance_assignments ba
        INNER JOIN #assignments_validated av ON ba.load_id = av.load_id;
        
        -- 6. Registrar execução
        INSERT INTO bronze.etl_control (
            etl_name, execution_start, execution_end, execution_status,
            source_system, target_table, records_read, records_written, records_error
        )
        SELECT 
            'TRANSFORM_ASSIGNMENTS',
            GETDATE(),
            GETDATE(),
            CASE 
                WHEN SUM(CASE WHEN can_process = 0 THEN 1 ELSE 0 END) > 0 THEN 'WARNING'
                ELSE 'SUCCESS'
            END,
            'bronze.performance_assignments',
            'metadados.performance_assignments',
            COUNT(*),
            SUM(CASE WHEN can_process = 1 THEN 1 ELSE 0 END),
            SUM(CASE WHEN can_process = 0 THEN 1 ELSE 0 END)
        FROM #assignments_validated;
        
        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
```

### 4.3 Transformação: performance_targets

**Origem**: bronze.performance_targets  
**Destino**: metadados.performance_targets

#### 4.3.1 Validações de Lógica de Negócio

```sql
-- Validar lógica stretch > target > minimum
CREATE FUNCTION bronze.fn_validate_target_logic(
    @target DECIMAL(18,4),
    @stretch DECIMAL(18,4),
    @minimum DECIMAL(18,4),
    @is_inverted BIT
)
RETURNS VARCHAR(100)
AS
BEGIN
    IF @stretch IS NULL OR @minimum IS NULL
        RETURN 'OK - Valores opcionais não definidos';
    
    IF @is_inverted = 0
    BEGIN
        -- Normal: stretch > target > minimum
        IF @stretch >= @target AND @target >= @minimum
            RETURN 'OK';
        ELSE
            RETURN CONCAT('Erro: Esperado stretch(', @stretch, ') > target(', @target, ') > minimum(', @minimum, ')');
    END
    ELSE
    BEGIN
        -- Invertido: stretch < target < minimum
        IF @stretch <= @target AND @target <= @minimum
            RETURN 'OK';
        ELSE
            RETURN CONCAT('Erro: Esperado stretch(', @stretch, ') < target(', @target, ') < minimum(', @minimum, ') [INVERTIDO]');
    END
    
    RETURN 'OK';
END;
```

#### 4.3.2 Procedure de Transformação (Otimizada para Volume)

```sql
CREATE PROCEDURE bronze.prc_process_targets_to_metadata
    @year INT = NULL,
    @batch_size INT = 500
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    -- Se não especificar ano, processar todos os pendentes
    SET @year = ISNULL(@year, 0);
    
    DECLARE @processed_count INT = 0;
    DECLARE @error_count INT = 0;
    DECLARE @start_time DATETIME = GETDATE();
    
    -- Processar em batches para melhor performance
    WHILE EXISTS (
        SELECT 1 
        FROM bronze.performance_targets 
        WHERE is_processed = 0 
          AND (@year = 0 OR target_year = @year)
    )
    BEGIN
        BEGIN TRANSACTION;
        
        BEGIN TRY
            -- 1. Selecionar batch para processar
            CREATE TABLE #batch (
                load_id INT PRIMARY KEY,
                cod_assessor VARCHAR(20),
                indicator_code VARCHAR(50),
                indicator_id INT,
                period_start DATE,
                period_end DATE,
                target_value DECIMAL(18,4),
                stretch_value DECIMAL(18,4),
                minimum_value DECIMAL(18,4),
                is_inverted BIT,
                validation_result VARCHAR(200)
            );
            
            -- 2. Preparar e validar batch
            INSERT INTO #batch
            SELECT TOP (@batch_size)
                t.load_id,
                UPPER(LTRIM(RTRIM(t.cod_assessor))) as cod_assessor,
                UPPER(REPLACE(LTRIM(RTRIM(t.indicator_code)), ' ', '_')) as indicator_code,
                i.indicator_id,
                TRY_CAST(t.period_start AS DATE) as period_start,
                EOMONTH(TRY_CAST(t.period_start AS DATE)) as period_end, -- Garantir último dia
                TRY_CAST(t.target_value AS DECIMAL(18,4)) as target_value,
                TRY_CAST(t.stretch_value AS DECIMAL(18,4)) as stretch_value,
                TRY_CAST(t.minimum_value AS DECIMAL(18,4)) as minimum_value,
                i.is_inverted,
                'PENDING' as validation_result
            FROM bronze.performance_targets t
            LEFT JOIN metadados.performance_indicators i 
                ON UPPER(REPLACE(LTRIM(RTRIM(t.indicator_code)), ' ', '_')) = i.indicator_code
            WHERE t.is_processed = 0
              AND (@year = 0 OR t.target_year = @year)
            ORDER BY t.load_id;
            
            -- 3. Validar lógica de valores
            UPDATE b
            SET validation_result = 
                CASE 
                    WHEN b.indicator_id IS NULL THEN 'Indicador não encontrado'
                    WHEN b.period_start IS NULL THEN 'Data inválida'
                    WHEN b.target_value IS NULL OR b.target_value <= 0 THEN 'Valor meta inválido'
                    ELSE bronze.fn_validate_target_logic(
                        b.target_value, 
                        b.stretch_value, 
                        b.minimum_value, 
                        b.is_inverted
                    )
                END
            FROM #batch b;
            
            -- 4. Processar registros válidos
            MERGE metadados.performance_targets AS target
            USING (
                SELECT 
                    b.*,
                    'MENSAL' as period_type
                FROM #batch b
                WHERE b.validation_result LIKE 'OK%'
            ) AS source
            ON target.cod_assessor = source.cod_assessor
               AND target.indicator_id = source.indicator_id
               AND target.period_start = source.period_start
               
            WHEN MATCHED THEN
                UPDATE SET
                    target_value = source.target_value,
                    stretch_target = source.stretch_value,
                    minimum_target = source.minimum_value,
                    modified_date = GETDATE(),
                    modified_by = 'ETL_UPDATE'
                    
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (indicator_id, cod_assessor, period_type, period_start, period_end,
                       target_value, stretch_target, minimum_target, created_date, created_by)
                VALUES (source.indicator_id, source.cod_assessor, source.period_type,
                       source.period_start, source.period_end, source.target_value,
                       source.stretch_value, source.minimum_value, GETDATE(), 'ETL_IMPORT');
            
            -- 5. Atualizar status no bronze
            UPDATE bt
            SET is_processed = 1,
                processing_date = GETDATE(),
                processing_status = CASE 
                    WHEN b.validation_result LIKE 'OK%' THEN 'SUCCESS'
                    ELSE 'ERROR'
                END,
                processing_notes = b.validation_result,
                target_logic_valid = CASE 
                    WHEN b.validation_result LIKE 'OK%' THEN 1 
                    ELSE 0 
                END
            FROM bronze.performance_targets bt
            INNER JOIN #batch b ON bt.load_id = b.load_id;
            
            -- 6. Contabilizar processamento
            SELECT 
                @processed_count = @processed_count + COUNT(*),
                @error_count = @error_count + SUM(CASE WHEN validation_result NOT LIKE 'OK%' THEN 1 ELSE 0 END)
            FROM #batch;
            
            DROP TABLE #batch;
            
            COMMIT TRANSACTION;
            
        END TRY
        BEGIN CATCH
            ROLLBACK TRANSACTION;
            
            -- Log erro e continuar
            INSERT INTO bronze.etl_control (
                etl_name, execution_start, execution_end, execution_status,
                source_system, target_table, error_message
            )
            VALUES (
                'TRANSFORM_TARGETS_BATCH',
                @start_time,
                GETDATE(),
                'ERROR',
                'bronze.performance_targets',
                'metadados.performance_targets',
                ERROR_MESSAGE()
            );
            
            -- Sair do loop em caso de erro crítico
            BREAK;
        END CATCH
    END
    
    -- 7. Log final da execução
    INSERT INTO bronze.etl_control (
        etl_name, execution_start, execution_end, execution_status,
        source_system, target_table, records_read, records_written, records_error,
        execution_log
    )
    VALUES (
        'TRANSFORM_TARGETS',
        @start_time,
        GETDATE(),
        CASE WHEN @error_count = 0 THEN 'SUCCESS' ELSE 'WARNING' END,
        'bronze.performance_targets',
        'metadados.performance_targets',
        @processed_count,
        @processed_count - @error_count,
        @error_count,
        CONCAT('Processados em ', DATEDIFF(SECOND, @start_time, GETDATE()), ' segundos')
    );
    
    -- Retornar resumo
    SELECT 
        @processed_count as total_processed,
        @processed_count - @error_count as success_count,
        @error_count as error_count,
        DATEDIFF(SECOND, @start_time, GETDATE()) as duration_seconds;
END;
```

## 5. Validações Cross-Entity

### 5.1 Validação de Integridade Referencial

```sql
CREATE PROCEDURE bronze.prc_validate_referential_integrity
AS
BEGIN
    -- Assignments sem indicador correspondente
    SELECT 
        'ASSIGNMENT_WITHOUT_INDICATOR' as issue_type,
        a.cod_assessor,
        a.indicator_code,
        COUNT(*) as occurrence_count
    FROM bronze.performance_assignments a
    LEFT JOIN metadados.performance_indicators i 
        ON UPPER(REPLACE(LTRIM(RTRIM(a.indicator_code)), ' ', '_')) = i.indicator_code
    WHERE i.indicator_id IS NULL
      AND a.is_processed = 0
    GROUP BY a.cod_assessor, a.indicator_code
    
    UNION ALL
    
    -- Targets sem assignment correspondente
    SELECT 
        'TARGET_WITHOUT_ASSIGNMENT' as issue_type,
        t.cod_assessor,
        t.indicator_code,
        COUNT(*) as occurrence_count
    FROM bronze.performance_targets t
    LEFT JOIN bronze.performance_assignments a
        ON t.cod_assessor = a.cod_assessor
        AND t.indicator_code = a.indicator_code
        AND TRY_CAST(t.period_start AS DATE) BETWEEN 
            TRY_CAST(a.valid_from AS DATE) AND 
            ISNULL(TRY_CAST(a.valid_to AS DATE), '9999-12-31')
    WHERE a.load_id IS NULL
      AND t.is_processed = 0
    GROUP BY t.cod_assessor, t.indicator_code;
END;
```

### 5.2 Validação de Completude Temporal

```sql
CREATE PROCEDURE bronze.prc_validate_temporal_completeness
    @year INT
AS
BEGIN
    -- Verificar se todos os assessores têm 12 meses de targets
    WITH expected_months AS (
        SELECT 
            a.cod_assessor,
            a.indicator_code,
            m.month_date
        FROM (
            SELECT DISTINCT cod_assessor, indicator_code
            FROM bronze.performance_assignments
            WHERE is_processed = 1
        ) a
        CROSS JOIN (
            SELECT DATEADD(MONTH, n, DATEFROMPARTS(@year, 1, 1)) as month_date
            FROM (
                SELECT TOP 12 ROW_NUMBER() OVER (ORDER BY object_id) - 1 as n
                FROM sys.objects
            ) x
        ) m
    ),
    actual_months AS (
        SELECT 
            cod_assessor,
            indicator_code,
            TRY_CAST(period_start AS DATE) as month_date
        FROM bronze.performance_targets
        WHERE target_year = @year
          AND is_processed = 1
    )
    SELECT 
        e.cod_assessor,
        e.indicator_code,
        STRING_AGG(FORMAT(e.month_date, 'MMM'), ', ') as missing_months,
        COUNT(*) as missing_count
    FROM expected_months e
    LEFT JOIN actual_months a
        ON e.cod_assessor = a.cod_assessor
        AND e.indicator_code = a.indicator_code
        AND e.month_date = a.month_date
    WHERE a.month_date IS NULL
    GROUP BY e.cod_assessor, e.indicator_code
    ORDER BY missing_count DESC, e.cod_assessor;
END;
```

## 6. Processo de Quarentena

### 6.1 Tabela de Quarentena

```sql
CREATE TABLE bronze.quarantine_records (
    quarantine_id INT IDENTITY(1,1) PRIMARY KEY,
    source_table VARCHAR(200) NOT NULL,
    source_load_id INT NOT NULL,
    error_type VARCHAR(100) NOT NULL,
    error_message VARCHAR(MAX) NOT NULL,
    record_data VARCHAR(MAX) NOT NULL, -- JSON do registro original
    quarantine_date DATETIME NOT NULL DEFAULT GETDATE(),
    resolution_status VARCHAR(50) DEFAULT 'PENDING',
    resolution_date DATETIME NULL,
    resolution_notes VARCHAR(MAX) NULL,
    resolved_by VARCHAR(100) NULL
);

-- Procedure para quarentenar registros
CREATE PROCEDURE bronze.prc_quarantine_record
    @source_table VARCHAR(200),
    @source_load_id INT,
    @error_type VARCHAR(100),
    @error_message VARCHAR(MAX)
AS
BEGIN
    DECLARE @record_json VARCHAR(MAX);
    
    -- Capturar registro como JSON baseado na tabela
    IF @source_table = 'bronze.performance_indicators'
    BEGIN
        SELECT @record_json = (
            SELECT * FROM bronze.performance_indicators 
            WHERE load_id = @source_load_id
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        );
    END
    -- Repetir para outras tabelas...
    
    INSERT INTO bronze.quarantine_records (
        source_table, source_load_id, error_type, 
        error_message, record_data
    )
    VALUES (
        @source_table, @source_load_id, @error_type,
        @error_message, @record_json
    );
    
    -- Marcar registro original
    EXEC sp_executesql 
        N'UPDATE @table SET processing_status = ''QUARANTINED'' WHERE load_id = @id',
        N'@table VARCHAR(200), @id INT',
        @source_table, @source_load_id;
END;
```

## 7. Monitoramento e Alertas

### 7.1 Dashboard de Transformação

```sql
CREATE VIEW bronze.vw_transformation_dashboard AS
WITH processing_stats AS (
    SELECT 
        'indicators' as entity,
        COUNT(*) as total_records,
        SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processed,
        SUM(CASE WHEN processing_status = 'ERROR' THEN 1 ELSE 0 END) as errors,
        MAX(load_timestamp) as last_load,
        MAX(processing_date) as last_processed
    FROM bronze.performance_indicators
    
    UNION ALL
    
    SELECT 'assignments', COUNT(*), SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END),
           SUM(CASE WHEN processing_status = 'ERROR' THEN 1 ELSE 0 END),
           MAX(load_timestamp), MAX(processing_date)
    FROM bronze.performance_assignments
    
    UNION ALL
    
    SELECT 'targets', COUNT(*), SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END),
           SUM(CASE WHEN processing_status = 'ERROR' THEN 1 ELSE 0 END),
           MAX(load_timestamp), MAX(processing_date)
    FROM bronze.performance_targets
)
SELECT 
    entity,
    total_records,
    processed,
    total_records - processed as pending,
    errors,
    CAST(processed AS FLOAT) / NULLIF(total_records, 0) * 100 as processed_percent,
    last_load,
    last_processed,
    DATEDIFF(MINUTE, last_load, GETDATE()) as minutes_since_load,
    CASE 
        WHEN total_records - processed > 0 AND DATEDIFF(HOUR, last_load, GETDATE()) > 1 THEN 'ALERT'
        WHEN errors > 0 THEN 'WARNING'
        ELSE 'OK'
    END as status
FROM processing_stats;
```

### 7.2 Alertas Automáticos

```sql
CREATE PROCEDURE bronze.prc_check_transformation_health
AS
BEGIN
    DECLARE @alert_message VARCHAR(MAX) = '';
    
    -- Check 1: Registros pendentes há muito tempo
    IF EXISTS (
        SELECT 1 FROM bronze.vw_transformation_dashboard
        WHERE pending > 0 AND minutes_since_load > 120
    )
    BEGIN
        SET @alert_message += 'ALERTA: Registros pendentes de processamento há mais de 2 horas. ';
    END
    
    -- Check 2: Taxa de erro alta
    IF EXISTS (
        SELECT 1 FROM bronze.vw_transformation_dashboard
        WHERE errors > 0 AND (CAST(errors AS FLOAT) / total_records) > 0.1
    )
    BEGIN
        SET @alert_message += 'ALERTA: Taxa de erro superior a 10%. ';
    END
    
    -- Check 3: Validação de pesos falhando
    IF EXISTS (
        SELECT cod_assessor, COUNT(*) 
        FROM bronze.performance_assignments
        WHERE weight_sum_valid = 0 AND is_processed = 1
        GROUP BY cod_assessor
        HAVING COUNT(*) > 0
    )
    BEGIN
        SET @alert_message += 'ALERTA: Assessores com soma de pesos inválida. ';
    END
    
    IF @alert_message != ''
    BEGIN
        -- Enviar notificação (implementar conforme sistema de alertas)
        EXEC msdb.dbo.sp_send_dbmail
            @profile_name = 'DBA_Profile',
            @recipients = 'data-team@m7investimentos.com.br',
            @subject = 'Alerta - Transformação Bronze to Metadados',
            @body = @alert_message;
    END
END;
```

## 8. Performance e Otimização

### 8.1 Índices Recomendados

```sql
-- Índices para joins durante transformação
CREATE INDEX IX_bronze_perf_ind_transform 
ON bronze.performance_indicators (indicator_code, is_processed)
INCLUDE (indicator_name, category, unit);

CREATE INDEX IX_bronze_perf_asgn_transform
ON bronze.performance_assignments (cod_assessor, indicator_code, is_processed)
INCLUDE (weight, indicator_type, valid_from);

CREATE INDEX IX_bronze_perf_tgt_transform
ON bronze.performance_targets (cod_assessor, indicator_code, period_start, is_processed)
WHERE is_processed = 0; -- Filtered index

-- Estatísticas para colunas importantes
UPDATE STATISTICS bronze.performance_indicators (indicator_code) WITH FULLSCAN;
UPDATE STATISTICS bronze.performance_assignments (cod_assessor, indicator_code) WITH FULLSCAN;
UPDATE STATISTICS bronze.performance_targets (cod_assessor, indicator_code, period_start) WITH FULLSCAN;
```

### 8.2 Configurações para Alto Volume

```sql
-- Para targets (2500+ registros)
ALTER DATABASE M7Medallion SET RECOVERY BULK_LOGGED; -- Durante carga massiva

-- Configurar paralelismo
EXEC sp_configure 'max degree of parallelism', 4;
RECONFIGURE;

-- Aumentar memória para sorts
EXEC sp_configure 'min memory per query', 2048; -- KB
RECONFIGURE;
```

## 9. Rollback e Recuperação

### 9.1 Procedure de Rollback

```sql
CREATE PROCEDURE bronze.prc_rollback_transformation
    @entity VARCHAR(50), -- 'indicators', 'assignments', 'targets'
    @from_date DATETIME
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRANSACTION;
    
    BEGIN TRY
        IF @entity IN ('indicators', 'ALL')
        BEGIN
            -- Reverter processamento
            UPDATE bronze.performance_indicators
            SET is_processed = 0,
                processing_status = NULL,
                processing_date = NULL,
                processing_notes = 'Rolled back'
            WHERE processing_date >= @from_date;
            
            -- Opcional: remover do metadados
            DELETE FROM metadados.performance_indicators
            WHERE created_date >= @from_date
              AND created_by = 'ETL_IMPORT';
        END
        
        -- Repetir para outras entidades...
        
        -- Log rollback
        INSERT INTO bronze.etl_control (
            etl_name, execution_start, execution_end, execution_status,
            source_system, target_table, execution_log
        )
        VALUES (
            'ROLLBACK_TRANSFORMATION',
            GETDATE(),
            GETDATE(),
            'SUCCESS',
            'MANUAL',
            @entity,
            CONCAT('Rolled back from ', @from_date)
        );
        
        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
```

## 10. Documentação de Erros Comuns

### 10.1 Catálogo de Erros

| Código | Descrição | Causa | Solução |
|--------|-----------|-------|---------|
| ERR_001 | Código indicador inválido | Nulo ou > 50 chars | Corrigir na planilha |
| ERR_002 | Soma pesos != 100% | Erro de digitação | Ajustar pesos CARD |
| ERR_003 | Indicador não encontrado | Referência quebrada | Criar indicador primeiro |
| ERR_004 | Data inválida | Formato incorreto | Usar YYYY-MM-DD |
| ERR_005 | Valor meta <= 0 | Meta zerada | Definir meta válida |
| ERR_006 | Lógica stretch incorreta | Valores invertidos | Revisar is_inverted |

### 10.2 Query de Diagnóstico de Erros

```sql
-- Análise detalhada de erros
WITH error_analysis AS (
    SELECT 
        'indicators' as entity,
        processing_status,
        processing_notes,
        COUNT(*) as error_count
    FROM bronze.performance_indicators
    WHERE processing_status = 'ERROR'
    GROUP BY processing_status, processing_notes
    
    UNION ALL
    
    -- Similar para outras tabelas
)
SELECT 
    entity,
    processing_notes as error_detail,
    error_count,
    CAST(error_count AS FLOAT) / 
        (SELECT COUNT(*) FROM bronze.performance_indicators) * 100 as error_percent
FROM error_analysis
ORDER BY error_count DESC;
```

## 11. Fluxo Completo de Execução

### 11.1 Procedure Master

```sql
CREATE PROCEDURE bronze.prc_execute_full_transformation
    @validate_all BIT = 1,
    @send_notifications BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @start_time DATETIME = GETDATE();
    DECLARE @status VARCHAR(50) = 'SUCCESS';
    DECLARE @message VARCHAR(MAX) = '';
    
    BEGIN TRY
        -- 1. Transformar indicators
        EXEC bronze.prc_process_indicators_to_metadata;
        SET @message += 'Indicators processed. ';
        
        -- 2. Transformar assignments
        EXEC bronze.prc_process_assignments_to_metadata 
            @validate_weights = @validate_all,
            @validate_references = @validate_all;
        SET @message += 'Assignments processed. ';
        
        -- 3. Transformar targets
        DECLARE @current_year INT = YEAR(GETDATE());
        EXEC bronze.prc_process_targets_to_metadata @year = @current_year;
        SET @message += 'Targets processed. ';
        
        -- 4. Validações cross-entity
        IF @validate_all = 1
        BEGIN
            EXEC bronze.prc_validate_referential_integrity;
            EXEC bronze.prc_validate_temporal_completeness @year = @current_year;
            SET @message += 'Validations completed. ';
        END
        
        -- 5. Check health
        IF @send_notifications = 1
        BEGIN
            EXEC bronze.prc_check_transformation_health;
        END
        
    END TRY
    BEGIN CATCH
        SET @status = 'ERROR';
        SET @message = ERROR_MESSAGE();
    END CATCH
    
    -- Log execução completa
    INSERT INTO bronze.etl_control (
        etl_name, execution_start, execution_end, execution_status,
        source_system, target_table, execution_log
    )
    VALUES (
        'FULL_TRANSFORMATION',
        @start_time,
        GETDATE(),
        @status,
        'bronze.*',
        'metadados.*',
        @message
    );
    
    -- Retornar status
    SELECT 
        @status as execution_status,
        @message as execution_message,
        DATEDIFF(SECOND, @start_time, GETDATE()) as duration_seconds;
END;
```

## 12. Referências

- [MOD-001 - Modelo Performance Tracking (Metadados/Platinum)]
- [MOD-002 - Estrutura Bronze Performance]
- [ETL-001 - Extração Performance Indicators]
- [ETL-002 - Extração Performance Assignments]  
- [ETL-003 - Extração Performance Targets]
- [ARQ-002 - Arquitetura Medallion M7]

---

**Documento criado por**: Arquitetura de Dados M7  
**Data**: 2025-01-16  
**Próxima revisão**: 2025-04-16