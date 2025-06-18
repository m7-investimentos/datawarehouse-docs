-- ==============================================================================
-- PROCEDURE CORRIGIDA FINAL - bronze.prc_bronze_to_silver_assignments
-- ==============================================================================
-- Problema identificado: Conflito com queries de debug e subqueries
-- Solução: Simplificar queries e garantir que todas as referências tenham alias
-- ==============================================================================

USE [M7Medallion];
GO

-- Drop procedure existente
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'[bronze].[prc_bronze_to_silver_assignments]'))
    DROP PROCEDURE [bronze].[prc_bronze_to_silver_assignments];
GO

CREATE PROCEDURE [bronze].[prc_bronze_to_silver_assignments]
    @load_id INT = NULL,
    @validate_weights BIT = 1,
    @force_update BIT = 0,
    @debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    -- Variáveis de controle
    DECLARE @start_time DATETIME = GETDATE();
    DECLARE @rows_read INT = 0;
    DECLARE @rows_inserted INT = 0;
    DECLARE @rows_updated INT = 0;
    DECLARE @rows_error INT = 0;
    DECLARE @error_message NVARCHAR(4000);
    DECLARE @procedure_name NVARCHAR(128) = OBJECT_NAME(@@PROCID);
    
    IF @debug = 1
        PRINT FORMATMESSAGE('[%s] Iniciando processamento Bronze → Silver Assignments', CONVERT(VARCHAR, GETDATE(), 120));
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Contar registros a processar
        SELECT @rows_read = COUNT(*)
        FROM bronze.performance_assignments
        WHERE is_processed = 0;
        
        IF @rows_read = 0
        BEGIN
            IF @debug = 1
                PRINT 'Nenhum registro para processar.';
            COMMIT TRANSACTION;
            RETURN 0;
        END
        
        IF @debug = 1
            PRINT FORMATMESSAGE('Registros a processar: %d', @rows_read);
        
        -- Processar registros diretamente sem tabela temporária complexa
        -- Isso evita o problema com valid_from
        
        -- Inserir novos registros
        INSERT INTO silver.performance_assignments (
            crm_id,
            indicator_id,
            indicator_weight,
            valid_from,
            valid_to,
            created_date,
            created_by,
            approved_date,
            approved_by,
            is_active,
            comments,
            bronze_load_id
        )
        SELECT 
            UPPER(LTRIM(RTRIM(b.crm_id))) as crm_id,
            i.indicator_id,
            CASE 
                WHEN b.indicator_type = 'CARD' THEN 
                    ISNULL(TRY_CAST(b.weight AS DECIMAL(5,2)), 0.00)
                ELSE 0.00
            END as indicator_weight,
            ISNULL(TRY_CAST(b.valid_from AS DATE), '2025-01-01') as valid_from,
            TRY_CAST(b.valid_to AS DATE) as valid_to,
            GETDATE() as created_date,
            'ETL_SYSTEM' as created_by,
            NULL as approved_date,
            NULL as approved_by,
            1 as is_active,
            b.notes as comments,
            b.load_id as bronze_load_id
        FROM bronze.performance_assignments b
        INNER JOIN silver.performance_indicators i 
            ON UPPER(LTRIM(RTRIM(b.indicator_code))) = i.indicator_code
        WHERE b.is_processed = 0
          AND NOT EXISTS (
              SELECT 1 
              FROM silver.performance_assignments s
              WHERE s.crm_id = UPPER(LTRIM(RTRIM(b.crm_id)))
                AND s.indicator_id = i.indicator_id
                AND s.valid_from = ISNULL(TRY_CAST(b.valid_from AS DATE), '2025-01-01')
          );
        
        SET @rows_inserted = @@ROWCOUNT;
        
        -- Validação de pesos se habilitada
        IF @validate_weights = 1
        BEGIN
            -- Contar assessores com peso inválido
            WITH weight_check AS (
                SELECT 
                    crm_id,
                    SUM(CASE WHEN i.category = 'CARD' THEN indicator_weight ELSE 0 END) as total_weight
                FROM silver.performance_assignments a
                INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
                WHERE a.is_active = 1
                  AND a.bronze_load_id IN (
                      SELECT DISTINCT load_id 
                      FROM bronze.performance_assignments 
                      WHERE is_processed = 0
                  )
                GROUP BY crm_id
            )
            SELECT @rows_error = COUNT(*)
            FROM weight_check
            WHERE ABS(total_weight - 100.00) >= 0.01;
            
            IF @rows_error > 0 AND @debug = 1
            BEGIN
                PRINT FORMATMESSAGE('AVISO: %d assessores com soma de pesos inválida', @rows_error);
            END
        END
        
        -- Marcar registros como processados
        UPDATE bronze.performance_assignments
        SET 
            is_processed = 1,
            processing_date = GETDATE(),
            processing_status = CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM bronze.performance_assignments b2
                    WHERE b2.load_id = bronze.performance_assignments.load_id
                      AND b2.indicator_code NOT IN (
                          SELECT indicator_code FROM silver.performance_indicators
                      )
                ) THEN 'WARNING'
                ELSE 'SUCCESS'
            END,
            processing_notes = CASE
                WHEN EXISTS (
                    SELECT 1 
                    FROM bronze.performance_assignments b2
                    WHERE b2.load_id = bronze.performance_assignments.load_id
                      AND b2.indicator_code NOT IN (
                          SELECT indicator_code FROM silver.performance_indicators
                      )
                ) THEN 'Processado com avisos - verificar indicadores não encontrados'
                ELSE 'Processado com sucesso'
            END
        WHERE is_processed = 0;
        
        -- Registrar execução se tabela de log existir
        IF OBJECT_ID('silver.etl_process_log') IS NOT NULL
        BEGIN
            INSERT INTO silver.etl_process_log (
                process_name,
                process_type,
                start_time,
                end_time,
                duration_seconds,
                rows_read,
                rows_inserted,
                rows_updated,
                rows_error,
                status,
                details
            )
            VALUES (
                @procedure_name,
                'BRONZE_TO_SILVER',
                @start_time,
                GETDATE(),
                DATEDIFF(SECOND, @start_time, GETDATE()),
                @rows_read,
                @rows_inserted,
                @rows_updated,
                @rows_error,
                'SUCCESS',
                'Processamento concluído'
            );
        END
        
        COMMIT TRANSACTION;
        
        -- Mensagem final
        IF @debug = 1
        BEGIN
            PRINT REPLICATE('-', 80);
            PRINT 'Processamento concluído com sucesso!';
            PRINT FORMATMESSAGE('Registros lidos: %d', @rows_read);
            PRINT FORMATMESSAGE('Registros inseridos: %d', @rows_inserted);
            PRINT FORMATMESSAGE('Erros de validação: %d', @rows_error);
            PRINT FORMATMESSAGE('Tempo de execução: %d segundos', DATEDIFF(SECOND, @start_time, GETDATE()));
            PRINT REPLICATE('-', 80);
        END
        
        RETURN 0;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        SET @error_message = ERROR_MESSAGE();
        
        -- Marcar registros com erro
        UPDATE bronze.performance_assignments
        SET 
            processing_status = 'ERROR',
            processing_notes = LEFT(@error_message, 500)
        WHERE is_processed = 0;
        
        -- Registrar erro no log se existir
        IF OBJECT_ID('silver.etl_process_log') IS NOT NULL
        BEGIN
            INSERT INTO silver.etl_process_log (
                process_name,
                process_type,
                start_time,
                end_time,
                duration_seconds,
                rows_read,
                rows_inserted,
                rows_updated,
                rows_error,
                status,
                details
            )
            VALUES (
                @procedure_name,
                'BRONZE_TO_SILVER',
                @start_time,
                GETDATE(),
                DATEDIFF(SECOND, @start_time, GETDATE()),
                @rows_read,
                0,
                0,
                @rows_read,
                'ERROR',
                @error_message
            );
        END
        
        -- Re-throw erro
        RAISERROR(@error_message, 16, 1);
        RETURN -1;
        
    END CATCH
END
GO

PRINT 'Procedure bronze.prc_bronze_to_silver_assignments criada com sucesso!';
PRINT '';
PRINT 'Principais mudanças:';
PRINT '1. Removida tabela temporária complexa que causava o erro';
PRINT '2. Processamento direto Bronze -> Silver';
PRINT '3. Validação de pesos simplificada';
PRINT '4. Queries de debug removidas ou simplificadas';