-- ==============================================================================
-- PROCEDURE REESCRITA - bronze.prc_bronze_to_silver_assignments_v2
-- ==============================================================================
-- Versão simplificada e testada passo a passo
-- ==============================================================================

USE [M7Medallion];
GO

-- Drop procedure se existir
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'[bronze].[prc_bronze_to_silver_assignments_v2]'))
    DROP PROCEDURE [bronze].[prc_bronze_to_silver_assignments_v2];
GO

CREATE PROCEDURE [bronze].[prc_bronze_to_silver_assignments_v2]
    @debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @rows_processed INT = 0;
    DECLARE @rows_inserted INT = 0;
    DECLARE @error_msg NVARCHAR(4000);
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        IF @debug = 1
            PRINT 'Iniciando processamento Bronze to Silver...';
        
        -- Processar registros do Bronze para Silver
        INSERT INTO silver.performance_assignments (
            crm_id,
            indicator_id,
            indicator_weight,
            valid_from,
            valid_to,
            created_date,
            created_by,
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
        
        -- Marcar registros como processados
        UPDATE bronze.performance_assignments
        SET 
            is_processed = 1,
            processing_date = GETDATE(),
            processing_status = 'SUCCESS',
            processing_notes = 'Processado com sucesso'
        WHERE is_processed = 0;
        
        SET @rows_processed = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        
        IF @debug = 1
        BEGIN
            PRINT 'Processamento concluído!';
            PRINT CONCAT('Registros processados: ', @rows_processed);
            PRINT CONCAT('Registros inseridos: ', @rows_inserted);
        END
        
        RETURN 0;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        SET @error_msg = ERROR_MESSAGE();
        
        -- Marcar registros com erro
        UPDATE bronze.performance_assignments
        SET 
            processing_status = 'ERROR',
            processing_notes = LEFT(@error_msg, 500)
        WHERE is_processed = 0;
        
        RAISERROR(@error_msg, 16, 1);
        RETURN -1;
        
    END CATCH
END
GO

-- Testar a nova procedure
PRINT '';
PRINT 'Testando nova procedure...';
PRINT '';

-- Verificar quantos registros existem para processar
DECLARE @count INT;
SELECT @count = COUNT(*) FROM bronze.performance_assignments WHERE is_processed = 0;
PRINT CONCAT('Registros para processar: ', @count);

-- Executar se houver registros
IF @count > 0
BEGIN
    EXEC bronze.prc_bronze_to_silver_assignments_v2 @debug = 1;
    
    -- Verificar resultados
    SELECT @count = COUNT(*) FROM silver.performance_assignments WHERE bronze_load_id > 0;
    PRINT CONCAT('Total na Silver após processamento: ', @count);
END