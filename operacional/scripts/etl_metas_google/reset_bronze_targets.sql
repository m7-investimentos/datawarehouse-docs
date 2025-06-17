-- ==============================================================================
-- Script para resetar bronze.performance_targets
-- Remove duplicatas e prepara para nova carga
-- ==============================================================================

USE M7Medallion;
GO

-- 1. Verificar situação atual
PRINT '1. Situação atual da tabela Bronze:';
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT CONCAT(cod_assessor, '_', indicator_code, '_', period_start)) as unique_combinations,
    SUM(CASE WHEN is_processed = 0 THEN 1 ELSE 0 END) as unprocessed_records,
    SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processed_records
FROM bronze.performance_targets
WHERE target_year = 2025;

PRINT '';
PRINT '2. Limpando todos os registros de 2025 da Bronze...';

-- 2. Limpar todos os registros de 2025 para permitir nova carga limpa
DELETE FROM bronze.performance_targets
WHERE target_year = 2025;

PRINT 'Registros deletados: ' + CAST(@@ROWCOUNT AS VARCHAR(10));

PRINT '';
PRINT '3. Próximos passos:';
PRINT '   a) Execute o ETL novamente para carregar dados limpos na Bronze';
PRINT '   b) Execute a procedure para processar Bronze -> Silver';
PRINT '';
PRINT 'Comandos:';
PRINT '-- Python:';
PRINT '-- python etl_003_targets.py';
PRINT '';
PRINT '-- SQL:';
PRINT '-- EXEC [bronze].[prc_bronze_to_silver_performance_targets] @validate_completeness = 1, @debug_mode = 1;';