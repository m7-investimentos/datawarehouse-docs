-- ==============================================================================
-- QRY-ASS-003-prc_bronze_to_silver_assignments
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [procedure, etl, bronze, silver, performance, assignments]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: 
    Procedure para processar atribuições de indicadores de performance da camada 
    Bronze para Silver, incluindo validações, transformações de tipos e merge 
    com histórico existente.

Casos de uso:
    - Processamento diário/sob demanda de atribuições do Bronze
    - Validação de integridade de pesos e relacionamentos
    - Atualização de atribuições vigentes mantendo histórico
    - Notificação de erros críticos de validação

Frequência de execução: Diária ou sob demanda
Tempo médio de execução: 10-30 segundos
Volume esperado de linhas: ~200-500 registros por execução
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros da procedure:

@load_id              INT         -- ID específico de carga para processar (NULL = última carga)
@validate_weights     BIT         -- Se deve validar soma de pesos (default: 1)
@force_update         BIT         -- Forçar atualização mesmo sem mudanças (default: 0)
@debug                BIT         -- Modo debug com mensagens detalhadas (default: 0)

Exemplo de uso:
    EXEC silver.prc_bronze_to_silver_assignments 
        @load_id = NULL,
        @validate_weights = 1,
        @force_update = 0,
        @debug = 0;
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Retorno da procedure:
    - Return code: 0 = sucesso, -1 = erro
    - Mensagens via PRINT em modo debug
    - Registros em tabela de log de processamento

Resultados esperados:
    - Registros inseridos em silver.performance_assignments
    - Registros atualizados com valid_to para mudanças
    - Flags is_processed = 1 em bronze.performance_assignments
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
    - bronze.performance_assignments: Fonte dos dados
    - silver.performance_indicators: Validação de indicadores
    - silver.performance_assignments: Destino dos dados
    
Pré-requisitos:
    - Dados devem estar carregados no Bronze
    - Indicadores devem existir em silver.performance_indicators
    - Usuário deve ter permissões de INSERT/UPDATE/SELECT
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
USE [M7Medallion];
GO

-- ==============================================================================
-- 6. PROCEDURE PRINCIPAL
-- ==============================================================================

-- Drop procedure existente se necessário
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
        
        -- ==============================================================================
        -- 7. VALIDAÇÕES INICIAIS
        -- ==============================================================================
        
        -- Determinar load_id a processar
        IF @load_id IS NULL
        BEGIN
            SELECT @load_id = MAX(load_id)
            FROM bronze.performance_assignments
            WHERE is_processed = 0;
            
            IF @load_id IS NULL
            BEGIN
                IF @debug = 1
                    PRINT 'Nenhuma carga pendente para processar.';
                COMMIT TRANSACTION;
                RETURN 0;
            END
        END
        
        -- Contar registros a processar
        SELECT @rows_read = COUNT(*)
        FROM bronze.performance_assignments
        WHERE load_id = @load_id
          AND is_processed = 0;
        
        IF @debug = 1
            PRINT FORMATMESSAGE('Load ID: %d | Registros a processar: %d', @load_id, @rows_read);
        
        -- ==============================================================================
        -- 8. PREPARAÇÃO DOS DADOS
        -- ==============================================================================
        
        -- Tabela temporária para dados transformados
        IF OBJECT_ID('tempdb..#assignments_staging') IS NOT NULL
            DROP TABLE #assignments_staging;
        
        CREATE TABLE #assignments_staging (
            cod_assessor VARCHAR(20),
            indicator_id INT,
            indicator_code VARCHAR(100),
            indicator_type VARCHAR(50),
            indicator_weight DECIMAL(5,2),
            valid_from DATE,
            valid_to DATE,
            created_by VARCHAR(100),
            approved_by VARCHAR(100),
            comments NVARCHAR(1000),
            row_hash VARCHAR(32),
            bronze_load_id INT,
            weight_sum_valid BIT,
            indicator_exists BIT,
            validation_errors NVARCHAR(MAX)
        );
        
        -- Transformar e validar dados
        INSERT INTO #assignments_staging
        SELECT 
            UPPER(LTRIM(RTRIM(b.cod_assessor))) as cod_assessor,
            i.indicator_id,
            UPPER(LTRIM(RTRIM(b.indicator_code))) as indicator_code,
            UPPER(LTRIM(RTRIM(b.indicator_type))) as indicator_type,
            CASE 
                WHEN b.indicator_type = 'CARD' THEN TRY_CAST(b.weight AS DECIMAL(5,2))
                ELSE 0.00
            END as indicator_weight,
            TRY_CAST(b.valid_from AS DATE) as valid_from,
            TRY_CAST(b.valid_to AS DATE) as valid_to,
            'ETL_SYSTEM' as created_by,
            NULL as approved_by,
            b.notes as comments,
            b.row_hash,
            b.load_id,
            CASE WHEN b.weight_validation = '1' THEN 1 ELSE 0 END as weight_sum_valid,
            CASE WHEN i.indicator_id IS NOT NULL THEN 1 ELSE 0 END as indicator_exists,
            NULL as validation_errors
        FROM bronze.performance_assignments b
        LEFT JOIN silver.performance_indicators i 
            ON UPPER(LTRIM(RTRIM(b.indicator_code))) = i.indicator_code
        WHERE b.load_id = @load_id
          AND b.is_processed = 0;
        
        -- Validar indicadores não encontrados
        DECLARE @missing_indicators INT;
        SELECT @missing_indicators = COUNT(*)
        FROM #assignments_staging
        WHERE indicator_exists = 0;
        
        IF @missing_indicators > 0
        BEGIN
            IF @debug = 1
            BEGIN
                PRINT FORMATMESSAGE('AVISO: %d indicadores não encontrados:', @missing_indicators);
                SELECT DISTINCT indicator_code
                FROM #assignments_staging
                WHERE indicator_exists = 0;
            END
        END
        
        -- ==============================================================================
        -- 9. VALIDAÇÃO DE PESOS (se habilitada)
        -- ==============================================================================
        
        IF @validate_weights = 1
        BEGIN
            -- Validar soma de pesos CARD por assessor/período
            WITH weight_validation AS (
                SELECT 
                    cod_assessor,
                    valid_from,
                    SUM(indicator_weight) as total_weight,
                    COUNT(*) as card_count
                FROM #assignments_staging
                WHERE indicator_type = 'CARD'
                  AND indicator_exists = 1
                  AND (valid_to IS NULL OR valid_to > GETDATE())
                GROUP BY cod_assessor, valid_from
            )
            UPDATE s
            SET s.weight_sum_valid = CASE 
                WHEN ABS(v.total_weight - 100.00) < 0.01 THEN 1 
                ELSE 0 
            END
            FROM #assignments_staging s
            INNER JOIN weight_validation v 
                ON s.cod_assessor = v.cod_assessor 
                AND s.valid_from = v.valid_from
            WHERE s.indicator_type = 'CARD';
            
            -- Contar erros de validação
            SELECT @rows_error = COUNT(DISTINCT cod_assessor + CAST(valid_from AS VARCHAR))
            FROM #assignments_staging
            WHERE weight_sum_valid = 0
              AND indicator_type = 'CARD';
            
            IF @rows_error > 0 AND @debug = 1
            BEGIN
                PRINT FORMATMESSAGE('AVISO: %d assessores com soma de pesos inválida:', @rows_error);
                SELECT DISTINCT 
                    cod_assessor,
                    valid_from,
                    SUM(indicator_weight) as soma_atual,
                    100.00 as soma_esperada
                FROM #assignments_staging
                WHERE indicator_type = 'CARD'
                  AND weight_sum_valid = 0
                GROUP BY cod_assessor, valid_from;
            END
        END
        
        -- ==============================================================================
        -- 10. MERGE COM SILVER
        -- ==============================================================================
        
        -- Encerrar vigências antigas que serão substituídas
        UPDATE m
        SET 
            m.valid_to = DATEADD(DAY, -1, s.valid_from),
            m.modified_date = GETDATE(),
            m.modified_by = 'ETL_SYSTEM',
            m.is_active = 0
        FROM silver.performance_assignments m
        INNER JOIN (
            SELECT DISTINCT cod_assessor, MIN(valid_from) as new_valid_from
            FROM #assignments_staging
            WHERE indicator_exists = 1
            GROUP BY cod_assessor
        ) s ON m.cod_assessor = s.cod_assessor
        WHERE m.valid_to IS NULL
          AND m.valid_from < s.new_valid_from;
        
        SET @rows_updated = @@ROWCOUNT;
        
        -- Inserir novas atribuições
        INSERT INTO silver.performance_assignments (
            cod_assessor,
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
            s.cod_assessor,
            s.indicator_id,
            s.indicator_weight,
            s.valid_from,
            s.valid_to,
            GETDATE(),
            s.created_by,
            CASE WHEN s.approved_by IS NOT NULL THEN GETDATE() ELSE NULL END,
            s.approved_by,
            CASE WHEN s.valid_to IS NULL OR s.valid_to > GETDATE() THEN 1 ELSE 0 END,
            s.comments,
            s.bronze_load_id
        FROM #assignments_staging s
        WHERE s.indicator_exists = 1
          AND NOT EXISTS (
              SELECT 1 
              FROM silver.performance_assignments m
              WHERE m.cod_assessor = s.cod_assessor
                AND m.indicator_id = s.indicator_id
                AND m.valid_from = s.valid_from
          );
        
        SET @rows_inserted = @@ROWCOUNT;
        
        -- ==============================================================================
        -- 11. ATUALIZAÇÃO DO BRONZE
        -- ==============================================================================
        
        -- Marcar registros como processados
        UPDATE bronze.performance_assignments
        SET 
            is_processed = 1,
            processing_date = GETDATE(),
            processing_status = CASE 
                WHEN EXISTS (
                    SELECT 1 FROM #assignments_staging s 
                    WHERE s.bronze_load_id = bronze.performance_assignments.load_id
                      AND s.indicator_exists = 0
                ) THEN 'WARNING'
                ELSE 'SUCCESS'
            END,
            processing_notes = CASE
                WHEN EXISTS (
                    SELECT 1 FROM #assignments_staging s 
                    WHERE s.bronze_load_id = bronze.performance_assignments.load_id
                      AND s.indicator_exists = 0
                ) THEN 'Processado com avisos - verificar indicadores não encontrados'
                ELSE 'Processado com sucesso'
            END
        WHERE load_id = @load_id
          AND is_processed = 0;
        
        -- ==============================================================================
        -- 12. AUDITORIA E LOG
        -- ==============================================================================
        
        -- Registrar execução (criar tabela de log se não existir)
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
                FORMATMESSAGE('Load ID: %d', @load_id)
            );
        END
        
        COMMIT TRANSACTION;
        
        -- Mensagem final
        IF @debug = 1
        BEGIN
            PRINT REPLICATE('-', 80);
            PRINT FORMATMESSAGE('Processamento concluído com sucesso!');
            PRINT FORMATMESSAGE('Registros lidos: %d', @rows_read);
            PRINT FORMATMESSAGE('Registros inseridos: %d', @rows_inserted);
            PRINT FORMATMESSAGE('Vigências encerradas: %d', @rows_updated);
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
        WHERE load_id = @load_id
          AND is_processed = 0;
        
        -- Registrar erro no log
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
        RAISERROR('%s', 16, 1, @error_message);
        
    END CATCH
END
GO

-- ==============================================================================
-- 13. PERMISSÕES
-- ==============================================================================

-- Conceder permissão de execução
-- GRANT EXECUTE ON [bronze].[prc_bronze_to_silver_assignments] TO [etl_user];
-- GO

-- ==============================================================================
-- 14. TESTES UNITÁRIOS
-- ==============================================================================

/*
-- Teste 1: Processar última carga
EXEC bronze.prc_bronze_to_silver_assignments 
    @debug = 1;

-- Teste 2: Processar carga específica
EXEC bronze.prc_bronze_to_silver_assignments 
    @load_id = 123,
    @validate_weights = 1,
    @debug = 1;

-- Teste 3: Forçar atualização
EXEC bronze.prc_bronze_to_silver_assignments 
    @force_update = 1,
    @debug = 1;

-- Verificar resultados
SELECT * FROM silver.vw_performance_assignments_current
ORDER BY cod_assessor, indicator_type;
*/

-- ==============================================================================
-- 15. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                | Descrição
--------|------------|----------------------|--------------------------------------------
1.0.0   | 2025-01-17 | bruno.chiaramonti   | Criação inicial da procedure

*/

-- ==============================================================================
-- 16. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
    - Procedure valida soma de pesos CARD = 100% por assessor
    - Indicadores não encontrados são registrados mas não impedem processamento
    - Histórico completo é mantido com valid_to
    - Transação garante consistência em caso de erro
    - Modo debug fornece informações detalhadas para troubleshooting

Troubleshooting comum:
    1. "Indicator not found": Executar ETL-001 primeiro
    2. "Weight sum invalid": Verificar planilha origem
    3. "Duplicate key": Verificar valid_from duplicados
    4. Performance lenta: Verificar índices e estatísticas

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/