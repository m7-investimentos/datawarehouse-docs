-- ==============================================================================
-- QRY-ASS-003-prc_bronze_to_silver_assignments
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 2.0.0
-- Última atualização: 2025-01-18
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
    Bronze para Silver, incluindo validações e transformações de tipos.

Casos de uso:
    - Processamento diário/sob demanda de atribuições do Bronze
    - Validação de integridade de pesos
    - Inserção de novas atribuições na Silver
    - Notificação de erros críticos de validação

Frequência de execução: Diária ou sob demanda
Tempo médio de execução: 5-15 segundos
Volume esperado de linhas: ~200-500 registros por execução
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros da procedure:

@load_id              INT         -- ID específico de carga (reservado para uso futuro)
@validate_weights     BIT         -- Se deve validar soma de pesos (default: 1)
@force_update         BIT         -- Forçar atualização (reservado para uso futuro)
@debug                BIT         -- Modo debug com mensagens detalhadas (default: 0)

Exemplo de uso:
    EXEC bronze.prc_bronze_to_silver_assignments 
        @validate_weights = 1,
        @debug = 1;
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Retorno da procedure:
    - Return code: 0 = sucesso, -1 = erro
    - Mensagens via PRINT em modo debug
    - Registros em tabela de log de processamento (se existir)

Resultados esperados:
    - Registros inseridos em silver.performance_assignments
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
        
        -- ==============================================================================
        -- 8. PROCESSAMENTO PRINCIPAL
        -- ==============================================================================
        
        -- Inserir novos registros diretamente na Silver
        INSERT INTO silver.performance_assignments (
            codigo_assessor_crm,
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
            UPPER(LTRIM(RTRIM(b.codigo_assessor_crm))) as codigo_assessor_crm,
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
            CASE WHEN b.approved_by IS NOT NULL THEN GETDATE() ELSE NULL END as approved_date,
            b.approved_by as approved_by,
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
              WHERE s.codigo_assessor_crm = UPPER(LTRIM(RTRIM(b.codigo_assessor_crm)))
                AND s.indicator_id = i.indicator_id
                AND s.valid_from = ISNULL(TRY_CAST(b.valid_from AS DATE), '2025-01-01')
          );
        
        SET @rows_inserted = @@ROWCOUNT;
        
        -- ==============================================================================
        -- 9. VALIDAÇÃO DE PESOS (se habilitada)
        -- ==============================================================================
        
        IF @validate_weights = 1
        BEGIN
            -- Contar assessores com peso inválido
            WITH weight_check AS (
                SELECT 
                    a.codigo_assessor_crm,
                    SUM(CASE WHEN i.category = 'CARD' THEN a.indicator_weight ELSE 0 END) as total_weight
                FROM silver.performance_assignments a
                INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
                WHERE a.is_active = 1
                  AND a.bronze_load_id IN (
                      SELECT DISTINCT load_id 
                      FROM bronze.performance_assignments 
                      WHERE processing_date >= DATEADD(MINUTE, -5, GETDATE())
                  )
                GROUP BY a.codigo_assessor_crm
            )
            SELECT @rows_error = COUNT(*)
            FROM weight_check
            WHERE ABS(total_weight - 100.00) >= 0.01;
            
            IF @rows_error > 0 AND @debug = 1
            BEGIN
                PRINT FORMATMESSAGE('AVISO: %d assessores com soma de pesos inválida', @rows_error);
            END
        END
        
        -- ==============================================================================
        -- 10. ATUALIZAÇÃO DO BRONZE
        -- ==============================================================================
        
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
                      AND UPPER(LTRIM(RTRIM(b2.indicator_code))) NOT IN (
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
                      AND UPPER(LTRIM(RTRIM(b2.indicator_code))) NOT IN (
                          SELECT indicator_code FROM silver.performance_indicators
                      )
                ) THEN 'Processado com avisos - verificar indicadores não encontrados'
                ELSE 'Processado com sucesso'
            END
        WHERE is_processed = 0;
        
        -- ==============================================================================
        -- 11. AUDITORIA E LOG
        -- ==============================================================================
        
        -- Registrar execução (se tabela de log existir)
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

-- ==============================================================================
-- 12. PERMISSÕES
-- ==============================================================================

-- Conceder permissão de execução
-- GRANT EXECUTE ON [bronze].[prc_bronze_to_silver_assignments] TO [etl_user];
-- GO

-- ==============================================================================
-- 13. TESTES UNITÁRIOS
-- ==============================================================================

/*
-- Teste 1: Processar todos os registros pendentes
EXEC bronze.prc_bronze_to_silver_assignments 
    @debug = 1;

-- Teste 2: Processar com validação de pesos
EXEC bronze.prc_bronze_to_silver_assignments 
    @validate_weights = 1,
    @debug = 1;

-- Verificar resultados
SELECT 
    a.codigo_assessor_crm,
    i.indicator_name,
    a.indicator_weight,
    a.valid_from,
    a.created_date
FROM silver.performance_assignments a
INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
WHERE a.bronze_load_id > 0
ORDER BY a.codigo_assessor_crm, i.indicator_name;
*/

-- ==============================================================================
-- 14. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                | Descrição
--------|------------|----------------------|--------------------------------------------
1.0.0   | 2025-01-17 | bruno.chiaramonti   | Criação inicial da procedure
2.0.0   | 2025-01-18 | bruno.chiaramonti   | Reescrita completa para corrigir erro valid_from
        |            |                      | - Removida tabela temporária complexa
        |            |                      | - Processamento direto Bronze -> Silver
        |            |                      | - Validação de pesos simplificada

*/

-- ==============================================================================
-- 15. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
    - Procedure processa todos os registros não processados de uma vez
    - Indicadores devem existir em silver.performance_indicators
    - Valid_from vazio é convertido para '2025-01-01'
    - Validação de pesos é opcional mas recomendada
    - Modo debug fornece informações detalhadas

Troubleshooting comum:
    1. "Indicator not found": Executar ETL-001 primeiro
    2. "Weight sum invalid": Verificar planilha origem
    3. "Duplicate key": Verificar valid_from duplicados
    4. Performance lenta: Verificar índices e estatísticas

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/