-- ==============================================================================
-- QRY-IND-003-prc_bronze_to_silver_indicators
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [bronze, silver, etl, performance, indicadores, procedure]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Processa indicadores de performance da camada Bronze para Silver,
           aplicando validações, transformações e controle de versionamento.

Casos de uso:
- Processamento após carga do Google Sheets
- Validação e limpeza de dados
- Atualização de indicadores existentes
- Versionamento de mudanças

Frequência de execução: Após cada carga no Bronze
Tempo médio de execução: < 5 segundos
Volume esperado de linhas: 10-50 registros
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros:
@load_id          INT     -- ID específico da carga a processar (NULL = última carga)
@validate_only    BIT     -- Se 1, apenas valida sem processar (default: 0)
@debug_mode       BIT     -- Se 1, mostra informações de debug (default: 0)

Exemplo de uso:
EXEC [bronze].[prc_process_indicators_to_silver] 
    @load_id = NULL,
    @validate_only = 0,
    @debug_mode = 1;
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Resultados retornados:
- Mensagens de status via PRINT
- Em modo debug: datasets com detalhes do processamento
- Código de retorno: 0 = sucesso, > 0 = erro

Tabelas afetadas:
- bronze.performance_indicators (atualiza is_processed)
- silver.performance_indicators (insere/atualiza registros)
- audit.etl_transformations (log de processamento)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- bronze.performance_indicators: Fonte dos dados
- silver.performance_indicators: Destino dos dados

Pré-requisitos:
- Dados devem estar carregados no Bronze
- Tabelas de destino devem existir
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
USE [M7Medallion];
GO

-- Drop procedure se existir
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'[bronze].[prc_process_indicators_to_silver]'))
    DROP PROCEDURE [bronze].[prc_process_indicators_to_silver];
GO

-- ==============================================================================
-- 6. CRIAÇÃO DA PROCEDURE
-- ==============================================================================

CREATE PROCEDURE [bronze].[prc_process_indicators_to_silver]
    @load_id INT = NULL,
    @validate_only BIT = 0,
    @debug_mode BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    -- Variáveis de controle
    DECLARE @start_time DATETIME = GETDATE();
    DECLARE @row_count INT = 0;
    DECLARE @error_count INT = 0;
    DECLARE @warning_count INT = 0;
    DECLARE @processed_count INT = 0;
    DECLARE @inserted_count INT = 0;
    DECLARE @updated_count INT = 0;
    DECLARE @error_msg NVARCHAR(4000);
    
    -- Tabela temporária para validações
    CREATE TABLE #validation_errors (
        row_id INT,
        indicator_code VARCHAR(50),
        error_type VARCHAR(50),
        error_message NVARCHAR(500),
        severity VARCHAR(10) -- 'ERROR' ou 'WARNING'
    );
    
    BEGIN TRY
        -- ======================================================================
        -- INÍCIO DO PROCESSAMENTO
        -- ======================================================================
        PRINT '========================================';
        PRINT 'BRONZE TO METADATA - PERFORMANCE INDICATORS';
        PRINT 'Início: ' + CONVERT(VARCHAR(30), @start_time, 120);
        PRINT '========================================';
        
        -- Determinar load_id a processar
        IF @load_id IS NULL
        BEGIN
            SELECT @load_id = MAX(load_id)
            FROM bronze.performance_indicators
            WHERE is_processed = 0;
            
            IF @load_id IS NULL
            BEGIN
                PRINT 'Nenhuma carga pendente para processar.';
                RETURN 0;
            END
        END
        
        PRINT 'Processando load_id: ' + CAST(@load_id AS VARCHAR(10));
        
        -- ======================================================================
        -- VALIDAÇÕES
        -- ======================================================================
        PRINT '';
        PRINT 'Executando validações...';
        
        -- Validação 1: Campos obrigatórios
        INSERT INTO #validation_errors (row_id, indicator_code, error_type, error_message, severity)
        SELECT 
            load_id,
            ISNULL(indicator_code, 'VAZIO'),
            'CAMPO_OBRIGATORIO',
            CASE 
                WHEN indicator_code IS NULL OR indicator_code = '' THEN 'Código do indicador é obrigatório'
                WHEN indicator_name IS NULL OR indicator_name = '' THEN 'Nome do indicador é obrigatório'
            END,
            'ERROR'
        FROM bronze.performance_indicators
        WHERE load_id = @load_id
          AND (indicator_code IS NULL OR indicator_code = '' 
               OR indicator_name IS NULL OR indicator_name = '');
        
        -- Validação 2: Formato do código
        INSERT INTO #validation_errors (row_id, indicator_code, error_type, error_message, severity)
        SELECT 
            load_id,
            indicator_code,
            'FORMATO_INVALIDO',
            'Código deve conter apenas letras, números e underscore',
            'ERROR'
        FROM bronze.performance_indicators
        WHERE load_id = @load_id
          AND indicator_code IS NOT NULL
          AND indicator_code != ''
          AND PATINDEX('%[^A-Z0-9_]%', UPPER(indicator_code)) > 0;
        
        -- Validação 3: Categoria válida
        INSERT INTO #validation_errors (row_id, indicator_code, error_type, error_message, severity)
        SELECT 
            load_id,
            indicator_code,
            'CATEGORIA_INVALIDA',
            'Categoria "' + category + '" não é válida',
            'WARNING'
        FROM bronze.performance_indicators
        WHERE load_id = @load_id
          AND category NOT IN ('FINANCEIRO', 'QUALIDADE', 'VOLUME', 'COMPORTAMENTAL', 'PROCESSO', 'GATILHO')
          AND category IS NOT NULL
          AND category != '';
        
        -- Validação 4: Unidade válida
        INSERT INTO #validation_errors (row_id, indicator_code, error_type, error_message, severity)
        SELECT 
            load_id,
            indicator_code,
            'UNIDADE_INVALIDA',
            'Unidade "' + unit + '" não é válida',
            'WARNING'
        FROM bronze.performance_indicators
        WHERE load_id = @load_id
          AND unit NOT IN ('R$', '%', 'QTD', 'SCORE', 'HORAS', 'DIAS', 'RATIO')
          AND unit IS NOT NULL
          AND unit != '';
        
        -- Validação 5: Códigos duplicados na mesma carga
        INSERT INTO #validation_errors (row_id, indicator_code, error_type, error_message, severity)
        SELECT 
            MIN(load_id),
            indicator_code,
            'CODIGO_DUPLICADO',
            'Código aparece ' + CAST(COUNT(*) AS VARCHAR(10)) + ' vezes na mesma carga',
            'ERROR'
        FROM bronze.performance_indicators
        WHERE load_id = @load_id
          AND indicator_code IS NOT NULL
          AND indicator_code != ''
        GROUP BY indicator_code
        HAVING COUNT(*) > 1;
        
        -- Contar erros e warnings
        SELECT 
            @error_count = COUNT(CASE WHEN severity = 'ERROR' THEN 1 END),
            @warning_count = COUNT(CASE WHEN severity = 'WARNING' THEN 1 END)
        FROM #validation_errors;
        
        PRINT 'Validações concluídas:';
        PRINT '  - Erros: ' + CAST(@error_count AS VARCHAR(10));
        PRINT '  - Warnings: ' + CAST(@warning_count AS VARCHAR(10));
        
        -- Mostrar detalhes em modo debug
        IF @debug_mode = 1 AND EXISTS (SELECT 1 FROM #validation_errors)
        BEGIN
            PRINT '';
            PRINT 'Detalhes das validações:';
            SELECT * FROM #validation_errors ORDER BY severity DESC, row_id;
        END
        
        -- Se validate_only, parar aqui
        IF @validate_only = 1
        BEGIN
            PRINT '';
            PRINT 'Modo validação apenas. Processamento não executado.';
            RETURN @error_count;
        END
        
        -- Se houver erros críticos, parar
        IF @error_count > 0
        BEGIN
            PRINT '';
            PRINT 'Processamento abortado devido a erros de validação.';
            
            -- Marcar registros com erro
            UPDATE b
            SET processing_status = 'ERROR',
                processing_notes = 'Falha na validação: ' + ISNULL(e.error_message, 'Erro desconhecido'),
                processing_date = GETDATE()
            FROM bronze.performance_indicators b
            LEFT JOIN #validation_errors e ON b.load_id = e.row_id
            WHERE b.load_id = @load_id;
            
            RETURN @error_count;
        END
        
        -- ======================================================================
        -- TRANSFORMAÇÃO E CARGA
        -- ======================================================================
        PRINT '';
        PRINT 'Iniciando transformação e carga...';
        
        BEGIN TRANSACTION;
        
        -- Preparar dados para merge
        WITH transformed_data AS (
            SELECT 
                -- Padronizar código
                UPPER(REPLACE(LTRIM(RTRIM(indicator_code)), ' ', '_')) AS indicator_code,
                
                -- Limpar nome
                LTRIM(RTRIM(indicator_name)) AS indicator_name,
                
                -- Categoria com default
                CASE 
                    WHEN category IN ('FINANCEIRO', 'QUALIDADE', 'VOLUME', 'COMPORTAMENTAL', 'PROCESSO', 'GATILHO')
                    THEN category
                    ELSE 'PROCESSO' -- Default
                END AS category,
                
                -- Unidade com default
                CASE 
                    WHEN unit IN ('R$', '%', 'QTD', 'SCORE', 'HORAS', 'DIAS', 'RATIO')
                    THEN unit
                    ELSE 'QTD' -- Default
                END AS unit,
                
                -- Aggregation com default
                CASE 
                    WHEN aggregation IN ('SUM', 'AVG', 'COUNT', 'MAX', 'MIN', 'LAST', 'CUSTOM')
                    THEN aggregation
                    ELSE 'CUSTOM'
                END AS aggregation_method,
                
                -- Formula (manter vazia se não houver)
                CASE 
                    WHEN formula IS NOT NULL AND formula != '' AND formula != 'nan'
                    THEN LTRIM(RTRIM(formula))
                    ELSE NULL
                END AS calculation_formula,
                
                -- Conversão de booleanos
                CASE 
                    WHEN is_inverted IN ('1', 'TRUE', 'true', 'True') THEN 1
                    ELSE 0
                END AS is_inverted,
                
                CASE 
                    WHEN is_active IN ('0', 'FALSE', 'false', 'False') THEN 0
                    ELSE 1 -- Default ativo
                END AS is_active,
                
                -- Descrição
                CASE 
                    WHEN description IS NOT NULL AND description != '' AND description != 'nan'
                    THEN LTRIM(RTRIM(description))
                    ELSE NULL
                END AS description,
                
                -- Notes
                CASE 
                    WHEN notes IS NOT NULL AND notes != '' AND notes != 'nan'
                    THEN LTRIM(RTRIM(notes))
                    ELSE NULL
                END AS notes,
                
                -- Hash para detectar mudanças
                row_hash,
                load_id
            FROM bronze.performance_indicators
            WHERE load_id = @load_id
              AND indicator_code IS NOT NULL
              AND indicator_code != ''
              AND load_id NOT IN (SELECT row_id FROM #validation_errors WHERE severity = 'ERROR')
        )
        
        -- MERGE com silver
        MERGE silver.performance_indicators AS target
        USING transformed_data AS source
        ON target.indicator_code = source.indicator_code
        
        -- UPDATE quando existe e mudou
        WHEN MATCHED AND (
            target.indicator_name != source.indicator_name
            OR target.category != source.category
            OR target.unit != source.unit
            OR target.aggregation_method != source.aggregation_method
            OR ISNULL(target.calculation_formula, '') != ISNULL(source.calculation_formula, '')
            OR target.is_inverted != source.is_inverted
            OR target.is_active != source.is_active
            OR ISNULL(target.description, '') != ISNULL(source.description, '')
            OR ISNULL(target.notes, '') != ISNULL(source.notes, '')
        ) THEN UPDATE SET
            indicator_name = source.indicator_name,
            category = source.category,
            unit = source.unit,
            aggregation_method = source.aggregation_method,
            calculation_formula = source.calculation_formula,
            is_inverted = source.is_inverted,
            is_active = source.is_active,
            description = source.description,
            notes = source.notes,
            version = target.version + 1,
            modified_date = GETDATE(),
            modified_by = 'ETL_BRONZE_TO_SILVER'
        
        -- INSERT quando não existe
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            indicator_code,
            indicator_name,
            category,
            unit,
            aggregation_method,
            calculation_formula,
            is_inverted,
            is_active,
            description,
            notes,
            created_by
        ) VALUES (
            source.indicator_code,
            source.indicator_name,
            source.category,
            source.unit,
            source.aggregation_method,
            source.calculation_formula,
            source.is_inverted,
            source.is_active,
            source.description,
            source.notes,
            'ETL_BRONZE_TO_SILVER'
        );
        
        -- Capturar contadores
        SET @processed_count = @@ROWCOUNT;
        
        -- Marcar registros como processados
        UPDATE bronze.performance_indicators
        SET is_processed = 1,
            processing_date = GETDATE(),
            processing_status = 'SUCCESS',
            processing_notes = 'Processado com sucesso'
        WHERE load_id = @load_id
          AND load_id NOT IN (SELECT row_id FROM #validation_errors WHERE severity = 'ERROR');
        
        -- Marcar registros com warning
        UPDATE b
        SET processing_notes = 'Processado com warnings: ' + e.error_message
        FROM bronze.performance_indicators b
        INNER JOIN #validation_errors e ON b.load_id = e.row_id
        WHERE b.load_id = @load_id
          AND e.severity = 'WARNING';
        
        COMMIT TRANSACTION;
        
        -- ======================================================================
        -- RELATÓRIO FINAL
        -- ======================================================================
        PRINT '';
        PRINT '========================================';
        PRINT 'PROCESSAMENTO CONCLUÍDO';
        PRINT 'Registros processados: ' + CAST(@processed_count AS VARCHAR(10));
        PRINT 'Warnings: ' + CAST(@warning_count AS VARCHAR(10));
        PRINT 'Tempo de execução: ' + CAST(DATEDIFF(ms, @start_time, GETDATE()) AS VARCHAR(10)) + ' ms';
        PRINT '========================================';
        
        -- Mostrar resumo em modo debug
        IF @debug_mode = 1
        BEGIN
            PRINT '';
            PRINT 'Indicadores ativos após processamento:';
            SELECT 
                indicator_id,
                indicator_code,
                indicator_name,
                category,
                unit,
                version,
                modified_date
            FROM silver.performance_indicators
            WHERE is_active = 1
            ORDER BY CASE WHEN modified_date >= @start_time THEN 0 ELSE 1 END,
                     indicator_code;
        END
        
        RETURN 0;
        
    END TRY
    BEGIN CATCH
        -- Rollback em caso de erro
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Capturar erro
        SET @error_msg = ERROR_MESSAGE();
        
        PRINT '';
        PRINT '========================================';
        PRINT 'ERRO NO PROCESSAMENTO';
        PRINT 'Erro: ' + @error_msg;
        PRINT 'Linha: ' + CAST(ERROR_LINE() AS VARCHAR(10));
        PRINT '========================================';
        
        -- Marcar registros com erro
        UPDATE bronze.performance_indicators
        SET processing_status = 'ERROR',
            processing_notes = 'Erro no processamento: ' + LEFT(@error_msg, 500),
            processing_date = GETDATE()
        WHERE load_id = @load_id
          AND is_processed = 0;
        
        -- Re-lançar erro
        THROW;
        
    END CATCH
    
    -- Limpar objetos temporários
    IF OBJECT_ID('tempdb..#validation_errors') IS NOT NULL
        DROP TABLE #validation_errors;
        
END
GO

-- ==============================================================================
-- 7. PERMISSÕES
-- ==============================================================================

-- Dar permissão de execução para role ETL (descomentar e ajustar usuário quando necessário)
-- GRANT EXECUTE ON [bronze].[prc_process_indicators_to_silver] TO [db_etl_executor];
-- GO

-- ==============================================================================
-- 8. DOCUMENTAÇÃO
-- ==============================================================================

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Processa indicadores de performance da camada Bronze para Silver, aplicando validações, transformações e controle de versionamento.',
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'PROCEDURE', @level1name=N'prc_process_indicators_to_silver';
GO

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                   | Descrição
--------|------------|-------------------------|--------------------------------------------
1.0.0   | 2025-01-17 | bruno.chiaramonti      | Criação inicial da procedure
*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Procedure processa apenas registros não processados (is_processed = 0)
- Validações impedem processamento se houver erros críticos
- Warnings são registrados mas não impedem processamento
- Versionamento automático ao detectar mudanças
- Indicadores podem ser desativados mas não deletados

Troubleshooting comum:
1. "Nenhuma carga pendente": Verificar se há registros com is_processed = 0
2. Erros de validação: Executar com @debug_mode = 1 para ver detalhes
3. Timeout em cargas grandes: Improvável com volume esperado

Exemplo de execução:
-- Processar última carga
EXEC bronze.prc_process_indicators_to_silver;

-- Validar sem processar
EXEC bronze.prc_process_indicators_to_silver @validate_only = 1, @debug_mode = 1;

-- Reprocessar carga específica
UPDATE bronze.performance_indicators SET is_processed = 0 WHERE load_id = 123;
EXEC bronze.prc_process_indicators_to_silver @load_id = 123;

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

PRINT 'Procedure bronze.prc_process_indicators_to_silver criada com sucesso!';