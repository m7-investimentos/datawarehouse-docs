-- ==============================================================================
-- QRY-TAR-003-prc_bronze_to_silver_performance_targets
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [procedure, etl, bronze, silver, performance, targets]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Processa dados de metas de performance da camada Bronze para Silver,
aplicando validações, conversões de tipos e merge com dados existentes.

Casos de uso:
- Processamento mensal de novas metas carregadas
- Atualização de metas existentes
- Validação de integridade e completude anual
- Geração de relatório de processamento

Frequência de execução: Mensal ou sob demanda
Tempo médio de execução: 10-30 segundos
Volume esperado de linhas: 2500-3000 registros por execução
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros necessários para execução:

@target_year          INT      -- Ano das metas a processar (NULL = ano atual)
@validate_completeness BIT     -- Validar se há 12 meses por assessor/indicador (default: 1)
@debug_mode           BIT      -- Modo debug com saídas detalhadas (default: 0)

Exemplo de uso:
-- Processar ano atual com validação
EXEC silver.prc_bronze_to_silver_performance_targets;

-- Processar ano específico
EXEC silver.prc_bronze_to_silver_performance_targets 
    @target_year = 2025,
    @validate_completeness = 1;

-- Modo debug
EXEC silver.prc_bronze_to_silver_performance_targets 
    @debug_mode = 1;
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Saídas da procedure:

1. Resultset de resumo:
   - total_processed: Total de registros processados
   - inserted_count: Novos registros inseridos
   - updated_count: Registros atualizados
   - error_count: Registros com erro
   - incomplete_combinations: Combinações sem 12 meses

2. Em modo debug, resultsets adicionais com detalhes

3. Return value:
   - 0: Sucesso
   - 1: Erro na execução
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- bronze.performance_targets: Fonte dos dados brutos
- silver.performance_indicators: Para validar indicator_code
- silver.performance_targets: Destino dos dados processados

Pré-requisitos:
- Dados carregados na bronze.performance_targets
- Indicadores cadastrados em silver.performance_indicators
- Permissões de SELECT/INSERT/UPDATE nas tabelas
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

-- Dropar procedure se existir
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[prc_bronze_to_silver_performance_targets]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [bronze].[prc_bronze_to_silver_performance_targets];
GO

-- ==============================================================================
-- 6. CRIAÇÃO DA PROCEDURE
-- ==============================================================================

CREATE PROCEDURE [bronze].[prc_bronze_to_silver_performance_targets]
    @target_year INT = NULL,
    @validate_completeness BIT = 1,
    @debug_mode BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    -- Variáveis locais
    DECLARE @start_time DATETIME = GETDATE();
    DECLARE @error_message NVARCHAR(4000);
    DECLARE @error_severity INT;
    DECLARE @error_state INT;
    DECLARE @row_count INT;
    DECLARE @incomplete_count INT;
    DECLARE @inserted_count INT = 0;
    DECLARE @updated_count INT = 0;
    DECLARE @error_count INT = 0;
    
    -- Definir ano se não especificado
    SET @target_year = ISNULL(@target_year, YEAR(GETDATE()));
    
    -- Log inicial
    IF @debug_mode = 1
    BEGIN
        PRINT '========================================';
        PRINT 'Iniciando processamento Bronze → Silver';
        PRINT 'Ano alvo: ' + CAST(@target_year AS VARCHAR(4));
        PRINT 'Validar completude: ' + CASE WHEN @validate_completeness = 1 THEN 'SIM' ELSE 'NÃO' END;
        PRINT 'Timestamp: ' + CONVERT(VARCHAR(23), @start_time, 121);
        PRINT '========================================';
    END;
    
    BEGIN TRY
        -- Verificar se já estamos em uma transação
        DECLARE @trancount INT = @@TRANCOUNT;
        
        IF @trancount = 0
            BEGIN TRANSACTION;
        
        -- ==============================================================================
        -- ETAPA 1: VALIDAÇÕES INICIAIS
        -- ==============================================================================
        
        -- Verificar se há dados para processar
        SELECT @row_count = COUNT(*)
        FROM bronze.performance_targets
        WHERE target_year = @target_year
          AND is_processed = 0;
          
        IF @row_count = 0
        BEGIN
            -- Commit transação vazia se iniciamos uma
            IF @trancount = 0 AND @@TRANCOUNT > 0
                COMMIT TRANSACTION;
                
            RAISERROR('Nenhum registro encontrado para processar no ano %d', 16, 1, @target_year);
            RETURN 1;
        END;
        
        IF @debug_mode = 1
            PRINT 'Registros a processar: ' + CAST(@row_count AS VARCHAR(10));
        
        -- ==============================================================================
        -- ETAPA 2: VALIDAR COMPLETUDE (SE SOLICITADO)
        -- ==============================================================================
        
        IF @validate_completeness = 1
        BEGIN
            -- Criar tabela temporária para análise
            CREATE TABLE #completeness_check (
                crm_id VARCHAR(20),
                indicator_code VARCHAR(50),
                months_count INT,
                missing_months VARCHAR(100)
            );
            
            -- Analisar completude
            INSERT INTO #completeness_check
            SELECT 
                crm_id,
                indicator_code,
                12 - COUNT(DISTINCT missing_month) as months_count,
                STRING_AGG(
                    CAST(missing_month AS VARCHAR(2)), ','
                ) WITHIN GROUP (ORDER BY missing_month) as missing_months
            FROM (
                -- Gerar todos os meses esperados vs existentes
                SELECT 
                    a.crm_id,
                    a.indicator_code,
                    m.month_num as missing_month
                FROM (
                    SELECT DISTINCT crm_id, indicator_code
                    FROM bronze.performance_targets
                    WHERE target_year = @target_year
                      AND is_processed = 0
                ) a
                CROSS JOIN (
                    SELECT 1 as month_num UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL
                    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL
                    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL
                    SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
                ) m
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM bronze.performance_targets t
                    WHERE t.crm_id = a.crm_id
                      AND t.indicator_code = a.indicator_code
                      AND t.target_year = @target_year
                      AND MONTH(TRY_CAST(t.period_start AS DATE)) = m.month_num
                      AND t.is_processed = 0
                )
            ) missing
            GROUP BY crm_id, indicator_code;
            
            -- Contar combinações incompletas
            SELECT @incomplete_count = COUNT(*)
            FROM #completeness_check
            WHERE months_count < 12;
            
            IF @incomplete_count > 0
            BEGIN
                IF @debug_mode = 1
                BEGIN
                    PRINT '';
                    PRINT 'AVISO: ' + CAST(@incomplete_count AS VARCHAR(10)) + ' combinações com ano incompleto:';
                    
                    SELECT TOP 10
                        crm_id,
                        indicator_code,
                        CAST(12 - months_count AS VARCHAR(2)) + ' meses faltando: ' + missing_months as detalhe
                    FROM #completeness_check
                    WHERE months_count < 12
                    ORDER BY months_count, crm_id;
                END;
                
                -- Continuar processamento mas registrar warning
                UPDATE bronze.performance_targets
                SET processing_notes = ISNULL(processing_notes, '') + 
                    ' | AVISO: Ano incompleto - faltam meses: ' + 
                    (SELECT missing_months FROM #completeness_check c 
                     WHERE c.crm_id = bronze.performance_targets.crm_id 
                       AND c.indicator_code = bronze.performance_targets.indicator_code)
                WHERE target_year = @target_year
                  AND is_processed = 0
                  AND EXISTS (
                      SELECT 1 FROM #completeness_check c
                      WHERE c.crm_id = bronze.performance_targets.crm_id
                        AND c.indicator_code = bronze.performance_targets.indicator_code
                        AND c.months_count < 12
                  );
            END;
            
            DROP TABLE #completeness_check;
        END;
        
        -- ==============================================================================
        -- ETAPA 3: PREPARAR DADOS PARA MERGE
        -- ==============================================================================
        
        -- Criar tabela temporária com dados transformados
        CREATE TABLE #targets_staging (
            crm_id VARCHAR(20),
            indicator_id INT,
            period_type VARCHAR(20),
            period_start DATE,
            period_end DATE,
            target_value DECIMAL(18,4),
            stretch_target DECIMAL(18,4),
            minimum_target DECIMAL(18,4),
            bronze_load_id INT,
            is_valid BIT
        );
        
        -- Transformar e validar dados
        INSERT INTO #targets_staging
        SELECT 
            UPPER(LTRIM(RTRIM(t.crm_id))) as crm_id,
            i.indicator_id,
            'MENSAL' as period_type,
            TRY_CAST(t.period_start AS DATE) as period_start,
            TRY_CAST(t.period_end AS DATE) as period_end,
            TRY_CAST(t.target_value AS DECIMAL(18,4)) as target_value,
            TRY_CAST(t.stretch_value AS DECIMAL(18,4)) as stretch_target,
            TRY_CAST(t.minimum_value AS DECIMAL(18,4)) as minimum_target,
            t.load_id as bronze_load_id,
            CASE 
                WHEN i.indicator_id IS NULL THEN 0
                WHEN TRY_CAST(t.period_start AS DATE) IS NULL THEN 0
                WHEN TRY_CAST(t.target_value AS DECIMAL(18,4)) IS NULL THEN 0
                ELSE 1
            END as is_valid
        FROM bronze.performance_targets t
        LEFT JOIN silver.performance_indicators i 
            ON UPPER(LTRIM(RTRIM(t.indicator_code))) = i.indicator_code
        WHERE t.target_year = @target_year
          AND t.is_processed = 0;
        
        -- Contar registros inválidos
        SELECT @error_count = COUNT(*)
        FROM #targets_staging
        WHERE is_valid = 0;
        
        IF @error_count > 0
        BEGIN
            IF @debug_mode = 1
            BEGIN
                PRINT '';
                PRINT 'ERRO: ' + CAST(@error_count AS VARCHAR(10)) + ' registros inválidos encontrados:';
                
                -- Mostrar detalhes dos erros
                SELECT TOP 10
                    crm_id,
                    CASE 
                        WHEN indicator_id IS NULL THEN 'Indicador não encontrado'
                        WHEN period_start IS NULL THEN 'Data inválida'
                        WHEN target_value IS NULL THEN 'Valor de meta inválido'
                        ELSE 'Erro desconhecido'
                    END as tipo_erro,
                    bronze_load_id
                FROM #targets_staging
                WHERE is_valid = 0;
            END;
            
            -- Marcar registros inválidos no bronze
            UPDATE b
            SET b.processing_status = 'ERROR',
                b.processing_notes = 
                    CASE 
                        WHEN s.indicator_id IS NULL THEN 'Indicador não cadastrado: ' + b.indicator_code
                        WHEN s.period_start IS NULL THEN 'Data inválida: ' + ISNULL(b.period_start, 'NULL')
                        WHEN s.target_value IS NULL THEN 'Valor de meta inválido: ' + ISNULL(b.target_value, 'NULL')
                        ELSE 'Erro de validação'
                    END,
                b.processing_date = GETDATE()
            FROM bronze.performance_targets b
            INNER JOIN #targets_staging s ON b.load_id = s.bronze_load_id
            WHERE s.is_valid = 0;
        END;
        
        -- ==============================================================================
        -- ETAPA 4: MERGE COM TABELA DE DESTINO
        -- ==============================================================================
        
        -- Executar MERGE apenas com registros válidos
        WITH valid_targets AS (
            SELECT * FROM #targets_staging WHERE is_valid = 1
        )
        MERGE silver.performance_targets AS target
        USING valid_targets AS source
            ON target.crm_id = source.crm_id
           AND target.indicator_id = source.indicator_id
           AND target.period_start = source.period_start
        
        -- Atualizar registros existentes se houver mudança
        WHEN MATCHED AND (
            target.target_value <> source.target_value OR
            ISNULL(target.stretch_target, -999999) <> ISNULL(source.stretch_target, -999999) OR
            ISNULL(target.minimum_target, -999999) <> ISNULL(source.minimum_target, -999999)
        ) THEN
            UPDATE SET 
                target_value = source.target_value,
                stretch_target = source.stretch_target,
                minimum_target = source.minimum_target,
                period_end = source.period_end,
                modified_date = GETDATE(),
                modified_by = SUSER_SNAME(),
                bronze_load_id = source.bronze_load_id
        
        -- Inserir novos registros
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                crm_id, indicator_id, period_type, period_start, period_end,
                target_value, stretch_target, minimum_target,
                is_active, created_date, created_by,
                source_system, bronze_load_id
            )
            VALUES (
                source.crm_id, source.indicator_id, source.period_type,
                source.period_start, source.period_end,
                source.target_value, source.stretch_target, source.minimum_target,
                1, GETDATE(), SUSER_SNAME(),
                'GoogleSheets', source.bronze_load_id
            );
        
        -- Capturar contadores
        SET @inserted_count = @@ROWCOUNT;
        
        -- Calcular updates (diferença do total válido)
        SELECT @updated_count = COUNT(*) - @inserted_count
        FROM #targets_staging
        WHERE is_valid = 1;
        
        -- ==============================================================================
        -- ETAPA 5: MARCAR REGISTROS COMO PROCESSADOS
        -- ==============================================================================
        
        UPDATE bronze.performance_targets
        SET is_processed = 1,
            processing_date = GETDATE(),
            processing_status = 'SUCCESS'
        WHERE target_year = @target_year
          AND is_processed = 0
          AND load_id IN (SELECT bronze_load_id FROM #targets_staging WHERE is_valid = 1);
        
        -- ==============================================================================
        -- ETAPA 6: GERAR RELATÓRIO DE PROCESSAMENTO
        -- ==============================================================================
        
        -- Relatório resumido
        SELECT 
            @row_count as total_processed,
            @inserted_count as inserted_count,
            @updated_count as updated_count,
            @error_count as error_count,
            @incomplete_count as incomplete_combinations,
            DATEDIFF(SECOND, @start_time, GETDATE()) as processing_seconds;
        
        -- Em modo debug, mostrar estatísticas adicionais
        IF @debug_mode = 1
        BEGIN
            -- Estatísticas por indicador
            SELECT 
                i.indicator_code,
                i.indicator_name,
                COUNT(DISTINCT t.crm_id) as assessores,
                COUNT(*) as total_metas,
                AVG(t.target_value) as avg_target,
                SUM(t.target_value) as total_target
            FROM silver.performance_targets t
            INNER JOIN silver.performance_indicators i ON t.indicator_id = i.indicator_id
            WHERE YEAR(t.period_start) = @target_year
            GROUP BY i.indicator_code, i.indicator_name
            ORDER BY i.indicator_code;
            
            PRINT '';
            PRINT 'Processamento concluído com sucesso!';
            PRINT 'Tempo total: ' + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS VARCHAR(10)) + ' segundos';
        END;
        
        -- Limpar tabelas temporárias
        DROP TABLE #targets_staging;
        
        -- Commit apenas se iniciamos a transação
        IF @trancount = 0 AND @@TRANCOUNT > 0
            COMMIT TRANSACTION;
            
        RETURN 0;
        
    END TRY
    BEGIN CATCH
        -- Rollback apenas se iniciamos a transação
        IF @trancount = 0 AND @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Capturar informações do erro
        SELECT 
            @error_message = ERROR_MESSAGE(),
            @error_severity = ERROR_SEVERITY(),
            @error_state = ERROR_STATE();
        
        -- Log do erro
        PRINT 'ERRO: ' + @error_message;
        
        -- Marcar todos os registros como erro
        UPDATE bronze.performance_targets
        SET processing_status = 'ERROR',
            processing_notes = 'Erro no processamento: ' + @error_message,
            processing_date = GETDATE()
        WHERE target_year = @target_year
          AND is_processed = 0;
        
        -- Re-lançar o erro
        RAISERROR(@error_message, @error_severity, @error_state);
        RETURN 1;
        
    END CATCH;
END;
GO

-- ==============================================================================
-- 7. PERMISSÕES
-- ==============================================================================

-- Conceder permissões de execução
-- GRANT EXECUTE ON [bronze].[prc_bronze_to_silver_performance_targets] TO [etl_user];
GO

-- ==============================================================================
-- 8. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================

/*
-- Verificar registros pendentes de processamento
SELECT 
    target_year,
    COUNT(*) as pending_records,
    COUNT(DISTINCT crm_id) as unique_assessors,
    COUNT(DISTINCT indicator_code) as unique_indicators
FROM bronze.performance_targets
WHERE is_processed = 0
GROUP BY target_year
ORDER BY target_year DESC;

-- Analisar erros de processamento
SELECT 
    processing_status,
    processing_notes,
    COUNT(*) as count
FROM bronze.performance_targets
WHERE processing_status = 'ERROR'
GROUP BY processing_status, processing_notes
ORDER BY count DESC;

-- Verificar últimas execuções
SELECT TOP 10
    MIN(processing_date) as start_time,
    MAX(processing_date) as end_time,
    COUNT(*) as records_processed,
    SUM(CASE WHEN processing_status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
    SUM(CASE WHEN processing_status = 'ERROR' THEN 1 ELSE 0 END) as error_count
FROM bronze.performance_targets
WHERE processing_date IS NOT NULL
GROUP BY CAST(processing_date AS DATE)
ORDER BY CAST(processing_date AS DATE) DESC;
*/

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                    | Descrição
--------|------------|--------------------------|--------------------------------------------
1.0.0   | 2025-01-17 | bruno.chiaramonti       | Criação inicial da procedure

*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Processa apenas registros não processados (is_processed = 0)
- Validação de completude é opcional mas recomendada
- MERGE atualiza apenas se houver mudança nos valores
- Erros são registrados mas não impedem processamento dos demais
- Transação garante atomicidade do processamento

Troubleshooting comum:
1. "Indicador não encontrado": Cadastrar em silver.performance_indicators
2. "Data inválida": Verificar formato no Google Sheets
3. "Ano incompleto": Normal em início de ano ou para novos assessores
4. Timeout: Processar por lotes menores se volume > 5000

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/

-- Confirmar criação
PRINT 'Procedure bronze.prc_bronze_to_silver_performance_targets criada com sucesso!';
GO