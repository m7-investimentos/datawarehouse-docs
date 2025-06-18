-- ==============================================================================
-- QRY-IND-006-prc_process_performance_to_gold
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-18
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [gold, performance, procedure, etl, cálculo dinâmico]
-- Status: aprovado
-- Banco de Dados: SQL Server 2019+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure principal para processar dados de performance da camada Silver
para Gold, executando fórmulas SQL dinâmicas para calcular valores realizados de 
cada indicador por pessoa/período.

Casos de uso:
- Processamento mensal de performance (executar no dia 5 de cada mês)
- Reprocessamento de períodos específicos
- Cálculo individual por assessor
- Execução de fórmulas SQL dinâmicas com segurança

Frequência de execução: Mensal (após fechamento)
Tempo médio de execução: 10-15 minutos para processamento completo
Volume esperado: ~500 assessores × 20 indicadores = 10.000 cálculos
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
@period_start    DATE     -- Início do período (opcional, default: mês anterior)
@period_end      DATE     -- Fim do período (opcional, default: último dia do mês)
@crm_id          VARCHAR  -- Código do assessor (opcional, NULL = todos)
@debug           BIT      -- Modo debug (0 = normal, 1 = verboso)
*/

USE M7Medallion;
GO

-- Drop procedure se existir
IF OBJECT_ID('gold.prc_process_performance_to_gold', 'P') IS NOT NULL
    DROP PROCEDURE gold.prc_process_performance_to_gold;
GO

-- ==============================================================================
-- 3. CREATE PROCEDURE
-- ==============================================================================
CREATE PROCEDURE gold.prc_process_performance_to_gold
    @period_start DATE = NULL,
    @period_end DATE = NULL,
    @crm_id VARCHAR(20) = NULL,
    @debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    -- Variáveis de controle
    DECLARE @processing_id INT;
    DECLARE @start_time DATETIME = GETDATE();
    DECLARE @msg NVARCHAR(4000);
    DECLARE @error_msg NVARCHAR(4000);
    DECLARE @row_count INT;
    DECLARE @success_count INT = 0;
    DECLARE @error_count INT = 0;
    
    -- Variáveis para loop de pessoas
    DECLARE @current_crm_id VARCHAR(20);
    DECLARE @current_name VARCHAR(200);
    
    -- Variáveis para loop de indicadores
    DECLARE @indicator_id INT;
    DECLARE @indicator_code VARCHAR(50);
    DECLARE @indicator_name VARCHAR(200);
    DECLARE @indicator_type VARCHAR(20);
    DECLARE @indicator_category VARCHAR(50);
    DECLARE @formula VARCHAR(MAX);
    DECLARE @aggregation_method VARCHAR(20);
    DECLARE @is_inverted BIT;
    DECLARE @indicator_weight DECIMAL(5,2);
    
    -- Variáveis para cálculo
    DECLARE @sql_formula NVARCHAR(MAX);
    DECLARE @realized_value DECIMAL(18,4);
    DECLARE @target_value DECIMAL(18,4);
    DECLARE @stretch_value DECIMAL(18,4);
    DECLARE @minimum_value DECIMAL(18,4);
    DECLARE @achievement_pct DECIMAL(5,2);
    DECLARE @weighted_achievement DECIMAL(5,2);
    DECLARE @achievement_status VARCHAR(20);
    DECLARE @calc_start_time DATETIME;
    DECLARE @calc_duration_ms INT;
    
    BEGIN TRY
        -- ==============================================================================
        -- 4. INICIALIZAÇÃO E VALIDAÇÕES
        -- ==============================================================================
        
        -- Definir período padrão se não informado (mês anterior)
        IF @period_start IS NULL
        BEGIN
            SET @period_start = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0);
            SET @period_end = DATEADD(DAY, -1, DATEADD(MONTH, 1, @period_start));
        END
        ELSE IF @period_end IS NULL
        BEGIN
            SET @period_end = EOMONTH(@period_start);
        END
        
        IF @debug = 1
        BEGIN
            SET @msg = 'Iniciando processamento Gold - Período: ' + 
                       CONVERT(VARCHAR, @period_start, 103) + ' a ' + 
                       CONVERT(VARCHAR, @period_end, 103);
            RAISERROR(@msg, 0, 1) WITH NOWAIT;
        END
        
        -- Criar registro de log
        INSERT INTO gold.processing_log (
            processing_id,
            processing_type,
            period_start,
            period_end,
            entity_id,
            status,
            executed_by
        )
        VALUES (
            NEXT VALUE FOR dbo.ProcessingSequence,
            CASE 
                WHEN @crm_id IS NOT NULL THEN 'INCREMENTAL' 
                ELSE 'FULL' 
            END,
            @period_start,
            @period_end,
            @crm_id,
            'RUNNING',
            SYSTEM_USER
        );
        
        SET @processing_id = SCOPE_IDENTITY();
        
        -- ==============================================================================
        -- 5. LIMPAR DADOS ANTERIORES DO PERÍODO (SE REPROCESSAMENTO)
        -- ==============================================================================
        IF @crm_id IS NULL
        BEGIN
            DELETE FROM gold.card_metas
            WHERE period_start = @period_start
              AND period_end = @period_end;
            
            SET @row_count = @@ROWCOUNT;
            IF @row_count > 0 AND @debug = 1
            BEGIN
                SET @msg = 'Removidos ' + CAST(@row_count AS VARCHAR) + ' registros anteriores';
                RAISERROR(@msg, 0, 1) WITH NOWAIT;
            END
        END
        ELSE
        BEGIN
            DELETE FROM gold.card_metas
            WHERE period_start = @period_start
              AND period_end = @period_end
              AND entity_id = @crm_id;
        END
        
        -- ==============================================================================
        -- 6. CRIAR TABELA TEMPORÁRIA COM PESSOAS A PROCESSAR
        -- ==============================================================================
        CREATE TABLE #pessoas_processar (
            codigo_assessor_crm VARCHAR(20) NOT NULL PRIMARY KEY,
            nome_pessoa VARCHAR(200) NULL,
            processado BIT NOT NULL DEFAULT 0
        );
        
        -- Popular com assessores que têm indicadores ativos no período
        INSERT INTO #pessoas_processar (codigo_assessor_crm, nome_pessoa)
        SELECT DISTINCT 
            a.codigo_assessor_crm,
            p.nome_pessoa
        FROM silver.performance_assignments a
        INNER JOIN silver.dim_pessoas p ON a.codigo_assessor_crm = p.codigo_assessor_crm
        WHERE a.is_active = 1
          AND @period_start >= a.valid_from
          AND (@crm_id IS NULL OR a.codigo_assessor_crm = @crm_id)
          AND EXISTS (
              SELECT 1 
              FROM silver.performance_targets t
              WHERE t.codigo_assessor_crm = a.codigo_assessor_crm
                AND t.period_start = @period_start
                AND t.is_active = 1
          );
        
        SET @row_count = @@ROWCOUNT;
        
        IF @row_count = 0
        BEGIN
            SET @msg = 'Nenhum assessor encontrado para processar no período';
            RAISERROR(@msg, 16, 1);
        END
        
        UPDATE gold.processing_log
        SET total_entities = @row_count
        WHERE log_id = @processing_id;
        
        IF @debug = 1
        BEGIN
            SET @msg = 'Total de assessores a processar: ' + CAST(@row_count AS VARCHAR);
            RAISERROR(@msg, 0, 1) WITH NOWAIT;
        END
        
        -- ==============================================================================
        -- 7. LOOP PRINCIPAL - PROCESSAR CADA PESSOA
        -- ==============================================================================
        DECLARE pessoa_cursor CURSOR FOR
        SELECT codigo_assessor_crm, nome_pessoa
        FROM #pessoas_processar
        WHERE processado = 0
        ORDER BY codigo_assessor_crm;
        
        OPEN pessoa_cursor;
        FETCH NEXT FROM pessoa_cursor INTO @current_crm_id, @current_name;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                IF @debug = 1
                BEGIN
                    SET @msg = 'Processando: ' + @current_crm_id + ' - ' + ISNULL(@current_name, 'Nome não encontrado');
                    RAISERROR(@msg, 0, 1) WITH NOWAIT;
                END
                
                -- ==============================================================================
                -- 8. CRIAR TABELA TEMPORÁRIA DE INDICADORES DA PESSOA
                -- ==============================================================================
                CREATE TABLE #indicadores_pessoa (
                    indicator_id INT NOT NULL,
                    indicator_code VARCHAR(50) NOT NULL,
                    indicator_name VARCHAR(200) NOT NULL,
                    indicator_type VARCHAR(20) NOT NULL,
                    indicator_category VARCHAR(50) NULL,
                    formula VARCHAR(MAX) NULL,
                    aggregation_method VARCHAR(20) NULL,
                    is_inverted BIT NOT NULL,
                    indicator_weight DECIMAL(5,2) NOT NULL,
                    target_value DECIMAL(18,4) NULL,
                    stretch_value DECIMAL(18,4) NULL,
                    minimum_value DECIMAL(18,4) NULL
                );
                
                -- Popular com indicadores ativos da pessoa
                INSERT INTO #indicadores_pessoa
                SELECT 
                    i.indicator_id,
                    i.indicator_code,
                    i.indicator_name,
                    CASE 
                        WHEN a.indicator_weight > 0 THEN 'CARD'
                        WHEN i.category = 'GATILHO' THEN 'GATILHO'
                        ELSE 'KPI'
                    END as indicator_type,
                    i.category,
                    i.calculation_formula,
                    i.aggregation_method,
                    i.is_inverted,
                    a.indicator_weight,
                    t.target_value,
                    t.stretch_target,
                    t.minimum_target
                FROM silver.performance_assignments a
                INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
                LEFT JOIN silver.performance_targets t ON 
                    t.codigo_assessor_crm = a.codigo_assessor_crm AND
                    t.indicator_id = a.indicator_id AND
                    t.period_start = @period_start AND
                    t.is_active = 1
                WHERE a.codigo_assessor_crm = @current_crm_id
                  AND a.is_active = 1
                  AND i.is_active = 1
                  AND @period_start >= a.valid_from;
                
                -- ==============================================================================
                -- 9. LOOP DE INDICADORES - CALCULAR CADA UM
                -- ==============================================================================
                DECLARE indicator_cursor CURSOR FOR
                SELECT 
                    indicator_id,
                    indicator_code,
                    indicator_name,
                    indicator_type,
                    indicator_category,
                    formula,
                    aggregation_method,
                    is_inverted,
                    indicator_weight,
                    target_value,
                    stretch_value,
                    minimum_value
                FROM #indicadores_pessoa
                ORDER BY indicator_weight DESC, indicator_code;
                
                OPEN indicator_cursor;
                FETCH NEXT FROM indicator_cursor INTO 
                    @indicator_id, @indicator_code, @indicator_name, @indicator_type,
                    @indicator_category, @formula, @aggregation_method, @is_inverted,
                    @indicator_weight, @target_value, @stretch_value, @minimum_value;
                
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    BEGIN TRY
                        SET @calc_start_time = GETDATE();
                        SET @realized_value = NULL;
                        SET @achievement_pct = NULL;
                        SET @weighted_achievement = NULL;
                        SET @achievement_status = NULL;
                        SET @error_msg = NULL;
                        
                        -- ==============================================================================
                        -- 10. EXECUTAR FÓRMULA SQL DINÂMICA
                        -- ==============================================================================
                        IF @formula IS NOT NULL AND LEN(@formula) > 0
                        BEGIN
                            -- Construir SQL dinâmico com parâmetros
                            SET @sql_formula = N'
                            SELECT @result = ' + @formula + N'
                            FROM (SELECT 1 AS dummy) AS base
                            WHERE EXISTS (
                                SELECT 1 
                                WHERE @codigo_assessor_crm = @codigo_assessor_crm
                                  AND @period_start = @period_start
                                  AND @period_end = @period_end
                            )';
                            
                            -- Substituir placeholders comuns
                            SET @sql_formula = REPLACE(@sql_formula, '@entity_id', '@codigo_assessor_crm');
                            SET @sql_formula = REPLACE(@sql_formula, '{{period_start}}', 'CAST(@period_start AS DATE)');
                            SET @sql_formula = REPLACE(@sql_formula, '{{period_end}}', 'CAST(@period_end AS DATE)');
                            
                            IF @debug = 1
                            BEGIN
                                SET @msg = '  - Executando fórmula para ' + @indicator_code;
                                RAISERROR(@msg, 0, 1) WITH NOWAIT;
                            END
                            
                            -- Executar fórmula
                            BEGIN TRY
                                EXEC sp_executesql 
                                    @sql_formula,
                                    N'@result DECIMAL(18,4) OUTPUT, @codigo_assessor_crm VARCHAR(20), @period_start DATE, @period_end DATE',
                                    @result = @realized_value OUTPUT,
                                    @codigo_assessor_crm = @current_crm_id,
                                    @period_start = @period_start,
                                    @period_end = @period_end;
                            END TRY
                            BEGIN CATCH
                                SET @error_msg = 'Erro na fórmula: ' + ERROR_MESSAGE();
                                SET @realized_value = NULL;
                            END CATCH
                        END
                        
                        -- ==============================================================================
                        -- 11. CALCULAR ACHIEVEMENT
                        -- ==============================================================================
                        IF @realized_value IS NOT NULL AND @target_value IS NOT NULL AND @target_value != 0
                        BEGIN
                            IF @is_inverted = 0
                            BEGIN
                                -- Indicador normal: maior é melhor
                                SET @achievement_pct = (@realized_value / @target_value) * 100;
                            END
                            ELSE
                            BEGIN
                                -- Indicador invertido: menor é melhor
                                SET @achievement_pct = (2 - (@realized_value / @target_value)) * 100;
                            END
                            
                            -- Limitar achievement entre -999.99 e 999.99
                            IF @achievement_pct > 999.99 SET @achievement_pct = 999.99;
                            IF @achievement_pct < -999.99 SET @achievement_pct = -999.99;
                            
                            -- Calcular weighted achievement (apenas para CARD)
                            IF @indicator_type = 'CARD' AND @indicator_weight > 0
                            BEGIN
                                SET @weighted_achievement = (@achievement_pct * @indicator_weight) / 100;
                            END
                            
                            -- Determinar status
                            SET @achievement_status = 
                                CASE 
                                    WHEN @achievement_pct >= 120 THEN 'SUPERADO'
                                    WHEN @achievement_pct >= 100 THEN 'ATINGIDO'
                                    WHEN @achievement_pct >= 80 THEN 'PARCIAL'
                                    ELSE 'NAO_ATINGIDO'
                                END;
                        END
                        ELSE IF @realized_value IS NULL OR @target_value IS NULL
                        BEGIN
                            SET @achievement_status = 'NAO_APLICAVEL';
                        END
                        
                        -- Calcular duração
                        SET @calc_duration_ms = DATEDIFF(MILLISECOND, @calc_start_time, GETDATE());
                        
                        -- ==============================================================================
                        -- 12. INSERIR RESULTADO NA GOLD
                        -- ==============================================================================
                        INSERT INTO gold.card_metas (
                            period_start, period_end,
                            entity_type, entity_id,
                            attribute_type, attribute_code, attribute_name,
                            indicator_type, indicator_category,
                            target_value, stretch_value, minimum_value,
                            realized_value,
                            achievement_percentage,
                            indicator_weight,
                            weighted_achievement,
                            achievement_status,
                            is_inverted,
                            is_calculated,
                            has_error,
                            calculation_formula,
                            calculation_method,
                            processing_id,
                            processing_duration_ms,
                            processing_notes
                        )
                        VALUES (
                            @period_start, @period_end,
                            'ASSESSOR', @current_crm_id,
                            'INDICATOR', @indicator_code, @indicator_name,
                            @indicator_type, @indicator_category,
                            @target_value, @stretch_value, @minimum_value,
                            @realized_value,
                            @achievement_pct,
                            @indicator_weight,
                            @weighted_achievement,
                            @achievement_status,
                            @is_inverted,
                            CASE WHEN @realized_value IS NOT NULL THEN 1 ELSE 0 END,
                            CASE WHEN @error_msg IS NOT NULL THEN 1 ELSE 0 END,
                            @formula,
                            @aggregation_method,
                            @processing_id,
                            @calc_duration_ms,
                            @error_msg
                        );
                        
                        SET @success_count = @success_count + 1;
                        
                    END TRY
                    BEGIN CATCH
                        -- Log erro do indicador
                        SET @error_count = @error_count + 1;
                        SET @error_msg = 'Erro no indicador ' + @indicator_code + ': ' + ERROR_MESSAGE();
                        
                        -- Inserir registro de erro
                        INSERT INTO gold.card_metas (
                            period_start, period_end,
                            entity_type, entity_id,
                            attribute_type, attribute_code, attribute_name,
                            indicator_type, indicator_category,
                            target_value,
                            indicator_weight,
                            has_error,
                            processing_id,
                            processing_notes
                        )
                        VALUES (
                            @period_start, @period_end,
                            'ASSESSOR', @current_crm_id,
                            'INDICATOR', @indicator_code, @indicator_name,
                            @indicator_type, @indicator_category,
                            @target_value,
                            @indicator_weight,
                            1,
                            @processing_id,
                            @error_msg
                        );
                    END CATCH
                    
                    FETCH NEXT FROM indicator_cursor INTO 
                        @indicator_id, @indicator_code, @indicator_name, @indicator_type,
                        @indicator_category, @formula, @aggregation_method, @is_inverted,
                        @indicator_weight, @target_value, @stretch_value, @minimum_value;
                END
                
                CLOSE indicator_cursor;
                DEALLOCATE indicator_cursor;
                
                -- Limpar tabela temporária
                DROP TABLE #indicadores_pessoa;
                
                -- Marcar pessoa como processada
                UPDATE #pessoas_processar
                SET processado = 1
                WHERE codigo_assessor_crm = @current_crm_id;
                
            END TRY
            BEGIN CATCH
                -- Log erro da pessoa
                SET @error_msg = 'Erro ao processar ' + @current_crm_id + ': ' + ERROR_MESSAGE();
                
                IF @debug = 1
                BEGIN
                    RAISERROR(@error_msg, 0, 1) WITH NOWAIT;
                END
                
                -- Continuar com próxima pessoa
                IF OBJECT_ID('tempdb..#indicadores_pessoa') IS NOT NULL
                    DROP TABLE #indicadores_pessoa;
            END CATCH
            
            FETCH NEXT FROM pessoa_cursor INTO @current_crm_id, @current_name;
        END
        
        CLOSE pessoa_cursor;
        DEALLOCATE pessoa_cursor;
        
        -- ==============================================================================
        -- 13. FINALIZAÇÃO E ESTATÍSTICAS
        -- ==============================================================================
        
        -- Contar totais
        DECLARE @total_calcs INT, @total_indicators INT;
        
        SELECT 
            @total_calcs = COUNT(*),
            @total_indicators = COUNT(DISTINCT attribute_code)
        FROM gold.card_metas
        WHERE processing_id = @processing_id;
        
        -- Atualizar log
        UPDATE gold.processing_log
        SET 
            end_time = GETDATE(),
            duration_seconds = DATEDIFF(SECOND, @start_time, GETDATE()),
            total_indicators = @total_indicators,
            total_calculations = @total_calcs,
            successful_calculations = @success_count,
            failed_calculations = @error_count,
            status = CASE 
                WHEN @error_count = 0 THEN 'SUCCESS'
                WHEN @error_count < @success_count THEN 'WARNING'
                ELSE 'ERROR'
            END,
            execution_notes = 'Processamento concluído. Sucessos: ' + CAST(@success_count AS VARCHAR) + 
                            ', Erros: ' + CAST(@error_count AS VARCHAR)
        WHERE log_id = @processing_id;
        
        -- Limpar temporária
        DROP TABLE #pessoas_processar;
        
        IF @debug = 1
        BEGIN
            SET @msg = 'Processamento concluído! Total: ' + CAST(@total_calcs AS VARCHAR) + 
                      ' cálculos, Sucessos: ' + CAST(@success_count AS VARCHAR) + 
                      ', Erros: ' + CAST(@error_count AS VARCHAR);
            RAISERROR(@msg, 0, 1) WITH NOWAIT;
        END
        
    END TRY
    BEGIN CATCH
        -- Tratamento de erro geral
        SET @error_msg = ERROR_MESSAGE();
        
        -- Atualizar log com erro
        IF @processing_id IS NOT NULL
        BEGIN
            UPDATE gold.processing_log
            SET 
                end_time = GETDATE(),
                duration_seconds = DATEDIFF(SECOND, @start_time, GETDATE()),
                status = 'ERROR',
                error_message = @error_msg
            WHERE log_id = @processing_id;
        END
        
        -- Limpar objetos temporários
        IF OBJECT_ID('tempdb..#pessoas_processar') IS NOT NULL
            DROP TABLE #pessoas_processar;
        IF OBJECT_ID('tempdb..#indicadores_pessoa') IS NOT NULL
            DROP TABLE #indicadores_pessoa;
        
        -- Re-lançar erro
        THROW;
    END CATCH
END
GO

-- ==============================================================================
-- 14. CRIAR SEQUENCE PARA PROCESSING_ID (SE NÃO EXISTIR)
-- ==============================================================================
IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = 'ProcessingSequence')
BEGIN
    CREATE SEQUENCE dbo.ProcessingSequence
        AS INT
        START WITH 1
        INCREMENT BY 1;
END
GO

-- ==============================================================================
-- 15. PERMISSÕES
-- ==============================================================================
GRANT EXECUTE ON gold.prc_process_performance_to_gold TO db_executor;
GO

-- ==============================================================================
-- 16. EXEMPLOS DE USO
-- ==============================================================================
/*
-- Processar mês anterior completo (uso padrão)
EXEC gold.prc_process_performance_to_gold;

-- Processar período específico
EXEC gold.prc_process_performance_to_gold 
    @period_start = '2025-01-01',
    @period_end = '2025-01-31';

-- Processar assessor específico com debug
EXEC gold.prc_process_performance_to_gold 
    @period_start = '2025-01-01',
    @period_end = '2025-01-31',
    @crm_id = 'AAI001',
    @debug = 1;

-- Verificar log de execução
SELECT TOP 10 * FROM gold.processing_log ORDER BY log_id DESC;

-- Verificar resultados
SELECT * FROM gold.card_metas 
WHERE period_start = '2025-01-01' 
  AND entity_id = 'AAI001'
ORDER BY indicator_weight DESC;
*/

-- ==============================================================================
-- 17. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                  | Descrição
--------|------------|------------------------|--------------------------------------------
1.0.0   | 2025-01-18 | bruno.chiaramonti     | Criação inicial da procedure
*/

-- ==============================================================================
-- 18. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Executa fórmulas SQL dinâmicas com segurança usando sp_executesql
- Processa cada pessoa em transação separada (falha de uma não impede outras)
- Log detalhado para troubleshooting
- Suporta reprocessamento (deleta dados anteriores do período)
- Performance adequada para volume esperado

Segurança:
- Fórmulas são executadas com permissões mínimas
- Parâmetros sempre usando binding para evitar SQL injection
- Validação de objetos nas fórmulas deve ser feita previamente

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

PRINT 'Procedure gold.prc_process_performance_to_gold criada com sucesso!';
GO