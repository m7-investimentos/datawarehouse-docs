-- ==============================================================================
-- QRY-IND-008-prc_validate_processing
-- ==============================================================================
-- Tipo: Stored Procedure - Validação
-- Versão: 1.0.0
-- Última atualização: 2025-01-18
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [gold, validação, qualidade, procedure, performance]
-- Status: aprovado
-- Banco de Dados: SQL Server 2019+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure de validação para garantir a qualidade e integridade dos 
dados processados na camada Gold. Executa validações críticas de negócio e 
técnicas, gerando relatório de inconsistências.

Casos de uso:
- Executar após processamento Gold para validar resultados
- Verificar integridade antes de liberação para consumo
- Auditoria periódica de qualidade de dados
- Troubleshooting de problemas reportados

Frequência de execução: Após cada processamento Gold
Tempo médio de execução: 1-2 minutos
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
@period_start    DATE     -- Início do período (opcional, default: último processado)
@period_end      DATE     -- Fim do período (opcional)
@validation_type VARCHAR  -- Tipo validação: 'FULL', 'BASIC', 'WEIGHTS' (default: FULL)
@fix_issues      BIT      -- Tentar corrigir problemas encontrados (default: 0)
@debug           BIT      -- Modo debug (0 = normal, 1 = verboso)
*/

USE M7Medallion;
GO

-- Drop procedure se existir
IF OBJECT_ID('gold.prc_validate_processing', 'P') IS NOT NULL
    DROP PROCEDURE gold.prc_validate_processing;
GO

-- ==============================================================================
-- 3. CREATE PROCEDURE
-- ==============================================================================
CREATE PROCEDURE gold.prc_validate_processing
    @period_start DATE = NULL,
    @period_end DATE = NULL,
    @validation_type VARCHAR(20) = 'FULL',
    @fix_issues BIT = 0,
    @debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Variáveis de controle
    DECLARE @validation_id INT;
    DECLARE @start_time DATETIME = GETDATE();
    DECLARE @msg NVARCHAR(4000);
    DECLARE @error_count INT = 0;
    DECLARE @warning_count INT = 0;
    DECLARE @validation_passed BIT = 1;
    
    BEGIN TRY
        -- ==============================================================================
        -- 4. INICIALIZAÇÃO
        -- ==============================================================================
        
        -- Determinar período se não informado (último processado)
        IF @period_start IS NULL
        BEGIN
            SELECT TOP 1 
                @period_start = period_start,
                @period_end = period_end
            FROM gold.card_metas
            ORDER BY period_start DESC;
            
            IF @period_start IS NULL
            BEGIN
                RAISERROR('Nenhum período encontrado para validar', 16, 1);
                RETURN;
            END
        END
        
        IF @debug = 1
        BEGIN
            SET @msg = 'Iniciando validação - Período: ' + 
                       CONVERT(VARCHAR, @period_start, 103) + ' a ' + 
                       CONVERT(VARCHAR, @period_end, 103) +
                       ' - Tipo: ' + @validation_type;
            RAISERROR(@msg, 0, 1) WITH NOWAIT;
        END
        
        -- Criar tabela temporária para resultados
        CREATE TABLE #validation_results (
            validation_id INT IDENTITY(1,1),
            validation_type VARCHAR(50),
            severity VARCHAR(20), -- ERROR, WARNING, INFO
            entity_id VARCHAR(20),
            indicator_code VARCHAR(50),
            issue_description VARCHAR(1000),
            details VARCHAR(MAX),
            records_affected INT,
            can_be_fixed BIT DEFAULT 0,
            fixed BIT DEFAULT 0
        );
        
        -- ==============================================================================
        -- 5. VALIDAÇÃO 1: COMPLETUDE - TODOS ASSESSORES PROCESSADOS
        -- ==============================================================================
        IF @validation_type IN ('FULL', 'BASIC')
        BEGIN
            IF @debug = 1 RAISERROR('Validação 1: Verificando completude de assessores...', 0, 1) WITH NOWAIT;
            
            INSERT INTO #validation_results (
                validation_type, severity, entity_id, 
                issue_description, details, records_affected
            )
            SELECT 
                'COMPLETUDE_ASSESSOR' as validation_type,
                'ERROR' as severity,
                a.codigo_assessor_crm,
                'Assessor sem dados processados no período',
                'Assessor ativo com indicadores mas sem registros em card_metas',
                1
            FROM (
                -- Assessores que deveriam ter dados
                SELECT DISTINCT codigo_assessor_crm
                FROM silver.performance_assignments
                WHERE is_active = 1
                  AND @period_start >= valid_from
                  AND EXISTS (
                      SELECT 1 FROM silver.performance_targets t
                      WHERE t.codigo_assessor_crm = performance_assignments.codigo_assessor_crm
                        AND t.period_start = @period_start
                  )
            ) a
            LEFT JOIN (
                -- Assessores que têm dados
                SELECT DISTINCT entity_id as codigo_assessor_crm
                FROM gold.card_metas
                WHERE period_start = @period_start
            ) g ON a.codigo_assessor_crm = g.codigo_assessor_crm
            WHERE g.codigo_assessor_crm IS NULL;
            
            SET @error_count = @error_count + @@ROWCOUNT;
        END
        
        -- ==============================================================================
        -- 6. VALIDAÇÃO 2: PESOS CARD - SOMA DEVE SER 100%
        -- ==============================================================================
        IF @validation_type IN ('FULL', 'WEIGHTS')
        BEGIN
            IF @debug = 1 RAISERROR('Validação 2: Verificando soma de pesos CARD...', 0, 1) WITH NOWAIT;
            
            WITH WeightCheck AS (
                SELECT 
                    entity_id,
                    SUM(indicator_weight) as soma_pesos,
                    COUNT(*) as qtd_card
                FROM gold.card_metas
                WHERE period_start = @period_start
                  AND indicator_type = 'CARD'
                GROUP BY entity_id
                HAVING ABS(SUM(indicator_weight) - 100) >= 0.01
            )
            INSERT INTO #validation_results (
                validation_type, severity, entity_id, 
                issue_description, details, records_affected, can_be_fixed
            )
            SELECT 
                'SOMA_PESOS_CARD',
                'ERROR',
                entity_id,
                'Soma dos pesos CARD diferente de 100%',
                'Soma: ' + CAST(soma_pesos AS VARCHAR) + '% (' + 
                CAST(qtd_card AS VARCHAR) + ' indicadores)',
                qtd_card,
                1
            FROM WeightCheck;
            
            SET @error_count = @error_count + @@ROWCOUNT;
            
            -- Corrigir se solicitado (normalizar pesos)
            IF @fix_issues = 1 AND EXISTS (SELECT 1 FROM #validation_results WHERE validation_type = 'SOMA_PESOS_CARD' AND can_be_fixed = 1)
            BEGIN
                UPDATE cm
                SET 
                    indicator_weight = (cm.indicator_weight / wc.soma_pesos) * 100,
                    modified_date = GETDATE(),
                    modified_by = SYSTEM_USER + '_VALIDATION_FIX',
                    processing_notes = ISNULL(processing_notes + ' | ', '') + 
                                     'Peso ajustado de ' + CAST(cm.indicator_weight AS VARCHAR) + 
                                     ' para normalizar 100%'
                FROM gold.card_metas cm
                INNER JOIN (
                    SELECT entity_id, SUM(indicator_weight) as soma_pesos
                    FROM gold.card_metas
                    WHERE period_start = @period_start
                      AND indicator_type = 'CARD'
                    GROUP BY entity_id
                    HAVING ABS(SUM(indicator_weight) - 100) >= 0.01
                ) wc ON cm.entity_id = wc.entity_id
                WHERE cm.period_start = @period_start
                  AND cm.indicator_type = 'CARD';
                
                UPDATE #validation_results
                SET fixed = 1
                WHERE validation_type = 'SOMA_PESOS_CARD';
                
                IF @debug = 1 RAISERROR('  - Pesos normalizados para somar 100%', 0, 1) WITH NOWAIT;
            END
        END
        
        -- ==============================================================================
        -- 7. VALIDAÇÃO 3: INDICADORES CARD SEM VALOR REALIZADO
        -- ==============================================================================
        IF @validation_type IN ('FULL', 'BASIC')
        BEGIN
            IF @debug = 1 RAISERROR('Validação 3: Verificando indicadores CARD sem valor realizado...', 0, 1) WITH NOWAIT;
            
            INSERT INTO #validation_results (
                validation_type, severity, entity_id, indicator_code,
                issue_description, details, records_affected
            )
            SELECT 
                'CARD_SEM_VALOR',
                'WARNING',
                entity_id,
                attribute_code,
                'Indicador CARD sem valor realizado',
                'Target: ' + CAST(target_value AS VARCHAR) + 
                ', Peso: ' + CAST(indicator_weight AS VARCHAR) + '%',
                1
            FROM gold.card_metas
            WHERE period_start = @period_start
              AND indicator_type = 'CARD'
              AND realized_value IS NULL
              AND has_error = 0;
            
            SET @warning_count = @warning_count + @@ROWCOUNT;
        END
        
        -- ==============================================================================
        -- 8. VALIDAÇÃO 4: TARGETS EXISTEM PARA O PERÍODO
        -- ==============================================================================
        IF @validation_type IN ('FULL')
        BEGIN
            IF @debug = 1 RAISERROR('Validação 4: Verificando existência de targets...', 0, 1) WITH NOWAIT;
            
            INSERT INTO #validation_results (
                validation_type, severity, entity_id, indicator_code,
                issue_description, details, records_affected
            )
            SELECT 
                'TARGET_AUSENTE',
                'ERROR',
                entity_id,
                attribute_code,
                'Indicador sem target definido',
                'Indicador tipo ' + indicator_type + ' processado sem meta',
                1
            FROM gold.card_metas
            WHERE period_start = @period_start
              AND target_value IS NULL
              AND indicator_type IN ('CARD', 'GATILHO');
            
            SET @error_count = @error_count + @@ROWCOUNT;
        END
        
        -- ==============================================================================
        -- 9. VALIDAÇÃO 5: VALORES CALCULADOS FORA DO ESPERADO
        -- ==============================================================================
        IF @validation_type IN ('FULL')
        BEGIN
            IF @debug = 1 RAISERROR('Validação 5: Verificando valores anômalos...', 0, 1) WITH NOWAIT;
            
            -- Achievement muito alto ou muito baixo
            INSERT INTO #validation_results (
                validation_type, severity, entity_id, indicator_code,
                issue_description, details, records_affected
            )
            SELECT 
                'ACHIEVEMENT_ANOMALO',
                CASE 
                    WHEN achievement_percentage > 500 OR achievement_percentage < -100 
                    THEN 'ERROR' 
                    ELSE 'WARNING' 
                END,
                entity_id,
                attribute_code,
                'Achievement fora do range esperado',
                'Achievement: ' + CAST(achievement_percentage AS VARCHAR) + '% ' +
                '(Realizado: ' + CAST(realized_value AS VARCHAR) + 
                ', Target: ' + CAST(target_value AS VARCHAR) + ')',
                1
            FROM gold.card_metas
            WHERE period_start = @period_start
              AND achievement_percentage IS NOT NULL
              AND (achievement_percentage > 300 OR achievement_percentage < -50);
            
            SET @warning_count = @warning_count + @@ROWCOUNT;
        END
        
        -- ==============================================================================
        -- 10. VALIDAÇÃO 6: FÓRMULAS COM ERRO
        -- ==============================================================================
        IF @validation_type IN ('FULL', 'BASIC')
        BEGIN
            IF @debug = 1 RAISERROR('Validação 6: Verificando fórmulas com erro...', 0, 1) WITH NOWAIT;
            
            INSERT INTO #validation_results (
                validation_type, severity, entity_id, indicator_code,
                issue_description, details, records_affected
            )
            SELECT 
                'FORMULA_ERRO',
                'ERROR',
                entity_id,
                attribute_code,
                'Erro ao executar fórmula do indicador',
                LEFT(processing_notes, 500),
                1
            FROM gold.card_metas
            WHERE period_start = @period_start
              AND has_error = 1;
            
            SET @error_count = @error_count + @@ROWCOUNT;
        END
        
        -- ==============================================================================
        -- 11. VALIDAÇÃO 7: WEIGHTED ACHIEVEMENT CONSISTENTE
        -- ==============================================================================
        IF @validation_type IN ('FULL')
        BEGIN
            IF @debug = 1 RAISERROR('Validação 7: Verificando weighted achievement...', 0, 1) WITH NOWAIT;
            
            INSERT INTO #validation_results (
                validation_type, severity, entity_id, indicator_code,
                issue_description, details, records_affected, can_be_fixed
            )
            SELECT 
                'WEIGHTED_INCORRETO',
                'WARNING',
                entity_id,
                attribute_code,
                'Weighted achievement calculado incorretamente',
                'Esperado: ' + CAST(ROUND((achievement_percentage * indicator_weight) / 100, 2) AS VARCHAR) +
                ', Atual: ' + CAST(weighted_achievement AS VARCHAR),
                1,
                1
            FROM gold.card_metas
            WHERE period_start = @period_start
              AND indicator_type = 'CARD'
              AND achievement_percentage IS NOT NULL
              AND indicator_weight > 0
              AND ABS(weighted_achievement - (achievement_percentage * indicator_weight) / 100) > 0.01;
            
            SET @warning_count = @warning_count + @@ROWCOUNT;
            
            -- Corrigir se solicitado
            IF @fix_issues = 1
            BEGIN
                UPDATE gold.card_metas
                SET 
                    weighted_achievement = ROUND((achievement_percentage * indicator_weight) / 100, 2),
                    modified_date = GETDATE(),
                    modified_by = SYSTEM_USER + '_VALIDATION_FIX'
                WHERE period_start = @period_start
                  AND indicator_type = 'CARD'
                  AND achievement_percentage IS NOT NULL
                  AND indicator_weight > 0
                  AND ABS(weighted_achievement - (achievement_percentage * indicator_weight) / 100) > 0.01;
                
                UPDATE #validation_results
                SET fixed = 1
                WHERE validation_type = 'WEIGHTED_INCORRETO';
            END
        END
        
        -- ==============================================================================
        -- 12. VALIDAÇÃO 8: DUPLICATAS
        -- ==============================================================================
        IF @validation_type IN ('FULL', 'BASIC')
        BEGIN
            IF @debug = 1 RAISERROR('Validação 8: Verificando duplicatas...', 0, 1) WITH NOWAIT;
            
            INSERT INTO #validation_results (
                validation_type, severity, entity_id, indicator_code,
                issue_description, details, records_affected
            )
            SELECT 
                'DUPLICATA',
                'ERROR',
                entity_id,
                attribute_code,
                'Registro duplicado encontrado',
                'Quantidade: ' + CAST(COUNT(*) AS VARCHAR),
                COUNT(*)
            FROM gold.card_metas
            WHERE period_start = @period_start
            GROUP BY entity_id, attribute_code, period_start
            HAVING COUNT(*) > 1;
            
            SET @error_count = @error_count + @@ROWCOUNT;
        END
        
        -- ==============================================================================
        -- 13. SUMÁRIO DOS RESULTADOS
        -- ==============================================================================
        DECLARE @total_issues INT = (SELECT COUNT(*) FROM #validation_results);
        DECLARE @issues_fixed INT = (SELECT COUNT(*) FROM #validation_results WHERE fixed = 1);
        
        IF @total_issues = 0
        BEGIN
            SET @validation_passed = 1;
            SET @msg = 'Validação concluída com SUCESSO! Nenhum problema encontrado.';
        END
        ELSE
        BEGIN
            SET @validation_passed = 0;
            SET @msg = 'Validação concluída com PROBLEMAS: ' + 
                      CAST(@error_count AS VARCHAR) + ' erros, ' + 
                      CAST(@warning_count AS VARCHAR) + ' avisos' +
                      CASE WHEN @issues_fixed > 0 
                           THEN ' (' + CAST(@issues_fixed AS VARCHAR) + ' corrigidos)'
                           ELSE '' END;
        END
        
        IF @debug = 1 OR @total_issues > 0
        BEGIN
            RAISERROR(@msg, 0, 1) WITH NOWAIT;
            
            -- Mostrar resumo por tipo
            SELECT 
                validation_type,
                severity,
                COUNT(*) as qtd_issues,
                SUM(records_affected) as total_records_affected,
                SUM(CASE WHEN fixed = 1 THEN 1 ELSE 0 END) as qtd_fixed
            FROM #validation_results
            GROUP BY validation_type, severity
            ORDER BY 
                CASE severity 
                    WHEN 'ERROR' THEN 1 
                    WHEN 'WARNING' THEN 2 
                    ELSE 3 
                END,
                validation_type;
                
            -- Mostrar detalhes se debug
            IF @debug = 1 AND @total_issues > 0
            BEGIN
                SELECT 
                    validation_type,
                    severity,
                    entity_id,
                    indicator_code,
                    issue_description,
                    LEFT(details, 200) as details_truncated,
                    fixed
                FROM #validation_results
                ORDER BY 
                    CASE severity 
                        WHEN 'ERROR' THEN 1 
                        WHEN 'WARNING' THEN 2 
                        ELSE 3 
                    END,
                    entity_id,
                    indicator_code;
            END
        END
        
        -- ==============================================================================
        -- 14. LOG DE VALIDAÇÃO
        -- ==============================================================================
        INSERT INTO gold.processing_log (
            processing_id,
            processing_type,
            period_start,
            period_end,
            start_time,
            end_time,
            duration_seconds,
            status,
            execution_notes,
            executed_by
        )
        VALUES (
            0, -- Usar 0 para validações
            'VALIDATION_' + @validation_type,
            @period_start,
            @period_end,
            @start_time,
            GETDATE(),
            DATEDIFF(SECOND, @start_time, GETDATE()),
            CASE 
                WHEN @error_count = 0 AND @warning_count = 0 THEN 'SUCCESS'
                WHEN @error_count = 0 THEN 'WARNING'
                ELSE 'ERROR'
            END,
            'Validação executada. Erros: ' + CAST(@error_count AS VARCHAR) + 
            ', Avisos: ' + CAST(@warning_count AS VARCHAR) +
            CASE WHEN @issues_fixed > 0 
                 THEN ', Corrigidos: ' + CAST(@issues_fixed AS VARCHAR)
                 ELSE '' END,
            SYSTEM_USER
        );
        
        -- Retornar código de erro se validação falhou com erros
        IF @error_count > 0 AND @fix_issues = 0
        BEGIN
            -- Retornar tabela com problemas para aplicação
            SELECT * FROM #validation_results 
            WHERE severity = 'ERROR'
            ORDER BY entity_id, indicator_code;
            
            RETURN 1; -- Código de erro
        END
        
        DROP TABLE #validation_results;
        RETURN 0; -- Sucesso
        
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#validation_results') IS NOT NULL
            DROP TABLE #validation_results;
            
        THROW;
    END CATCH
END
GO

-- ==============================================================================
-- 15. PERMISSÕES
-- ==============================================================================
GRANT EXECUTE ON gold.prc_validate_processing TO db_executor;
GO

-- ==============================================================================
-- 16. EXEMPLOS DE USO
-- ==============================================================================
/*
-- Validação completa do último período processado
EXEC gold.prc_validate_processing;

-- Validação básica de período específico
EXEC gold.prc_validate_processing 
    @period_start = '2025-01-01',
    @period_end = '2025-01-31',
    @validation_type = 'BASIC';

-- Validação apenas de pesos
EXEC gold.prc_validate_processing 
    @period_start = '2025-01-01',
    @validation_type = 'WEIGHTS';

-- Validação completa com correção automática
EXEC gold.prc_validate_processing 
    @period_start = '2025-01-01',
    @validation_type = 'FULL',
    @fix_issues = 1,
    @debug = 1;

-- Verificar resultado em código de retorno
DECLARE @result INT;
EXEC @result = gold.prc_validate_processing;
IF @result = 0
    PRINT 'Validação passou!'
ELSE
    PRINT 'Validação falhou com erros!';
*/

-- ==============================================================================
-- 17. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                  | Descrição
--------|------------|------------------------|--------------------------------------------
1.0.0   | 2025-01-18 | bruno.chiaramonti     | Criação inicial da procedure de validação
*/

-- ==============================================================================
-- 18. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Validações implementadas:
1. Completude - todos assessores ativos processados
2. Soma de pesos CARD = 100% por pessoa
3. Indicadores CARD têm valor realizado
4. Targets existem para o período
5. Valores de achievement dentro do esperado
6. Fórmulas executadas sem erro
7. Weighted achievement calculado corretamente
8. Sem registros duplicados

Recursos:
- Modo de correção automática para alguns problemas
- Log detalhado de todas as validações
- Retorna código de erro para integração com pipelines
- Debug mode para troubleshooting

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

PRINT 'Procedure gold.prc_validate_processing criada com sucesso!';
GO