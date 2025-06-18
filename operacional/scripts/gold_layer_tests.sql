-- ==============================================================================
-- GOLD LAYER TEST SUITE
-- ==============================================================================
-- Descrição: Script de testes para validar a implementação da camada Gold
-- Autor: bruno.chiaramonti@multisete.com
-- Data: 2025-01-18
-- Versão: 1.0.0
-- ==============================================================================

USE M7Medallion;
GO

PRINT '==============================================================================';
PRINT 'INICIANDO TESTES DA CAMADA GOLD - ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '==============================================================================';
GO

-- ==============================================================================
-- TESTE 1: VERIFICAR CRIAÇÃO DOS OBJETOS
-- ==============================================================================
PRINT '';
PRINT 'TESTE 1: Verificando existência dos objetos Gold...';
PRINT '----------------------------------------------------------------------';

-- Verificar tabelas
IF OBJECT_ID('gold.card_metas', 'U') IS NOT NULL
    PRINT '✓ Tabela gold.card_metas existe'
ELSE
    PRINT '✗ ERRO: Tabela gold.card_metas NÃO existe'

IF OBJECT_ID('gold.processing_log', 'U') IS NOT NULL
    PRINT '✓ Tabela gold.processing_log existe'
ELSE
    PRINT '✗ ERRO: Tabela gold.processing_log NÃO existe'

-- Verificar procedures
IF OBJECT_ID('gold.prc_process_performance_to_gold', 'P') IS NOT NULL
    PRINT '✓ Procedure gold.prc_process_performance_to_gold existe'
ELSE
    PRINT '✗ ERRO: Procedure gold.prc_process_performance_to_gold NÃO existe'

IF OBJECT_ID('gold.prc_validate_processing', 'P') IS NOT NULL
    PRINT '✓ Procedure gold.prc_validate_processing existe'
ELSE
    PRINT '✗ ERRO: Procedure gold.prc_validate_processing NÃO existe'

-- Verificar views
DECLARE @view_name VARCHAR(100);
DECLARE view_cursor CURSOR FOR
    SELECT name FROM sys.views 
    WHERE schema_id = SCHEMA_ID('gold')
    ORDER BY name;

OPEN view_cursor;
FETCH NEXT FROM view_cursor INTO @view_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '✓ View gold.' + @view_name + ' existe';
    FETCH NEXT FROM view_cursor INTO @view_name;
END

CLOSE view_cursor;
DEALLOCATE view_cursor;
GO

-- ==============================================================================
-- TESTE 2: VERIFICAR ESTRUTURA DAS TABELAS
-- ==============================================================================
PRINT '';
PRINT 'TESTE 2: Verificando estrutura da tabela card_metas...';
PRINT '----------------------------------------------------------------------';

-- Verificar colunas críticas
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('gold.card_metas') AND name = 'meta_id')
    PRINT '✓ Coluna meta_id (PK) existe'
    
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('gold.card_metas') AND name = 'entity_id')
    PRINT '✓ Coluna entity_id existe'
    
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('gold.card_metas') AND name = 'attribute_code')
    PRINT '✓ Coluna attribute_code existe'
    
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('gold.card_metas') AND name = 'realized_value')
    PRINT '✓ Coluna realized_value existe'
    
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('gold.card_metas') AND name = 'achievement_percentage')
    PRINT '✓ Coluna achievement_percentage existe'

-- Verificar constraints
SELECT 
    'Constraints encontradas:' as info,
    COUNT(*) as total_constraints,
    SUM(CASE WHEN type = 'PK' THEN 1 ELSE 0 END) as primary_keys,
    SUM(CASE WHEN type = 'UQ' THEN 1 ELSE 0 END) as unique_constraints,
    SUM(CASE WHEN type = 'C' THEN 1 ELSE 0 END) as check_constraints
FROM sys.objects
WHERE parent_object_id = OBJECT_ID('gold.card_metas')
  AND type IN ('PK', 'UQ', 'C');

-- Verificar índices
SELECT 
    'Índices encontrados:' as info,
    COUNT(*) as total_indices
FROM sys.indexes
WHERE object_id = OBJECT_ID('gold.card_metas')
  AND type > 0;
GO

-- ==============================================================================
-- TESTE 3: DADOS DE TESTE SILVER (PREPARAÇÃO)
-- ==============================================================================
PRINT '';
PRINT 'TESTE 3: Preparando dados de teste no Silver...';
PRINT '----------------------------------------------------------------------';

-- Verificar se existem dados de teste
DECLARE @test_indicator_count INT, @test_assignment_count INT, @test_target_count INT;

SELECT @test_indicator_count = COUNT(*) 
FROM silver.performance_indicators 
WHERE indicator_code IN ('TEST_IND_1', 'TEST_IND_2', 'TEST_IND_3');

SELECT @test_assignment_count = COUNT(*) 
FROM silver.performance_assignments 
WHERE codigo_assessor_crm = 'TEST001';

SELECT @test_target_count = COUNT(*) 
FROM silver.performance_targets 
WHERE codigo_assessor_crm = 'TEST001' 
  AND period_start = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()) - 1, 1);

IF @test_indicator_count = 0 OR @test_assignment_count = 0 OR @test_target_count = 0
BEGIN
    PRINT 'Inserindo dados de teste no Silver...';
    
    -- Inserir indicadores de teste
    IF @test_indicator_count = 0
    BEGIN
        INSERT INTO silver.performance_indicators (
            indicator_code, indicator_name, category, unit, 
            aggregation_method, calculation_formula, is_inverted, is_active
        )
        VALUES 
        ('TEST_IND_1', 'Teste Indicador 1', 'FINANCEIRO', 'R$', 'SUM', 
         'SELECT 1000000', 0, 1),
        ('TEST_IND_2', 'Teste Indicador 2', 'VOLUME', 'QTD', 'COUNT', 
         'SELECT 50', 0, 1),
        ('TEST_IND_3', 'Teste Indicador 3', 'QUALIDADE', '%', 'AVG', 
         'SELECT 85.5', 1, 1); -- Invertido
         
        PRINT '✓ 3 indicadores de teste criados';
    END
    
    -- Inserir assignments de teste
    IF @test_assignment_count = 0
    BEGIN
        INSERT INTO silver.performance_assignments (
            codigo_assessor_crm, indicator_id, indicator_weight, 
            valid_from, is_active
        )
        SELECT 
            'TEST001',
            indicator_id,
            CASE 
                WHEN indicator_code = 'TEST_IND_1' THEN 50.00
                WHEN indicator_code = 'TEST_IND_2' THEN 30.00
                WHEN indicator_code = 'TEST_IND_3' THEN 20.00
            END,
            DATEFROMPARTS(YEAR(GETDATE()), 1, 1),
            1
        FROM silver.performance_indicators
        WHERE indicator_code IN ('TEST_IND_1', 'TEST_IND_2', 'TEST_IND_3');
        
        PRINT '✓ Assignments de teste criados (soma pesos = 100%)';
    END
    
    -- Inserir targets de teste
    IF @test_target_count = 0
    BEGIN
        DECLARE @test_period_start DATE = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()) - 1, 1);
        DECLARE @test_period_end DATE = EOMONTH(@test_period_start);
        
        INSERT INTO silver.performance_targets (
            codigo_assessor_crm, indicator_id, period_start, period_end,
            target_value, stretch_target, minimum_target, is_active
        )
        SELECT 
            'TEST001',
            indicator_id,
            @test_period_start,
            @test_period_end,
            CASE 
                WHEN indicator_code = 'TEST_IND_1' THEN 900000
                WHEN indicator_code = 'TEST_IND_2' THEN 40
                WHEN indicator_code = 'TEST_IND_3' THEN 90
            END,
            CASE 
                WHEN indicator_code = 'TEST_IND_1' THEN 1100000
                WHEN indicator_code = 'TEST_IND_2' THEN 60
                WHEN indicator_code = 'TEST_IND_3' THEN 80 -- Invertido: menor é melhor
            END,
            CASE 
                WHEN indicator_code = 'TEST_IND_1' THEN 700000
                WHEN indicator_code = 'TEST_IND_2' THEN 30
                WHEN indicator_code = 'TEST_IND_3' THEN 95 -- Invertido: maior é pior
            END,
            1
        FROM silver.performance_indicators
        WHERE indicator_code IN ('TEST_IND_1', 'TEST_IND_2', 'TEST_IND_3');
        
        PRINT '✓ Targets de teste criados para período: ' + CONVERT(VARCHAR, @test_period_start, 103);
    END
END
ELSE
BEGIN
    PRINT '✓ Dados de teste já existem no Silver';
END
GO

-- ==============================================================================
-- TESTE 4: EXECUTAR PROCESSAMENTO GOLD
-- ==============================================================================
PRINT '';
PRINT 'TESTE 4: Executando processamento Gold para assessor de teste...';
PRINT '----------------------------------------------------------------------';

DECLARE @start_time DATETIME = GETDATE();
DECLARE @test_period DATE = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()) - 1, 1);

-- Executar processamento
EXEC gold.prc_process_performance_to_gold 
    @period_start = @test_period,
    @crm_id = 'TEST001',
    @debug = 1;

DECLARE @duration_ms INT = DATEDIFF(MILLISECOND, @start_time, GETDATE());
PRINT '';
PRINT '✓ Processamento concluído em ' + CAST(@duration_ms AS VARCHAR) + ' ms';

-- Verificar resultados
DECLARE @results_count INT;
SELECT @results_count = COUNT(*) 
FROM gold.card_metas 
WHERE entity_id = 'TEST001' 
  AND period_start = @test_period;

PRINT '✓ ' + CAST(@results_count AS VARCHAR) + ' registros criados em gold.card_metas';

-- Mostrar resultados
SELECT 
    attribute_code,
    attribute_name,
    indicator_type,
    indicator_weight,
    target_value,
    realized_value,
    achievement_percentage,
    weighted_achievement,
    achievement_status,
    is_inverted
FROM gold.card_metas
WHERE entity_id = 'TEST001' 
  AND period_start = @test_period
ORDER BY indicator_weight DESC;
GO

-- ==============================================================================
-- TESTE 5: VALIDAR CÁLCULOS
-- ==============================================================================
PRINT '';
PRINT 'TESTE 5: Validando cálculos de achievement...';
PRINT '----------------------------------------------------------------------';

-- Validar cálculo normal
DECLARE @normal_calc_ok BIT = 0;
SELECT @normal_calc_ok = 1
FROM gold.card_metas
WHERE entity_id = 'TEST001'
  AND attribute_code = 'TEST_IND_1'
  AND is_inverted = 0
  AND ABS(achievement_percentage - (realized_value / target_value * 100)) < 0.01;

IF @normal_calc_ok = 1
    PRINT '✓ Cálculo de achievement normal está correto'
ELSE
    PRINT '✗ ERRO: Cálculo de achievement normal incorreto'

-- Validar cálculo invertido
DECLARE @inverted_calc_ok BIT = 0;
SELECT @inverted_calc_ok = 1
FROM gold.card_metas
WHERE entity_id = 'TEST001'
  AND attribute_code = 'TEST_IND_3'
  AND is_inverted = 1
  AND ABS(achievement_percentage - ((2 - (realized_value / target_value)) * 100)) < 0.01;

IF @inverted_calc_ok = 1
    PRINT '✓ Cálculo de achievement invertido está correto'
ELSE
    PRINT '✗ ERRO: Cálculo de achievement invertido incorreto'

-- Validar weighted achievement
DECLARE @weighted_ok BIT = 0;
SELECT @weighted_ok = 1
FROM gold.card_metas
WHERE entity_id = 'TEST001'
  AND indicator_type = 'CARD'
  AND ABS(weighted_achievement - (achievement_percentage * indicator_weight / 100)) < 0.01;

IF @weighted_ok = 1
    PRINT '✓ Cálculo de weighted achievement está correto'
ELSE
    PRINT '✗ ERRO: Cálculo de weighted achievement incorreto'
GO

-- ==============================================================================
-- TESTE 6: EXECUTAR VALIDAÇÕES
-- ==============================================================================
PRINT '';
PRINT 'TESTE 6: Executando procedure de validação...';
PRINT '----------------------------------------------------------------------';

DECLARE @validation_result INT;
EXEC @validation_result = gold.prc_validate_processing 
    @validation_type = 'FULL',
    @fix_issues = 0,
    @debug = 1;

IF @validation_result = 0
    PRINT '✓ Validação passou sem erros críticos'
ELSE
    PRINT '✗ Validação encontrou erros'
GO

-- ==============================================================================
-- TESTE 7: TESTAR VIEWS
-- ==============================================================================
PRINT '';
PRINT 'TESTE 7: Testando views de consumo...';
PRINT '----------------------------------------------------------------------';

-- Testar cada view
DECLARE @view_test_sql NVARCHAR(MAX);
DECLARE @view_name VARCHAR(100);
DECLARE @row_count INT;

DECLARE view_test_cursor CURSOR FOR
    SELECT name 
    FROM sys.views 
    WHERE schema_id = SCHEMA_ID('gold')
    ORDER BY name;

OPEN view_test_cursor;
FETCH NEXT FROM view_test_cursor INTO @view_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @view_test_sql = N'SELECT @count = COUNT(*) FROM gold.' + @view_name + 
                         N' WHERE codigo_assessor_crm = ''TEST001''';
    
    EXEC sp_executesql @view_test_sql, N'@count INT OUTPUT', @count = @row_count OUTPUT;
    
    IF @row_count > 0
        PRINT '✓ View gold.' + @view_name + ' retornou ' + CAST(@row_count AS VARCHAR) + ' registros'
    ELSE
        PRINT '⚠ View gold.' + @view_name + ' não retornou dados (pode ser normal dependendo da view)'
    
    FETCH NEXT FROM view_test_cursor INTO @view_name;
END

CLOSE view_test_cursor;
DEALLOCATE view_test_cursor;
GO

-- ==============================================================================
-- TESTE 8: PERFORMANCE DAS VIEWS
-- ==============================================================================
PRINT '';
PRINT 'TESTE 8: Testando performance das views principais...';
PRINT '----------------------------------------------------------------------';

DECLARE @perf_start DATETIME, @perf_duration_ms INT;

-- Dashboard view
SET @perf_start = GETDATE();
SELECT COUNT(*) FROM gold.vw_card_metas_dashboard WHERE period_start >= DATEADD(MONTH, -3, GETDATE());
SET @perf_duration_ms = DATEDIFF(MILLISECOND, @perf_start, GETDATE());
PRINT 'Dashboard view: ' + CAST(@perf_duration_ms AS VARCHAR) + ' ms';

-- Weighted score view
SET @perf_start = GETDATE();
SELECT COUNT(*) FROM gold.vw_card_metas_weighted_score WHERE period_start >= DATEADD(MONTH, -3, GETDATE());
SET @perf_duration_ms = DATEDIFF(MILLISECOND, @perf_start, GETDATE());
PRINT 'Weighted score view: ' + CAST(@perf_duration_ms AS VARCHAR) + ' ms';

-- Ranking view
SET @perf_start = GETDATE();
SELECT COUNT(*) FROM gold.vw_card_metas_ranking WHERE period_start >= DATEADD(MONTH, -3, GETDATE());
SET @perf_duration_ms = DATEDIFF(MILLISECOND, @perf_start, GETDATE());
PRINT 'Ranking view: ' + CAST(@perf_duration_ms AS VARCHAR) + ' ms';

IF @perf_duration_ms < 2000
    PRINT '✓ Performance das views está dentro do esperado (< 2s)'
ELSE
    PRINT '⚠ Performance pode precisar de otimização (> 2s)'
GO

-- ==============================================================================
-- TESTE 9: SIMULAR ERRO E CORREÇÃO
-- ==============================================================================
PRINT '';
PRINT 'TESTE 9: Simulando erro de peso e auto-correção...';
PRINT '----------------------------------------------------------------------';

-- Quebrar propositalmente a soma de pesos
UPDATE gold.card_metas
SET indicator_weight = 45.00
WHERE entity_id = 'TEST001'
  AND attribute_code = 'TEST_IND_1'
  AND indicator_type = 'CARD';

PRINT 'Peso alterado para simular erro (soma != 100%)';

-- Executar validação com correção
DECLARE @fix_result INT;
EXEC @fix_result = gold.prc_validate_processing 
    @validation_type = 'WEIGHTS',
    @fix_issues = 1,
    @debug = 0;

-- Verificar se foi corrigido
DECLARE @sum_after_fix DECIMAL(5,2);
SELECT @sum_after_fix = SUM(indicator_weight)
FROM gold.card_metas
WHERE entity_id = 'TEST001'
  AND indicator_type = 'CARD';

IF ABS(@sum_after_fix - 100.00) < 0.01
    PRINT '✓ Auto-correção funcionou! Soma de pesos = ' + CAST(@sum_after_fix AS VARCHAR) + '%'
ELSE
    PRINT '✗ ERRO: Auto-correção falhou. Soma = ' + CAST(@sum_after_fix AS VARCHAR) + '%'
GO

-- ==============================================================================
-- TESTE 10: VERIFICAR LOG DE PROCESSAMENTO
-- ==============================================================================
PRINT '';
PRINT 'TESTE 10: Verificando log de processamento...';
PRINT '----------------------------------------------------------------------';

SELECT TOP 5
    log_id,
    processing_type,
    period_start,
    entity_id,
    duration_seconds,
    total_calculations,
    successful_calculations,
    failed_calculations,
    status,
    executed_by
FROM gold.processing_log
ORDER BY log_id DESC;

DECLARE @log_count INT;
SELECT @log_count = COUNT(*) FROM gold.processing_log;
PRINT '✓ Total de ' + CAST(@log_count AS VARCHAR) + ' registros no log de processamento';
GO

-- ==============================================================================
-- LIMPEZA (OPCIONAL)
-- ==============================================================================
PRINT '';
PRINT 'LIMPEZA: Removendo dados de teste...';
PRINT '----------------------------------------------------------------------';

-- Remover dados Gold de teste
DELETE FROM gold.card_metas WHERE entity_id = 'TEST001';
PRINT '✓ Dados de teste removidos de gold.card_metas';

-- Remover dados Silver de teste (comentado por segurança)
-- DELETE FROM silver.performance_targets WHERE codigo_assessor_crm = 'TEST001';
-- DELETE FROM silver.performance_assignments WHERE codigo_assessor_crm = 'TEST001';
-- DELETE FROM silver.performance_indicators WHERE indicator_code LIKE 'TEST_IND_%';

PRINT '';
PRINT '==============================================================================';
PRINT 'TESTES CONCLUÍDOS - ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '==============================================================================';
GO