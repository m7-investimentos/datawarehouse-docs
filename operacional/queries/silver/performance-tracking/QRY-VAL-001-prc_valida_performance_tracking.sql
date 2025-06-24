SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ==============================================================================
-- SP: sp_ValidateSilverLayer_USEFUL
-- ==============================================================================
-- VERSÃO ÚTIL: Mostra EXATAMENTE quais são os problemas e como resolver
-- ==============================================================================

CREATE   PROCEDURE [silver].[prc_valida_performance_tracking]
(
    @trimestre NVARCHAR(10) = NULL,           -- Ex: '2025-Q1'
    @periodo_competencia NVARCHAR(7) = NULL,  -- Ex: '2025-01'
    @crm_id VARCHAR(20) = NULL,               -- CRM ID específico
    @show_details BIT = 1                     -- Sempre mostra detalhes por padrão
)
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Configurações automáticas
    IF @trimestre IS NULL
        SET @trimestre = CONCAT(YEAR(GETDATE()), '-Q', CEILING(MONTH(GETDATE())/3.0));
    
    IF @periodo_competencia IS NULL
        SET @periodo_competencia = FORMAT(GETDATE(), 'yyyy-MM');
    
    PRINT '==================================================================';
    PRINT 'VALIDAÇÃO SILVER LAYER - PERFORMANCE TRACKING';
    PRINT 'Data/Hora: ' + CONVERT(VARCHAR(20), GETDATE(), 120);
    PRINT 'Trimestre: ' + @trimestre + ' | Período: ' + @periodo_competencia;
    PRINT '==================================================================';
    PRINT '';
    
    -- ==============================================================================
    -- 1. PROBLEMA: SOMA DE PESOS CARD ≠ 100%
    -- ==============================================================================
    
    PRINT '🔍 1. VERIFICANDO SOMA DE PESOS DOS INDICADORES CARD...';
    PRINT '------------------------------------------------------';
    
    WITH peso_problems AS (
        SELECT 
            a.crm_id,
            p.nome_pessoa,
            SUM(CASE WHEN i.tipo = 'CARD' THEN a.peso ELSE 0 END) as soma_pesos_card,
            COUNT(CASE WHEN i.tipo = 'CARD' THEN 1 END) as qtd_cards,
            100 - SUM(CASE WHEN i.tipo = 'CARD' THEN a.peso ELSE 0 END) as diferenca,
            STRING_AGG(
                CASE WHEN i.tipo = 'CARD' 
                THEN i.indicator_id + ' (' + CAST(a.peso AS VARCHAR(10)) + '%)' 
                END, ', ') as detalhes_cards
        FROM silver.fact_performance_assignments a
        INNER JOIN silver.dim_pessoas p ON a.crm_id = p.crm_id
        INNER JOIN silver.dim_indicators i ON a.indicator_sk = i.indicator_sk
        WHERE a.is_current = 1
          AND a.trimestre = @trimestre
          AND (@crm_id IS NULL OR a.crm_id = @crm_id)
        GROUP BY a.crm_id, p.nome_pessoa
        HAVING SUM(CASE WHEN i.tipo = 'CARD' THEN a.peso ELSE 0 END) <> 100
           AND COUNT(CASE WHEN i.tipo = 'CARD' THEN 1 END) > 0
    )
    SELECT 
        'PROBLEMA' as status,
        crm_id,
        nome_pessoa,
        soma_pesos_card as peso_atual,
        diferenca as falta_para_100,
        qtd_cards,
        detalhes_cards,
        CASE 
            WHEN soma_pesos_card = 0 THEN '❌ CRÍTICO: Todos os pesos zerados!'
            WHEN ABS(diferenca) <= 5 THEN '⚠️ PEQUENO: Ajustar ' + CAST(ABS(diferenca) AS VARCHAR(10)) + '%'
            ELSE '🚨 GRAVE: Faltam ' + CAST(ABS(diferenca) AS VARCHAR(10)) + '%'
        END as acao_necessaria
    FROM peso_problems
    ORDER BY ABS(diferenca) DESC;
    
    DECLARE @peso_errors INT = @@ROWCOUNT;
    
    IF @peso_errors = 0
    BEGIN
        PRINT '✅ OK: Todas as pessoas têm soma de pesos CARD = 100%';
    END
    ELSE
    BEGIN
        PRINT '❌ PROBLEMAS ENCONTRADOS: ' + CAST(@peso_errors AS VARCHAR(10)) + ' pessoas com soma de pesos incorreta';
        PRINT '';
        PRINT '💡 COMO RESOLVER:';
        PRINT '   1. Se todos os pesos estão zerados: Execute procedure de configuração inicial';
        PRINT '   2. Se faltam poucos %: Ajuste manualmente os pesos dos CARDs';
        PRINT '   3. Se há muita diferença: Revisar regras de negócio';
    END
    
    PRINT '';
    
    -- ==============================================================================
    -- 2. PROBLEMA: GATILHO/KPI COM PESO > 0
    -- ==============================================================================
    
    PRINT '🔍 2. VERIFICANDO GATILHO/KPI COM PESO INCORRETO...';
    PRINT '-------------------------------------------------';
    
    SELECT 
        'PROBLEMA' as status,
        a.crm_id,
        p.nome_pessoa,
        i.indicator_id,
        i.tipo as tipo_indicador,
        a.peso as peso_atual,
        '0.00' as peso_correto,
        '❌ ' + i.tipo + ' deve ter peso = 0' as acao_necessaria
    FROM silver.fact_performance_assignments a
    INNER JOIN silver.dim_pessoas p ON a.crm_id = p.crm_id
    INNER JOIN silver.dim_indicators i ON a.indicator_sk = i.indicator_sk
    WHERE i.tipo IN ('GATILHO', 'KPI')
      AND a.peso > 0
      AND a.is_current = 1
      AND a.trimestre = @trimestre
      AND (@crm_id IS NULL OR a.crm_id = @crm_id)
    ORDER BY a.peso DESC;
    
    DECLARE @gatilho_errors INT = @@ROWCOUNT;
    
    IF @gatilho_errors = 0
    BEGIN
        PRINT '✅ OK: Todos os GATILHO/KPI têm peso = 0';
    END
    ELSE
    BEGIN
        PRINT '❌ PROBLEMAS ENCONTRADOS: ' + CAST(@gatilho_errors AS VARCHAR(10)) + ' indicadores GATILHO/KPI com peso > 0';
        PRINT '';
        PRINT '💡 COMO RESOLVER:';
        PRINT '   UPDATE silver.fact_performance_assignments SET peso = 0';
        PRINT '   WHERE indicator_sk IN (SELECT indicator_sk FROM silver.dim_indicators WHERE tipo IN (''GATILHO'', ''KPI''))';
        PRINT '   AND is_current = 1;';
    END
    
    PRINT '';
    
    -- ==============================================================================
    -- 3. PROBLEMA: DUPLICAÇÕES
    -- ==============================================================================
    
    PRINT '🔍 3. VERIFICANDO DUPLICAÇÕES...';
    PRINT '-------------------------------';
    
    WITH duplicates AS (
        SELECT 
            a.crm_id,
            p.nome_pessoa,
            i.indicator_id,
            a.trimestre,
            COUNT(*) as qtd_duplicadas,
            STRING_AGG(CAST(a.assignment_sk AS VARCHAR(10)), ', ') as assignment_sks
        FROM silver.fact_performance_assignments a
        INNER JOIN silver.dim_pessoas p ON a.crm_id = p.crm_id
        INNER JOIN silver.dim_indicators i ON a.indicator_sk = i.indicator_sk
        WHERE a.is_current = 1
          AND a.trimestre = @trimestre
          AND (@crm_id IS NULL OR a.crm_id = @crm_id)
        GROUP BY a.crm_id, p.nome_pessoa, i.indicator_id, a.trimestre
        HAVING COUNT(*) > 1
    )
    SELECT 
        'PROBLEMA' as status,
        crm_id,
        nome_pessoa,
        indicator_id,
        trimestre,
        qtd_duplicadas,
        assignment_sks,
        '🔧 Manter apenas 1, deletar: ' + assignment_sks as acao_necessaria
    FROM duplicates
    ORDER BY qtd_duplicadas DESC;
    
    DECLARE @dup_errors INT = @@ROWCOUNT;
    
    IF @dup_errors = 0
    BEGIN
        PRINT '✅ OK: Não há duplicações de pessoa+indicador+trimestre';
    END
    ELSE
    BEGIN
        PRINT '❌ PROBLEMAS ENCONTRADOS: ' + CAST(@dup_errors AS VARCHAR(10)) + ' duplicações encontradas';
        PRINT '';
        PRINT '💡 COMO RESOLVER:';
        PRINT '   1. Escolher qual registro manter (geralmente o mais recente)';
        PRINT '   2. DELETE FROM silver.fact_performance_assignments WHERE assignment_sk IN (lista)';
        PRINT '   3. Ou usar ROW_NUMBER() para manter apenas o primeiro';
    END
    
    PRINT '';
    
    -- ==============================================================================
    -- 4. PROBLEMA: METAS SEM ASSIGNMENT
    -- ==============================================================================
    
    PRINT '🔍 4. VERIFICANDO METAS SEM ASSIGNMENT CORRESPONDENTE...';
    PRINT '-------------------------------------------------------';
    
    SELECT 
        'PROBLEMA' as status,
        t.crm_id,
        p.nome_pessoa,
        i.indicator_id,
        t.periodo_competencia,
        t.valor_meta,
        CONCAT(t.ano, '-Q', CEILING(t.mes/3.0)) as trimestre_esperado,
        '❌ Criar assignment para este trimestre' as acao_necessaria
    FROM silver.fact_performance_targets t
    INNER JOIN silver.dim_pessoas p ON t.crm_id = p.crm_id
    INNER JOIN silver.dim_indicators i ON t.indicator_sk = i.indicator_sk
    WHERE t.periodo_competencia = @periodo_competencia
      AND (@crm_id IS NULL OR t.crm_id = @crm_id)
      AND NOT EXISTS (
        SELECT 1
        FROM silver.fact_performance_assignments a
        WHERE a.crm_id = t.crm_id
          AND a.indicator_sk = t.indicator_sk
          AND a.trimestre = CONCAT(t.ano, '-Q', CEILING(t.mes/3.0))
          AND a.is_current = 1
      )
    ORDER BY t.crm_id, i.indicator_id;
    
    DECLARE @meta_errors INT = @@ROWCOUNT;
    
    IF @meta_errors = 0
    BEGIN
        PRINT '✅ OK: Todas as metas têm assignment correspondente';
    END
    ELSE
    BEGIN
        PRINT '❌ PROBLEMAS ENCONTRADOS: ' + CAST(@meta_errors AS VARCHAR(10)) + ' metas órfãs (sem assignment)';
        PRINT '';
        PRINT '💡 COMO RESOLVER:';
        PRINT '   1. Criar assignments para estes indicadores no trimestre correspondente';
        PRINT '   2. Ou remover as metas se não deveriam existir';
    END
    
    PRINT '';
    
    -- ==============================================================================
    -- 5. PROBLEMA: VALORES DE META INCOERENTES
    -- ==============================================================================
    
    PRINT '🔍 5. VERIFICANDO VALORES DE META INCOERENTES...';
    PRINT '----------------------------------------------';
    
    SELECT 
        'PROBLEMA' as status,
        t.crm_id,
        p.nome_pessoa,
        i.indicator_id,
        t.periodo_competencia,
        t.valor_minimo,
        t.valor_meta,
        t.valor_superacao,
        CASE 
            WHEN t.valor_minimo > t.valor_meta THEN '❌ Mínimo > Meta'
            WHEN t.valor_superacao < t.valor_meta THEN '❌ Superação < Meta'
            ELSE '❌ Verificar lógica'
        END as problema,
        'Corrigir: Mínimo ≤ Meta ≤ Superação' as acao_necessaria
    FROM silver.fact_performance_targets t
    INNER JOIN silver.dim_pessoas p ON t.crm_id = p.crm_id
    INNER JOIN silver.dim_indicators i ON t.indicator_sk = i.indicator_sk
    WHERE t.periodo_competencia = @periodo_competencia
      AND (@crm_id IS NULL OR t.crm_id = @crm_id)
      AND ((t.valor_minimo IS NOT NULL AND t.valor_minimo > t.valor_meta)
       OR (t.valor_superacao IS NOT NULL AND t.valor_superacao < t.valor_meta))
    ORDER BY t.crm_id, i.indicator_id;
    
    DECLARE @valor_errors INT = @@ROWCOUNT;
    
    IF @valor_errors = 0
    BEGIN
        PRINT '✅ OK: Todos os valores de meta estão coerentes';
    END
    ELSE
    BEGIN
        PRINT '❌ PROBLEMAS ENCONTRADOS: ' + CAST(@valor_errors AS VARCHAR(10)) + ' metas com valores incoerentes';
    END
    
    PRINT '';
    
    -- ==============================================================================
    -- 6. RESUMO EXECUTIVO
    -- ==============================================================================
    
    DECLARE @total_errors INT = @peso_errors + @gatilho_errors + @dup_errors + @meta_errors + @valor_errors;
    
    PRINT '==================================================================';
    PRINT 'RESUMO EXECUTIVO';
    PRINT '==================================================================';
    PRINT 'Total de problemas encontrados: ' + CAST(@total_errors AS VARCHAR(10));
    PRINT '';
    PRINT 'Breakdown por categoria:';
    PRINT '• Soma de pesos CARD ≠ 100%: ' + CAST(@peso_errors AS VARCHAR(10)) + ' pessoas';
    PRINT '• GATILHO/KPI com peso > 0: ' + CAST(@gatilho_errors AS VARCHAR(10)) + ' registros';
    PRINT '• Duplicações: ' + CAST(@dup_errors AS VARCHAR(10)) + ' casos';
    PRINT '• Metas sem assignment: ' + CAST(@meta_errors AS VARCHAR(10)) + ' registros';
    PRINT '• Valores incoerentes: ' + CAST(@valor_errors AS VARCHAR(10)) + ' metas';
    PRINT '';
    
    IF @total_errors = 0
    BEGIN
        PRINT '🎉 STATUS: TODAS AS VALIDAÇÕES PASSARAM!';
        PRINT '✅ Silver layer está íntegra e pronta para uso.';
    END
    ELSE
    BEGIN
        PRINT '🚨 STATUS: PROBLEMAS ENCONTRADOS!';
        PRINT '';
        PRINT '🔧 PRÓXIMOS PASSOS:';
        IF @peso_errors > 0
            PRINT '   1. URGENTE: Corrigir pesos dos indicadores CARD (soma deve ser 100%)';
        IF @gatilho_errors > 0
            PRINT '   2. Zerar pesos dos GATILHO/KPI (devem ser apenas controles)';
        IF @dup_errors > 0
            PRINT '   3. Remover registros duplicados de assignments';
        IF @meta_errors > 0
            PRINT '   4. Criar assignments para metas órfãs ou remover metas desnecessárias';
        IF @valor_errors > 0
            PRINT '   5. Corrigir valores de metas incoerentes';
        
        PRINT '';
        PRINT '💡 DICA: Execute esta procedure com @crm_id específico para focar em uma pessoa.';
    END
    
    PRINT '';
    PRINT 'Validação concluída em: ' + CONVERT(VARCHAR(20), GETDATE(), 120);
    
END;

-- ==============================================================================
-- EXEMPLOS DE USO
-- ==============================================================================

/*
-- Execução padrão (mostra TODOS os problemas)
EXEC silver.sp_ValidateSilverLayer_USEFUL;

-- Para uma pessoa específica
EXEC silver.sp_ValidateSilverLayer_USEFUL @crm_id = '134';

-- Para um período específico
EXEC silver.sp_ValidateSilverLayer_USEFUL 
    @trimestre = '2025-Q1',
    @periodo_competencia = '2025-01';
*/
GO
