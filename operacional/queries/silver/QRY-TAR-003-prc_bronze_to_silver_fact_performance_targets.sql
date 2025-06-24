-- ==============================================================================
-- QRY-TAR-004-prc_bronze_to_silver_performance_targets
-- ==============================================================================
-- Tipo: Stored Procedure (ETL)
-- Versão: 1.0.0
-- Última atualização: 2025-06-24
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [etl, silver, performance, targets, bronze-to-silver]
-- Status: produção
-- Banco de Dados: SQL Server 2019
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure ETL para popular a tabela silver.fact_performance_targets 
a partir dos dados validados em bronze.performance_targets.

Funcionalidades principais:
- Transformação de dados do bronze para silver
- Resolução de chaves estrangeiras (surrogate keys)
- Validação de lógica de metas (mínimo <= target <= stretch)
- Controle de processamento incremental
- Geração de período competência no formato YYYY-MM

Frequência de execução: Sob demanda (após carga do bronze)
Tempo médio de execução: 10-30 segundos
Volume esperado processado: 500-2000 registros por execução
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros da procedure:

@ExecutionId      VARCHAR(50)  -- ID único de execução (opcional, auto-gerado se NULL)
@ProcessOnlyNew   BIT          -- Processar apenas novos registros (default: 1)
@Debug            BIT          -- Ativar logs detalhados (default: 0)

Exemplo de uso:
-- Execução padrão (apenas registros novos)
EXEC silver.sp_populate_performance_targets;

-- Execução com debug ativo
EXEC silver.sp_populate_performance_targets @Debug = 1;

-- Reprocessar todos os registros
EXEC silver.sp_populate_performance_targets @ProcessOnlyNew = 0, @Debug = 1;

-- Execução com ID específico para rastreamento
EXEC silver.sp_populate_performance_targets @ExecutionId = 'MANUAL-20250624-001', @Debug = 1;
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Procedure não retorna resultado set, mas popula a tabela:
silver.fact_performance_targets

Registros processados em bronze.performance_targets são marcados com:
- is_processed = 1
- processing_date = timestamp da execução
- processing_status = 'SUCCESS' ou 'ERROR'
- processing_notes = detalhes do processamento

Logs de execução são exibidos via PRINT com:
- Quantidade de registros processados, inseridos e atualizados
- Duração total da execução
- ExecutionId para rastreamento
- Avisos sobre violações de regras de negócio
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas origem (leitura):
- bronze.performance_targets: Dados de entrada validados
- silver.dim_indicators: Dimensão de indicadores (para resolver indicator_sk)
- silver.dim_pessoas: Dimensão de pessoas (validação se pessoa existe)
- silver.dim_calendario: Dimensão temporal (para data_ref)
- silver.fact_estrutura_pessoas: Relacionamento pessoa-estrutura por período

Tabelas destino (escrita):
- silver.fact_performance_targets: Tabela fato principal

Pré-requisitos:
- Dados em bronze.performance_targets com is_processed = 0
- Dimensões atualizadas (dim_indicators, dim_pessoas, dim_calendario)
- Estruturas organizacionais atualizadas em fact_estrutura_pessoas
- Permissões: SELECT no bronze, INSERT/UPDATE no silver
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- Configurações da sessão para otimização
SET NOCOUNT ON;

-- ==============================================================================
-- 6. PROCEDURE PRINCIPAL
-- ==============================================================================
USE M7Medallion;
GO

CREATE OR ALTER PROCEDURE silver.sp_populate_performance_targets
    @ExecutionId VARCHAR(50) = NULL,
    @ProcessOnlyNew BIT = 1,
    @Debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Variáveis de controle
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @ProcessName VARCHAR(100) = 'sp_populate_performance_targets';
    DECLARE @RecordsProcessed INT = 0;
    DECLARE @RecordsInserted INT = 0;
    DECLARE @RecordsUpdated INT = 0;
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    -- ExecutionId único para rastreamento
    IF @ExecutionId IS NULL
        SET @ExecutionId = CONCAT('TAR-', FORMAT(GETDATE(), 'yyyyMMdd-HHmmss'));
    
    BEGIN TRY
        
        IF @Debug = 1
            PRINT CONCAT('[', FORMAT(GETDATE(), 'HH:mm:ss'), '] Iniciando ', @ProcessName, ' - ExecutionId: ', @ExecutionId);
        
        -- ==============================================================================
        -- VALIDAÇÕES INICIAIS
        -- ==============================================================================
        
        -- Verificar se existem dados no bronze para processar
        IF NOT EXISTS (SELECT 1 FROM bronze.performance_targets WHERE is_processed = 0 OR @ProcessOnlyNew = 0)
        BEGIN
            IF @Debug = 1
                PRINT '[INFO] Nenhum registro novo para processar no bronze.performance_targets';
            RETURN;
        END
        
        -- ==============================================================================
        -- PREPARAÇÃO - VERSIONAR REGISTROS EXISTENTES (APPEND-ONLY)
        -- ==============================================================================
        
        -- Para targets, fazemos append-only: marcar versões antigas como não-atuais
        UPDATE silver.fact_performance_targets
        SET 
            modified_date = @StartTime
        FROM bronze.performance_targets bronze
        INNER JOIN silver.dim_indicators di ON bronze.indicator_code = di.indicator_id
        WHERE (bronze.is_processed = 0 OR @ProcessOnlyNew = 0)
          AND silver.fact_performance_targets.crm_id = bronze.codigo_assessor_crm
          AND silver.fact_performance_targets.indicator_sk = di.indicator_sk
          AND silver.fact_performance_targets.periodo_competencia = FORMAT(CAST(bronze.period_start AS DATE), 'yyyy-MM');
        
        SET @RecordsUpdated = @@ROWCOUNT;
        
        IF @Debug = 1
            PRINT CONCAT('[INFO] Registros versionados (mantendo histórico): ', CAST(@RecordsUpdated AS VARCHAR(10)));
        
        -- ==============================================================================
        -- TRANSFORMAÇÃO E INSERÇÃO DE NOVOS REGISTROS (APPEND-ONLY)
        -- ==============================================================================
        
        INSERT INTO silver.fact_performance_targets (
            indicator_sk,
            crm_id,
            data_ref,
            id_estrutura,
            valor_meta,
            valor_minimo,
            valor_superacao,
            mes,
            ano,
            periodo_competencia,
            is_approved,
            approved_by,
            approved_date,
            created_date,
            created_by
        )
        SELECT 
            -- Resolução de chaves estrangeiras
            di.indicator_sk,
            bronze.codigo_assessor_crm AS crm_id,
            cal.data_ref,
            COALESCE(ep.id_estrutura, 1) AS id_estrutura, -- Default estrutura se não encontrar
            
            -- Transformações de valores (string para decimal)
            CAST(COALESCE(NULLIF(bronze.target_value, ''), '0') AS DECIMAL(18,4)) AS valor_meta,
            CASE 
                WHEN bronze.minimum_value IS NULL OR bronze.minimum_value = '' 
                THEN NULL 
                ELSE CAST(bronze.minimum_value AS DECIMAL(18,4)) 
            END AS valor_minimo,
            CASE 
                WHEN bronze.stretch_value IS NULL OR bronze.stretch_value = '' 
                THEN NULL 
                ELSE CAST(bronze.stretch_value AS DECIMAL(18,4)) 
            END AS valor_superacao,
            
            -- Campos temporais
            MONTH(CAST(bronze.period_start AS DATE)) AS mes,
            YEAR(CAST(bronze.period_start AS DATE)) AS ano,
            FORMAT(CAST(bronze.period_start AS DATE), 'yyyy-MM') AS periodo_competencia,
            
            -- Campos de workflow (sempre aprovado por padrão no bronze)
            1 AS is_approved,
            bronze.load_source AS approved_by, -- Considerando fonte como aprovação
            bronze.load_timestamp AS approved_date,
            
            -- Campos de auditoria
            @StartTime AS created_date,
            CONCAT(@ProcessName, '-', @ExecutionId) AS created_by
            
        FROM bronze.performance_targets bronze
        
        -- Joins para resolução de surrogate keys
        INNER JOIN silver.dim_indicators di 
            ON bronze.indicator_code = di.indicator_id
            AND di.is_active = 1
        
        -- Join com calendário para obter data_ref (primeiro dia do mês)
        INNER JOIN silver.dim_calendario cal 
            ON cal.data_ref = CAST(bronze.period_start AS DATE)
        
        -- Validação se pessoa existe
        INNER JOIN silver.dim_pessoas dp 
            ON bronze.codigo_assessor_crm = dp.crm_id
        
        -- Buscar estrutura organizacional vigente na data
        LEFT JOIN silver.fact_estrutura_pessoas ep 
            ON bronze.codigo_assessor_crm = ep.crm_id
            AND CAST(bronze.period_start AS DATE) >= ep.data_entrada
            AND (ep.data_saida IS NULL OR CAST(bronze.period_start AS DATE) <= ep.data_saida)
        
        WHERE 
            -- Filtros de qualidade de dados
            bronze.target_logic_valid = 1  -- Só metas com lógica válida
            AND bronze.validation_errors IS NULL  -- Sem erros de validação
            AND bronze.period_type = 'MENSAL'  -- Só períodos mensais
            AND (bronze.is_processed = 0 OR @ProcessOnlyNew = 0)  -- Controle de processamento
            
        -- Evitar duplicatas apenas se não existe versão mais recente
        AND NOT EXISTS (
            SELECT 1 
            FROM silver.fact_performance_targets existing
            WHERE existing.crm_id = bronze.codigo_assessor_crm
              AND existing.indicator_sk = di.indicator_sk
              AND existing.periodo_competencia = FORMAT(CAST(bronze.period_start AS DATE), 'yyyy-MM')
              AND existing.created_date > DATEADD(MINUTE, -5, @StartTime) -- Evita duplicata na mesma execução
        );
        
        SET @RecordsInserted = @@ROWCOUNT;
        
        IF @Debug = 1
            PRINT CONCAT('[INFO] Registros inseridos: ', CAST(@RecordsInserted AS VARCHAR(10)));
        
        -- ==============================================================================
        -- MARCAÇÃO DE PROCESSAMENTO
        -- ==============================================================================
        
        UPDATE bronze.performance_targets 
        SET 
            is_processed = 1,
            processing_date = @StartTime,
            processing_status = 'SUCCESS',
            processing_notes = CONCAT('Processado por ', @ProcessName, ' - ExecutionId: ', @ExecutionId)
        WHERE (is_processed = 0 OR @ProcessOnlyNew = 0);
        
        SET @RecordsProcessed = @@ROWCOUNT;
        
        -- ==============================================================================
        -- VALIDAÇÕES PÓS-PROCESSAMENTO
        -- ==============================================================================
        
        -- Validar regra de negócio: Metas com valores coerentes
        DECLARE @InvalidTargets TABLE (
            crm_id VARCHAR(20),
            indicator_code VARCHAR(50),
            periodo_competencia VARCHAR(7),
            valor_minimo DECIMAL(18,4),
            valor_meta DECIMAL(18,4),
            valor_superacao DECIMAL(18,4)
        );
        
        INSERT INTO @InvalidTargets
        SELECT 
            t.crm_id,
            di.indicator_id,
            t.periodo_competencia,
            t.valor_minimo,
            t.valor_meta,
            t.valor_superacao
        FROM silver.fact_performance_targets t
        INNER JOIN silver.dim_indicators di ON t.indicator_sk = di.indicator_sk
        WHERE (t.valor_minimo IS NOT NULL AND t.valor_minimo > t.valor_meta)
           OR (t.valor_superacao IS NOT NULL AND t.valor_superacao < t.valor_meta);
        
        IF EXISTS (SELECT 1 FROM @InvalidTargets)
        BEGIN
            SELECT 
                'AVISO: Metas com valores incoerentes' AS Tipo,
                crm_id,
                indicator_code,
                periodo_competencia,
                valor_minimo,
                valor_meta,
                valor_superacao
            FROM @InvalidTargets;
        END
        
        -- Validar cobertura: Pessoas com assignments mas sem metas
        DECLARE @MissingTargets TABLE (
            crm_id VARCHAR(20),
            indicator_code VARCHAR(50),
            trimestre VARCHAR(10)
        );
        
        INSERT INTO @MissingTargets
        SELECT DISTINCT
            fa.crm_id,
            di.indicator_id,
            fa.trimestre
        FROM silver.fact_performance_assignments fa
        INNER JOIN silver.dim_indicators di ON fa.indicator_sk = di.indicator_sk
        WHERE fa.is_current = 1
          AND di.tipo = 'CARD' -- Só CARDs precisam de meta
          AND NOT EXISTS (
              SELECT 1 
              FROM silver.fact_performance_targets ft
              WHERE ft.crm_id = fa.crm_id
                AND ft.indicator_sk = fa.indicator_sk
                AND LEFT(ft.periodo_competencia, 4) + '-Q' + CAST(CEILING(CAST(RIGHT(ft.periodo_competencia, 2) AS INT)/3.0) AS VARCHAR(1)) = fa.trimestre
          );
        
        IF EXISTS (SELECT 1 FROM @MissingTargets)
        BEGIN
            SELECT 
                'AVISO: Assignments sem metas correspondentes' AS Tipo,
                crm_id,
                indicator_code,
                trimestre
            FROM @MissingTargets;
        END
        
        -- ==============================================================================
        -- LOG DE EXECUÇÃO
        -- ==============================================================================
        
        DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, GETDATE());
        
        PRINT '=== RESUMO EXECUÇÃO ===';
        PRINT CONCAT('ExecutionId: ', @ExecutionId);
        PRINT CONCAT('Duração: ', CAST(@Duration AS VARCHAR(10)), ' segundos');
        PRINT CONCAT('Registros processados (bronze): ', CAST(@RecordsProcessed AS VARCHAR(10)));
        PRINT CONCAT('Registros versionados (silver): ', CAST(@RecordsUpdated AS VARCHAR(10)));
        PRINT CONCAT('Registros inseridos (silver): ', CAST(@RecordsInserted AS VARCHAR(10)));
        PRINT CONCAT('Finalizado em: ', FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss'));
        
    END TRY
    BEGIN CATCH
        
        -- Em caso de erro, reverter marcação de processado
        UPDATE bronze.performance_targets 
        SET 
            processing_status = 'ERROR',
            processing_notes = CONCAT('ERRO: ', ERROR_MESSAGE())
        WHERE processing_status IS NULL 
           OR processing_status = 'SUCCESS';
        
        SET @ErrorMessage = CONCAT(
            'Erro na execução da procedure ', @ProcessName, 
            ' - ExecutionId: ', @ExecutionId,
            ' - Erro: ', ERROR_MESSAGE(),
            ' - Linha: ', CAST(ERROR_LINE() AS VARCHAR(10))
        );
        
        PRINT @ErrorMessage;
        THROW;
        
    END CATCH
    
END;
GO

-- ==============================================================================
-- 7. PROCEDURES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================

/*
-- Procedure para validar metas de um período
CREATE OR ALTER PROCEDURE silver.sp_validate_targets_period
    @periodo_competencia VARCHAR(7) = NULL
AS
BEGIN
    SELECT 
        p.nome_pessoa,
        i.nome as indicador,
        t.periodo_competencia,
        t.valor_minimo,
        t.valor_meta,
        t.valor_superacao,
        t.is_approved,
        CASE 
            WHEN t.valor_minimo > t.valor_meta THEN 'Mínimo > Meta'
            WHEN t.valor_superacao < t.valor_meta THEN 'Stretch < Meta'
            WHEN t.valor_meta <= 0 AND i.is_inverted = 0 THEN 'Meta zero/negativa'
            ELSE 'OK'
        END as status_validacao
    FROM silver.fact_performance_targets t
    INNER JOIN silver.dim_pessoas p ON t.crm_id = p.crm_id
    INNER JOIN silver.dim_indicators i ON t.indicator_sk = i.indicator_sk
    WHERE (@periodo_competencia IS NULL OR t.periodo_competencia = @periodo_competencia)
    ORDER BY t.periodo_competencia DESC, p.nome_pessoa, i.nome;
END;

-- Procedure para consultar metas de uma pessoa
CREATE OR ALTER PROCEDURE silver.sp_get_person_targets
    @crm_id VARCHAR(20),
    @periodo_competencia VARCHAR(7) = NULL
AS
BEGIN
    SELECT 
        p.nome_pessoa,
        i.nome as indicador,
        i.unidade_medida,
        t.periodo_competencia,
        t.valor_minimo,
        t.valor_meta,
        t.valor_superacao,
        t.is_approved,
        t.approved_by,
        t.created_date
    FROM silver.fact_performance_targets t
    INNER JOIN silver.dim_pessoas p ON t.crm_id = p.crm_id
    INNER JOIN silver.dim_indicators i ON t.indicator_sk = i.indicator_sk
    WHERE t.crm_id = @crm_id
      AND (@periodo_competencia IS NULL OR t.periodo_competencia = @periodo_competencia)
    ORDER BY t.periodo_competencia DESC, i.nome;
END;
*/

-- ==============================================================================
-- 8. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                          | Descrição
--------|------------|--------------------------------|------------------------------------------
1.0.0   | 2025-06-24 | bruno.chiaramonti@multisete.com| Criação inicial da procedure ETL

*/

-- ==============================================================================
-- 9. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Procedure implementa estratégia UPSERT (DELETE + INSERT) para metas
- Validação de lógica de metas (mínimo <= meta <= stretch) pós-processamento
- Utiliza ExecutionId único para rastreabilidade completa
- Controle de processamento incremental via flag is_processed

Troubleshooting comum:
1. Erro "CHK_periodo_formato": Verificar se formato está correto (YYYY-MM)
2. FK constraint violation: Verificar se dimensões estão atualizadas
3. Valores incoerentes: Validar dados no bronze antes do processamento
4. Missing targets: Verificar se todas assignments têm metas correspondentes

Monitoramento:
- Logs detalhados disponíveis com @Debug = 1
- Status de processamento gravado em bronze.performance_targets
- Validações pós-processamento alertam sobre inconsistências

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/

-- ==============================================================================
-- GRANTS DE SEGURANÇA
-- ==============================================================================

-- GRANT EXECUTE ON silver.sp_populate_performance_targets TO role_etl_service;
-- GRANT EXECUTE ON silver.sp_populate_performance_targets TO role_performance_manager;