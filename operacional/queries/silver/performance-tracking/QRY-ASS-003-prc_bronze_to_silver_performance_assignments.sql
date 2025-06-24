-- ==============================================================================
-- QRY-ASS-003-prc_bronze_to_silver_performance_assignments
-- ==============================================================================
-- Tipo: Stored Procedure (ETL)
-- Versão: 1.0.0
-- Última atualização: 2025-06-24
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [etl, silver, performance, assignments, bronze-to-silver]
-- Status: produção
-- Banco de Dados: SQL Server 2019
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure ETL para popular a tabela silver.fact_performance_assignments 
a partir dos dados validados em bronze.performance_assignments.

Funcionalidades principais:
- Transformação de dados do bronze para silver
- Resolução de chaves estrangeiras (surrogate keys)
- Controle de historização com SCD Tipo 2
- Validação de regras de negócio pós-processamento
- Controle de processamento incremental

Frequência de execução: Sob demanda (após carga do bronze)
Tempo médio de execução: 5-15 segundos
Volume esperado processado: 50-200 registros por execução
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
EXEC silver.sp_populate_performance_assignments;

-- Execução com debug ativo
EXEC silver.sp_populate_performance_assignments @Debug = 1;

-- Reprocessar todos os registros
EXEC silver.sp_populate_performance_assignments @ProcessOnlyNew = 0, @Debug = 1;

-- Execução com ID específico para rastreamento
EXEC silver.sp_populate_performance_assignments @ExecutionId = 'MANUAL-20250624-001', @Debug = 1;
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Procedure não retorna resultado set, mas popula a tabela:
silver.fact_performance_assignments

Registros processados em bronze.performance_assignments são marcados com:
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
- bronze.performance_assignments: Dados de entrada validados
- silver.dim_indicators: Dimensão de indicadores (para resolver indicator_sk)
- silver.dim_pessoas: Dimensão de pessoas (validação se pessoa existe)
- silver.dim_calendario: Dimensão temporal (para data_ref e trimestre)
- silver.fact_estrutura_pessoas: Relacionamento pessoa-estrutura por período

Tabelas destino (escrita):
- silver.fact_performance_assignments: Tabela fato principal

Pré-requisitos:
- Dados em bronze.performance_assignments com is_processed = 0
- Dimensões atualizadas (dim_indicators, dim_pessoas, dim_calendario)
- Estruturas organizacionais atualizadas em fact_estrutura_pessoas
- Permissões: SELECT no bronze, INSERT/UPDATE no silver
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- Configurações da sessão para otimização
SET NOCOUNT ON;
-- SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- ==============================================================================
-- 6. PROCEDURE PRINCIPAL
-- ==============================================================================
USE M7Medallion;
GO
CREATE OR ALTER PROCEDURE silver.prc_bronze_to_silver_performance_assignments
    @ExecutionId VARCHAR(50) = NULL,
    @ProcessOnlyNew BIT = 1,
    @Debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Variáveis de controle
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @ProcessName VARCHAR(100) = 'sp_populate_performance_assignments';
    DECLARE @RecordsProcessed INT = 0;
    DECLARE @RecordsInserted INT = 0;
    DECLARE @RecordsUpdated INT = 0;
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    -- ExecutionId único para rastreamento
    IF @ExecutionId IS NULL
        SET @ExecutionId = CONCAT('ASS-', FORMAT(GETDATE(), 'yyyyMMdd-HHmmss'));
    
    BEGIN TRY
        
        IF @Debug = 1
            PRINT CONCAT('[', FORMAT(GETDATE(), 'HH:mm:ss'), '] Iniciando ', @ProcessName, ' - ExecutionId: ', @ExecutionId);
        
        -- ==============================================================================
        -- VALIDAÇÕES INICIAIS
        -- ==============================================================================
        
        -- Verificar se existem dados no bronze para processar
        IF NOT EXISTS (SELECT 1 FROM bronze.performance_assignments WHERE is_processed = 0 OR @ProcessOnlyNew = 0)
        BEGIN
            IF @Debug = 1
                PRINT '[INFO] Nenhum registro novo para processar no bronze.performance_assignments';
            RETURN;
        END
        
        -- ==============================================================================
        -- PREPARAÇÃO - INVALIDAR REGISTROS ATUAIS (SCD TIPO 2)
        -- ==============================================================================
        
        -- Invalidar registros que serão atualizados (historização)
        UPDATE silver.fact_performance_assignments
        SET 
            is_current = 0,
            valid_to = DATEADD(DAY, -1, CAST(bronze.valid_from AS DATE))
        FROM bronze.performance_assignments bronze
        INNER JOIN silver.dim_indicators di ON bronze.indicator_code = di.indicator_id
        WHERE (bronze.is_processed = 0 OR @ProcessOnlyNew = 0)
          AND silver.fact_performance_assignments.crm_id = bronze.codigo_assessor_crm
          AND silver.fact_performance_assignments.indicator_sk = di.indicator_sk
          AND silver.fact_performance_assignments.is_current = 1;
        
        SET @RecordsUpdated = @@ROWCOUNT;
        
        IF @Debug = 1
            PRINT CONCAT('[INFO] Registros invalidados: ', CAST(@RecordsUpdated AS VARCHAR(10)));
        
        -- ==============================================================================
        -- TRANSFORMAÇÃO E INSERÇÃO DE NOVOS REGISTROS
        -- ==============================================================================
        
        INSERT INTO silver.fact_performance_assignments (
            indicator_sk,
            crm_id,
            data_ref,
            id_estrutura,
            peso,
            trimestre,
            ano,
            valid_from,
            valid_to,
            is_current,
            created_date,
            created_by
        )
        SELECT 
            -- Resolução de chaves estrangeiras
            di.indicator_sk,
            bronze.codigo_assessor_crm AS crm_id,
            cal.data_ref,
            COALESCE(ep.id_estrutura, 1) AS id_estrutura, -- Default estrutura se não encontrar
            
            -- Transformações de atributos
            CAST(COALESCE(NULLIF(bronze.weight, ''), '0') AS DECIMAL(5,2)) AS peso,
            CONCAT(CAST(YEAR(CAST(bronze.valid_from AS DATE)) AS VARCHAR(4)), '-', cal.trimestre) AS trimestre,
            YEAR(CAST(bronze.valid_from AS DATE)) AS ano,
            CAST(bronze.valid_from AS DATE) AS valid_from,
            CASE 
                WHEN bronze.valid_to = '9999-12-31' OR bronze.valid_to IS NULL 
                THEN CAST('9999-12-31' AS DATE)
                ELSE CAST(bronze.valid_to AS DATE)
            END AS valid_to,
            1 AS is_current,
            
            -- Campos de auditoria
            @StartTime AS created_date,
            CONCAT(@ProcessName, '-', @ExecutionId) AS created_by
            
        FROM bronze.performance_assignments bronze
        
        -- Joins para resolução de surrogate keys
        INNER JOIN silver.dim_indicators di 
            ON bronze.indicator_code = di.indicator_id
            AND di.is_active = 1
        
        -- Join com calendário para obter data_ref e trimestre
        INNER JOIN silver.dim_calendario cal 
            ON cal.data_ref = CAST(bronze.valid_from AS DATE)
        
        -- Validação se pessoa existe
        INNER JOIN silver.dim_pessoas dp 
            ON bronze.codigo_assessor_crm = dp.crm_id
        
        -- Buscar estrutura organizacional vigente na data
        LEFT JOIN silver.fact_estrutura_pessoas ep 
            ON bronze.codigo_assessor_crm = ep.crm_id
            AND CAST(bronze.valid_from AS DATE) >= ep.data_entrada
            AND (ep.data_saida IS NULL OR CAST(bronze.valid_from AS DATE) <= ep.data_saida)
        
        WHERE 
            -- Filtros de qualidade de dados
            bronze.indicator_exists = 1  -- Só indicadores que existem
            AND bronze.weight_sum_valid = 1  -- Só pessoas com soma de peso válida
            AND bronze.validation_errors IS NULL  -- Sem erros de validação
            AND (bronze.is_processed = 0 OR @ProcessOnlyNew = 0)  -- Controle de processamento
            
        -- Evitar duplicatas na mesma execução
        AND NOT EXISTS (
            SELECT 1 
            FROM silver.fact_performance_assignments existing
            WHERE existing.crm_id = bronze.codigo_assessor_crm
              AND existing.indicator_sk = di.indicator_sk
              AND existing.trimestre = CONCAT(CAST(YEAR(CAST(bronze.valid_from AS DATE)) AS VARCHAR(4)), '-', cal.trimestre)
              AND existing.is_current = 1
        );
        
        SET @RecordsInserted = @@ROWCOUNT;
        
        IF @Debug = 1
            PRINT CONCAT('[INFO] Registros inseridos: ', CAST(@RecordsInserted AS VARCHAR(10)));
        
        -- ==============================================================================
        -- MARCAÇÃO DE PROCESSAMENTO
        -- ==============================================================================
        
        UPDATE bronze.performance_assignments 
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
        
        -- Validar regra de negócio: Soma de pesos CARD = 100% por pessoa/trimestre
        DECLARE @InvalidWeightSums TABLE (
            crm_id VARCHAR(20),
            trimestre VARCHAR(10),
            soma_card DECIMAL(8,2)
        );
        
        INSERT INTO @InvalidWeightSums
        SELECT 
            fa.crm_id,
            fa.trimestre,
            SUM(fa.peso) as soma_card
        FROM silver.fact_performance_assignments fa
        INNER JOIN silver.dim_indicators di ON fa.indicator_sk = di.indicator_sk
        WHERE fa.is_current = 1
          AND di.tipo = 'CARD'
        GROUP BY fa.crm_id, fa.trimestre
        HAVING SUM(fa.peso) <> 100;
        
        IF EXISTS (SELECT 1 FROM @InvalidWeightSums)
        BEGIN
            SELECT 
                'AVISO: Soma de pesos CARD diferente de 100%' AS Tipo,
                crm_id,
                trimestre,
                soma_card
            FROM @InvalidWeightSums;
        END
        
        -- ==============================================================================
        -- LOG DE EXECUÇÃO
        -- ==============================================================================
        
        DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, GETDATE());
        
        PRINT '=== RESUMO EXECUÇÃO ===';
        PRINT CONCAT('ExecutionId: ', @ExecutionId);
        PRINT CONCAT('Duração: ', CAST(@Duration AS VARCHAR(10)), ' segundos');
        PRINT CONCAT('Registros processados (bronze): ', CAST(@RecordsProcessed AS VARCHAR(10)));
        PRINT CONCAT('Registros atualizados (silver): ', CAST(@RecordsUpdated AS VARCHAR(10)));
        PRINT CONCAT('Registros inseridos (silver): ', CAST(@RecordsInserted AS VARCHAR(10)));
        PRINT CONCAT('Finalizado em: ', FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss'));
        
    END TRY
    BEGIN CATCH
        
        -- Em caso de erro, reverter marcação de processado
        UPDATE bronze.performance_assignments 
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
-- Procedure para validar soma de pesos por pessoa/trimestre
CREATE OR ALTER PROCEDURE silver.sp_validate_assignment_weights
    @trimestre VARCHAR(10) = NULL
AS
BEGIN
    SELECT 
        p.nome_pessoa,
        fa.crm_id,
        fa.trimestre,
        SUM(CASE WHEN di.tipo = 'CARD' THEN fa.peso ELSE 0 END) as soma_card,
        COUNT(CASE WHEN di.tipo = 'CARD' THEN 1 END) as qtd_cards
    FROM silver.fact_performance_assignments fa
    INNER JOIN silver.dim_pessoas p ON fa.crm_id = p.crm_id
    INNER JOIN silver.dim_indicators di ON fa.indicator_sk = di.indicator_sk
    WHERE fa.is_current = 1
      AND (@trimestre IS NULL OR fa.trimestre = @trimestre)
    GROUP BY p.nome_pessoa, fa.crm_id, fa.trimestre
    HAVING SUM(CASE WHEN di.tipo = 'CARD' THEN fa.peso ELSE 0 END) <> 100
    ORDER BY fa.trimestre DESC, p.nome_pessoa;
END;

-- Procedure para consultar atribuições vigentes de uma pessoa
CREATE OR ALTER PROCEDURE silver.sp_get_person_assignments
    @crm_id VARCHAR(20)
AS
BEGIN
    SELECT 
        p.nome_pessoa,
        i.nome as indicador,
        i.tipo,
        fa.peso,
        fa.trimestre,
        fa.valid_from,
        fa.valid_to,
        fa.is_current
    FROM silver.fact_performance_assignments fa
    INNER JOIN silver.dim_pessoas p ON fa.crm_id = p.crm_id
    INNER JOIN silver.dim_indicators i ON fa.indicator_sk = i.indicator_sk
    WHERE fa.crm_id = @crm_id
      AND fa.is_current = 1
    ORDER BY i.tipo, fa.peso DESC;
END;
*/

-- ==============================================================================
-- 8. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                          | Descrição
--------|------------|--------------------------------|------------------------------------------
1.0.0   | 2025-06-24 | bruno.chiaramonti@multisete.com| Criação inicial da procedure ETL
1.0.1   | 2025-06-24 | bruno.chiaramonti@multisete.com| Correção formato trimestre (remover -Q extra)

*/

-- ==============================================================================
-- 9. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Procedure implementa SCD Tipo 2 para manter histórico completo
- Validação de regras de negócio é executada após inserção
- Utiliza ExecutionId único para rastreabilidade completa
- Controle de processamento incremental via flag is_processed

Troubleshooting comum:
1. Erro "CHK_trimestre_formato": Verificar se formato está correto (YYYY-QN)
2. FK constraint violation: Verificar se dimensões estão atualizadas
3. Soma pesos ≠ 100%: Validar dados no bronze antes do processamento
4. Timeout: Executar em horário de menor carga no banco

Monitoramento:
- Logs detalhados disponíveis com @Debug = 1
- Status de processamento gravado em bronze.performance_assignments
- Validações pós-processamento alertam sobre inconsistências

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/

-- ==============================================================================
-- GRANTS DE SEGURANÇA
-- ==============================================================================

-- GRANT EXECUTE ON silver.sp_populate_performance_assignments TO role_etl_service;
-- GRANT EXECUTE ON silver.sp_populate_performance_assignments TO role_performance_manager;