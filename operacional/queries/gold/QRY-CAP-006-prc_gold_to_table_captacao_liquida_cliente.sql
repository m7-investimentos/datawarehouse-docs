-- ==============================================================================
-- QRY-CAP-006-prc_gold_to_table_captacao_liquida_cliente
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-16
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [captação, cliente, procedure, etl, gold]
-- Status: desenvolvimento
-- Banco de Dados: SQL Server
-- Schema: dbo
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por atualizar a tabela materializada de captação
           líquida por cliente a partir da view correspondente. Implementa lógica
           de carga incremental baseada em períodos modificados e controle de
           performance para grandes volumes.

Casos de uso:
- Execução diária via SQL Agent Job
- Recarga manual após correções em dados fonte
- Atualização após reprocessamento de períodos específicos
- Carga em lotes para grandes volumes de clientes

Frequência de execução: Diária (preferencialmente após 6h da manhã)
Tempo médio de execução: 2-3 minutos
Volume processado: ~50.000-100.000 registros por execução
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
@data_inicio     DATE        -- Data inicial para processamento (opcional)
@data_fim        DATE        -- Data final para processamento (opcional)
@modo_carga      VARCHAR(10) -- Modo de carga: 'INCREMENTAL' ou 'FULL' (default: INCREMENTAL)
@batch_size      INT         -- Tamanho do lote para processamento (default: 10000)
@debug           BIT         -- Modo debug: 1 = exibe mensagens, 0 = silencioso (default: 0)

Exemplo de uso:
-- Carga incremental padrão (processa todos os dados disponíveis)
EXEC [dbo].[prc_gold_performance_to_table_captacao_liquida_cliente];

-- Carga incremental com período específico
EXEC [dbo].[prc_gold_performance_to_table_captacao_liquida_cliente] 
    @data_inicio = '2024-01-01',
    @data_fim = '2024-12-31',
    @debug = 1;

-- Carga FULL (reprocessa todos os dados)
EXEC [dbo].[prc_gold_performance_to_table_captacao_liquida_cliente] 
    @modo_carga = 'FULL',
    @batch_size = 50000,
    @debug = 1;
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Retorno da procedure:
- 0: Sucesso
- 1: Erro na execução

Mensagens (quando @debug = 1):
- Quantidade de registros processados
- Tempo de execução por lote
- Períodos afetados
- Estatísticas de performance
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Views utilizadas:
- [gold].[view_captacao_liquida_cliente]: Fonte dos dados

Tabelas atualizadas:
- [gold].[captacao_liquida_cliente]: Destino dos dados

Pré-requisitos:
- View deve existir e estar funcional
- Tabela destino deve existir
- Permissões de SELECT na view e INSERT/UPDATE/DELETE na tabela
- Espaço adequado em tempdb para processamento
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ==============================================================================
-- 6. IMPLEMENTAÇÃO DA PROCEDURE
-- ==============================================================================

-- Remover procedure existente se necessário
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'[dbo].[prc_gold_to_table_captacao_liquida_cliente]'))
    DROP PROCEDURE [dbo].[prc_gold_to_table_captacao_liquida_cliente]
GO

CREATE PROCEDURE [dbo].[prc_gold_to_table_captacao_liquida_cliente]
    @data_inicio DATE = NULL,
    @data_fim DATE = NULL,
    @modo_carga VARCHAR(10) = 'INCREMENTAL',
    @batch_size INT = 10000,
    @debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Variáveis de controle
    DECLARE @inicio_processo DATETIME = GETDATE();
    DECLARE @inicio_lote DATETIME;
    DECLARE @registros_inseridos INT = 0;
    DECLARE @registros_atualizados INT = 0;
    DECLARE @registros_deletados INT = 0;
    DECLARE @total_registros INT = 0;
    DECLARE @lotes_processados INT = 0;
    DECLARE @erro_mensagem NVARCHAR(4000);
    DECLARE @erro_numero INT;
    
    BEGIN TRY
        -- Validar parâmetros
        IF @modo_carga NOT IN ('INCREMENTAL', 'FULL')
        BEGIN
            RAISERROR('Modo de carga inválido. Use INCREMENTAL ou FULL.', 16, 1);
            RETURN 1;
        END
        
        IF @batch_size < 1000 OR @batch_size > 100000
        BEGIN
            RAISERROR('Batch size deve estar entre 1000 e 100000.', 16, 1);
            RETURN 1;
        END
        
        -- Definir período de processamento
        IF @modo_carga = 'FULL'
        BEGIN
            -- Em modo FULL, processar todos os dados disponíveis
            SET @data_inicio = ISNULL(@data_inicio, '2020-01-01');
            SET @data_fim = ISNULL(@data_fim, DATEADD(DAY, -1, CAST(GETDATE() AS DATE)));
            
            IF @debug = 1
                PRINT 'Modo FULL: Processando período de ' + CONVERT(VARCHAR, @data_inicio, 103) + 
                      ' até ' + CONVERT(VARCHAR, @data_fim, 103);
        END
        ELSE
        BEGIN
            -- Em modo INCREMENTAL, processar todos os dados disponíveis por padrão
            SET @data_inicio = ISNULL(@data_inicio, '2020-01-01');
            SET @data_fim = ISNULL(@data_fim, DATEADD(DAY, -1, CAST(GETDATE() AS DATE)));
            
            IF @debug = 1
                PRINT 'Modo INCREMENTAL: Processando período de ' + CONVERT(VARCHAR, @data_inicio, 103) + 
                      ' até ' + CONVERT(VARCHAR, @data_fim, 103);
        END
        
        -- Criar tabela temporária com dados da view
        IF OBJECT_ID('tempdb..#temp_captacao_liquida_cliente') IS NOT NULL
            DROP TABLE #temp_captacao_liquida_cliente;
            
        -- Criar estrutura da tabela temporária com índice
        CREATE TABLE #temp_captacao_liquida_cliente (
            data_ref DATE NOT NULL,
            conta_xp_cliente INT NOT NULL,
            ano INT,
            mes INT,
            nome_mes VARCHAR(20),
            trimestre CHAR(2),
            nome_cliente VARCHAR(200),
            tipo_cliente VARCHAR(10),
            grupo_cliente VARCHAR(100),
            segmento_cliente VARCHAR(50),
            status_cliente VARCHAR(50),
            faixa_etaria VARCHAR(50),
            codigo_cliente_crm VARCHAR(100),
            cod_assessor VARCHAR(50),
            nome_assessor VARCHAR(200),
            assessor_nivel VARCHAR(50),
            assessor_status VARCHAR(50),
            codigo_assessor_crm VARCHAR(20),
            nome_estrutura VARCHAR(100),
            captacao_bruta_xp DECIMAL(18,2),
            captacao_bruta_transferencia DECIMAL(18,2),
            captacao_bruta_total DECIMAL(18,2),
            resgate_bruto_xp DECIMAL(18,2),
            resgate_bruto_transferencia DECIMAL(18,2),
            resgate_bruto_total DECIMAL(18,2),
            captacao_liquida_xp DECIMAL(18,2),
            captacao_liquida_transferencia DECIMAL(18,2),
            captacao_liquida_total DECIMAL(18,2),
            qtd_operacoes_aporte INT,
            qtd_operacoes_resgate INT,
            ticket_medio_aporte DECIMAL(18,2),
            ticket_medio_resgate DECIMAL(18,2),
            meses_como_cliente INT,
            primeira_captacao DATE,
            ultima_captacao DATE,
            ultimo_resgate DATE,
            lote_processamento INT,
            PRIMARY KEY CLUSTERED (data_ref, conta_xp_cliente)
        );
        
        -- Carregar dados da view em lotes
        IF @debug = 1
            PRINT 'Iniciando carga de dados da view para tabela temporária...';
            
        SET @inicio_lote = GETDATE();
        
        -- Inserir dados com numeração de lotes para processamento controlado
        INSERT INTO #temp_captacao_liquida_cliente
        SELECT 
            *,
            (ROW_NUMBER() OVER (ORDER BY data_ref, conta_xp_cliente) - 1) / @batch_size + 1 AS lote_processamento
        FROM [gold].[view_captacao_liquida_cliente]
        WHERE data_ref BETWEEN @data_inicio AND @data_fim;
        
        SET @total_registros = @@ROWCOUNT;
        
        IF @debug = 1
        BEGIN
            PRINT 'Dados carregados na tabela temporária: ' + CAST(@total_registros AS VARCHAR) + ' registros';
            PRINT 'Tempo de carga: ' + CAST(DATEDIFF(SECOND, @inicio_lote, GETDATE()) AS VARCHAR) + ' segundos';
            PRINT 'Total de lotes a processar: ' + CAST(CEILING(CAST(@total_registros AS FLOAT) / @batch_size) AS VARCHAR);
        END
        
        -- Processar dados em lotes
        DECLARE @lote_atual INT = 1;
        DECLARE @max_lote INT = CEILING(CAST(@total_registros AS FLOAT) / @batch_size);
        
        WHILE @lote_atual <= @max_lote
        BEGIN
            BEGIN TRANSACTION;
            
            SET @inicio_lote = GETDATE();
            
            -- MERGE para atualizar tabela destino (por lote)
            MERGE [gold].[captacao_liquida_cliente] AS destino
            USING (
                SELECT * FROM #temp_captacao_liquida_cliente
                WHERE lote_processamento = @lote_atual
            ) AS origem
            ON (destino.data_ref = origem.data_ref AND destino.conta_xp_cliente = origem.conta_xp_cliente)
            
            -- Atualizar registros existentes que mudaram
            WHEN MATCHED AND (
                ISNULL(destino.captacao_bruta_total, 0) <> ISNULL(origem.captacao_bruta_total, 0) OR
                ISNULL(destino.resgate_bruto_total, 0) <> ISNULL(origem.resgate_bruto_total, 0) OR
                ISNULL(destino.captacao_liquida_total, 0) <> ISNULL(origem.captacao_liquida_total, 0) OR
                ISNULL(destino.qtd_operacoes_aporte, 0) <> ISNULL(origem.qtd_operacoes_aporte, 0) OR
                ISNULL(destino.qtd_operacoes_resgate, 0) <> ISNULL(origem.qtd_operacoes_resgate, 0) OR
                ISNULL(destino.nome_cliente, '') <> ISNULL(origem.nome_cliente, '') OR
                ISNULL(destino.cod_assessor, '') <> ISNULL(origem.cod_assessor, '') OR
                ISNULL(destino.nome_assessor, '') <> ISNULL(origem.nome_assessor, '') OR
                ISNULL(destino.meses_como_cliente, 0) <> ISNULL(origem.meses_como_cliente, 0) OR
                ISNULL(destino.ultima_captacao, '1900-01-01') <> ISNULL(origem.ultima_captacao, '1900-01-01') OR
                ISNULL(destino.ultimo_resgate, '1900-01-01') <> ISNULL(origem.ultimo_resgate, '1900-01-01')
            )
            THEN UPDATE SET
                ano = origem.ano,
                mes = origem.mes,
                nome_mes = origem.nome_mes,
                trimestre = origem.trimestre,
                nome_cliente = origem.nome_cliente,
                tipo_cliente = origem.tipo_cliente,
                grupo_cliente = origem.grupo_cliente,
                segmento_cliente = origem.segmento_cliente,
                status_cliente = origem.status_cliente,
                faixa_etaria = origem.faixa_etaria,
                codigo_cliente_crm = origem.codigo_cliente_crm,
                cod_assessor = origem.cod_assessor,
                nome_assessor = origem.nome_assessor,
                assessor_nivel = origem.assessor_nivel,
                assessor_status = origem.assessor_status,
                codigo_assessor_crm = origem.codigo_assessor_crm,
                nome_estrutura = origem.nome_estrutura,
                captacao_bruta_xp = origem.captacao_bruta_xp,
                captacao_bruta_transferencia = origem.captacao_bruta_transferencia,
                captacao_bruta_total = origem.captacao_bruta_total,
                resgate_bruto_xp = origem.resgate_bruto_xp,
                resgate_bruto_transferencia = origem.resgate_bruto_transferencia,
                resgate_bruto_total = origem.resgate_bruto_total,
                captacao_liquida_xp = origem.captacao_liquida_xp,
                captacao_liquida_transferencia = origem.captacao_liquida_transferencia,
                captacao_liquida_total = origem.captacao_liquida_total,
                qtd_operacoes_aporte = origem.qtd_operacoes_aporte,
                qtd_operacoes_resgate = origem.qtd_operacoes_resgate,
                ticket_medio_aporte = origem.ticket_medio_aporte,
                ticket_medio_resgate = origem.ticket_medio_resgate,
                meses_como_cliente = origem.meses_como_cliente,
                primeira_captacao = origem.primeira_captacao,
                ultima_captacao = origem.ultima_captacao,
                ultimo_resgate = origem.ultimo_resgate,
                data_carga = GETDATE(),
                hash_registro = HASHBYTES('SHA2_256', 
                    CAST(origem.data_ref AS VARCHAR(10)) + '|' +
                    CAST(origem.conta_xp_cliente AS VARCHAR(20)) + '|' +
                    ISNULL(origem.cod_assessor, '') + '|' +
                    CAST(ISNULL(origem.captacao_bruta_total, 0) AS VARCHAR(20)) + '|' +
                    CAST(ISNULL(origem.resgate_bruto_total, 0) AS VARCHAR(20)) + '|' +
                    CAST(ISNULL(origem.meses_como_cliente, 0) AS VARCHAR(10))
                )
            
            -- Inserir novos registros
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                data_ref, ano, mes, nome_mes, trimestre,
                conta_xp_cliente, nome_cliente, tipo_cliente, grupo_cliente,
                segmento_cliente, status_cliente, faixa_etaria, codigo_cliente_crm,
                cod_assessor, nome_assessor, assessor_nivel, assessor_status,
                codigo_assessor_crm, nome_estrutura,
                captacao_bruta_xp, captacao_bruta_transferencia, captacao_bruta_total,
                resgate_bruto_xp, resgate_bruto_transferencia, resgate_bruto_total,
                captacao_liquida_xp, captacao_liquida_transferencia, captacao_liquida_total,
                qtd_operacoes_aporte, qtd_operacoes_resgate,
                ticket_medio_aporte, ticket_medio_resgate,
                meses_como_cliente, primeira_captacao, ultima_captacao, ultimo_resgate,
                data_carga, hash_registro
            )
            VALUES (
                origem.data_ref, origem.ano, origem.mes, origem.nome_mes, origem.trimestre,
                origem.conta_xp_cliente, origem.nome_cliente, origem.tipo_cliente, origem.grupo_cliente,
                origem.segmento_cliente, origem.status_cliente, origem.faixa_etaria, origem.codigo_cliente_crm,
                origem.cod_assessor, origem.nome_assessor, origem.assessor_nivel, origem.assessor_status,
                origem.codigo_assessor_crm, origem.nome_estrutura,
                origem.captacao_bruta_xp, origem.captacao_bruta_transferencia, origem.captacao_bruta_total,
                origem.resgate_bruto_xp, origem.resgate_bruto_transferencia, origem.resgate_bruto_total,
                origem.captacao_liquida_xp, origem.captacao_liquida_transferencia, origem.captacao_liquida_total,
                origem.qtd_operacoes_aporte, origem.qtd_operacoes_resgate,
                origem.ticket_medio_aporte, origem.ticket_medio_resgate,
                origem.meses_como_cliente, origem.primeira_captacao, origem.ultima_captacao, origem.ultimo_resgate,
                GETDATE(),
                HASHBYTES('SHA2_256', 
                    CAST(origem.data_ref AS VARCHAR(10)) + '|' +
                    CAST(origem.conta_xp_cliente AS VARCHAR(20)) + '|' +
                    ISNULL(origem.cod_assessor, '') + '|' +
                    CAST(ISNULL(origem.captacao_bruta_total, 0) AS VARCHAR(20)) + '|' +
                    CAST(ISNULL(origem.resgate_bruto_total, 0) AS VARCHAR(20)) + '|' +
                    CAST(ISNULL(origem.meses_como_cliente, 0) AS VARCHAR(10))
                )
            );
            
            SET @registros_inseridos = @registros_inseridos + @@ROWCOUNT;
            
            -- Deletar registros órfãos (apenas para o período e clientes do lote atual)
            IF @modo_carga = 'FULL'
            BEGIN
                DELETE destino
                FROM [gold].[captacao_liquida_cliente] destino
                WHERE destino.data_ref BETWEEN @data_inicio AND @data_fim
                  AND destino.conta_xp_cliente IN (
                      SELECT DISTINCT conta_xp_cliente 
                      FROM #temp_captacao_liquida_cliente 
                      WHERE lote_processamento = @lote_atual
                  )
                  AND NOT EXISTS (
                      SELECT 1 
                      FROM #temp_captacao_liquida_cliente origem
                      WHERE origem.data_ref = destino.data_ref
                        AND origem.conta_xp_cliente = destino.conta_xp_cliente
                        AND origem.lote_processamento = @lote_atual
                  );
                  
                SET @registros_deletados = @registros_deletados + @@ROWCOUNT;
            END
            
            COMMIT TRANSACTION;
            
            SET @lotes_processados = @lotes_processados + 1;
            
            IF @debug = 1
            BEGIN
                PRINT 'Lote ' + CAST(@lote_atual AS VARCHAR) + '/' + CAST(@max_lote AS VARCHAR) + 
                      ' processado em ' + CAST(DATEDIFF(SECOND, @inicio_lote, GETDATE()) AS VARCHAR) + ' segundos';
            END
            
            SET @lote_atual = @lote_atual + 1;
            
            -- Pequena pausa entre lotes para não sobrecarregar o sistema
            IF @lote_atual <= @max_lote
                WAITFOR DELAY '00:00:01';
        END
        
        -- Log de execução (quando debug ativado)
        IF @debug = 1
        BEGIN
            PRINT '=== RESUMO DA EXECUÇÃO ===';
            PRINT 'Total de registros processados: ' + CAST(@total_registros AS VARCHAR);
            PRINT 'Registros inseridos/atualizados: ' + CAST(@registros_inseridos AS VARCHAR);
            PRINT 'Registros deletados: ' + CAST(@registros_deletados AS VARCHAR);
            PRINT 'Lotes processados: ' + CAST(@lotes_processados AS VARCHAR);
            PRINT 'Tempo total de execução: ' + CAST(DATEDIFF(SECOND, @inicio_processo, GETDATE()) AS VARCHAR) + ' segundos';
            PRINT 'Status: Concluído com sucesso';
        END
        
        -- Atualizar estatísticas da tabela
        UPDATE STATISTICS [gold].[captacao_liquida_cliente] WITH FULLSCAN;
        
        -- Limpar tabela temporária
        DROP TABLE #temp_captacao_liquida_cliente;
        
        RETURN 0; -- Sucesso
        
    END TRY
    BEGIN CATCH
        -- Rollback em caso de erro
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        -- Capturar informações do erro
        SET @erro_numero = ERROR_NUMBER();
        SET @erro_mensagem = ERROR_MESSAGE();
        
        -- Log de erro
        IF @debug = 1
        BEGIN
            PRINT '=== ERRO NA EXECUÇÃO ===';
            PRINT 'Número do erro: ' + CAST(@erro_numero AS VARCHAR);
            PRINT 'Mensagem: ' + @erro_mensagem;
            PRINT 'Lote em processamento: ' + CAST(@lote_atual AS VARCHAR);
            PRINT 'Tempo decorrido: ' + CAST(DATEDIFF(SECOND, @inicio_processo, GETDATE()) AS VARCHAR) + ' segundos';
        END
        
        -- Limpar tabela temporária se existir
        IF OBJECT_ID('tempdb..#temp_captacao_liquida_cliente') IS NOT NULL
            DROP TABLE #temp_captacao_liquida_cliente;
        
        -- Re-lançar o erro
        RAISERROR(@erro_mensagem, 16, 1);
        
        RETURN 1; -- Erro
    END CATCH
END
GO

-- ==============================================================================
-- 7. PERMISSÕES
-- ==============================================================================
-- GRANT EXECUTE ON [dbo].[prc_gold_to_table_captacao_liquida_cliente] TO [role_etl_gold]
-- GO

-- ==============================================================================
-- 8. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================
/*
-- Verificar última execução
SELECT TOP 1 
    data_carga,
    COUNT(*) as registros_carregados,
    MIN(data_ref) as data_inicio,
    MAX(data_ref) as data_fim,
    COUNT(DISTINCT conta_xp_cliente) as clientes_unicos
FROM [gold].[captacao_liquida_cliente]
GROUP BY data_carga
ORDER BY data_carga DESC;

-- Comparar view vs tabela
SELECT 
    'View' as origem,
    COUNT(*) as total_registros,
    COUNT(DISTINCT conta_xp_cliente) as clientes_unicos,
    SUM(captacao_liquida_total) as captacao_liquida_total
FROM [gold].[view_captacao_liquida_cliente]
WHERE data_ref >= DATEADD(MONTH, -3, GETDATE())
UNION ALL
SELECT 
    'Tabela' as origem,
    COUNT(*) as total_registros,
    COUNT(DISTINCT conta_xp_cliente) as clientes_unicos,
    SUM(captacao_liquida_total) as captacao_liquida_total
FROM [gold].[captacao_liquida_cliente]
WHERE data_ref >= DATEADD(MONTH, -3, GETDATE());

-- Verificar clientes com maior movimentação
SELECT TOP 100
    conta_xp_cliente,
    nome_cliente,
    cod_assessor,
    COUNT(*) as meses_com_movimento,
    SUM(captacao_liquida_total) as captacao_liquida_acumulada,
    AVG(captacao_liquida_total) as captacao_liquida_media
FROM [gold].[captacao_liquida_cliente]
WHERE ano = 2024
GROUP BY conta_xp_cliente, nome_cliente, cod_assessor
ORDER BY ABS(SUM(captacao_liquida_total)) DESC;

-- Análise de performance por lote
WITH lote_stats AS (
    SELECT 
        DATEPART(HOUR, data_carga) as hora_carga,
        COUNT(*) as registros,
        COUNT(DISTINCT conta_xp_cliente) as clientes,
        DATEDIFF(SECOND, MIN(data_carga), MAX(data_carga)) as tempo_segundos
    FROM [gold].[captacao_liquida_cliente]
    WHERE data_carga >= DATEADD(DAY, -7, GETDATE())
    GROUP BY DATEPART(HOUR, data_carga), CAST(data_carga AS DATE)
)
SELECT 
    hora_carga,
    AVG(registros) as media_registros,
    AVG(tempo_segundos) as tempo_medio_segundos,
    AVG(CASE WHEN tempo_segundos > 0 THEN registros * 1.0 / tempo_segundos ELSE 0 END) as registros_por_segundo
FROM lote_stats
GROUP BY hora_carga
ORDER BY hora_carga;
*/

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor              | Descrição
--------|------------|--------------------|-----------------------------------------
1.0.0   | 2025-01-16 | Bruno Chiaramonti  | Criação inicial da procedure com processamento em lotes
*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Procedure implementa processamento em lotes para otimizar grandes volumes
- Modo INCREMENTAL processa todos os dados disponíveis por padrão
- Batch size configurável permite ajuste fino de performance
- Transação por lote garante consistência sem bloquear por muito tempo
- Estatísticas atualizadas com FULLSCAN ao final para otimizar consultas

Agendamento recomendado:
- Horário: 06:30 AM (após cargas do Bronze/Silver e tabela de assessores)
- Frequência: Diária
- Timeout: 10 minutos
- Notificação: Em caso de falha ou execução > 5 minutos

Performance esperada:
- 10.000 registros: ~10 segundos
- 100.000 registros: ~2 minutos
- 1.000.000 registros: ~15-20 minutos

Troubleshooting comum:
1. Timeout: Aumentar batch_size ou executar fora do horário de pico
2. Memória insuficiente: Reduzir batch_size
3. Bloqueios: Verificar processos concorrentes nas tabelas Silver
4. Espaço em tempdb: Monitorar crescimento durante execução

Monitoramento:
- Verificar log do SQL Agent diariamente
- Alertas se execução > 5 minutos
- Revisar estatísticas de performance semanalmente

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/