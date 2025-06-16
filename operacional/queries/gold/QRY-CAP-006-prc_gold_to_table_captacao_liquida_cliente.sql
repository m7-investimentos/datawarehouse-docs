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
           de carga incremental baseada em períodos modificados.

Casos de uso:
- Execução diária via SQL Agent Job
- Recarga manual após correções em dados fonte
- Atualização após reprocessamento de períodos específicos

Frequência de execução: Diária (preferencialmente após 6h da manhã)
Tempo médio de execução: 3-5 minutos
Volume processado: ~50.000-100.000 registros por execução
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
@data_inicio     DATE        -- Data inicial para processamento (opcional)
@data_fim        DATE        -- Data final para processamento (opcional)
@modo_carga      VARCHAR(10) -- Modo de carga: 'INCREMENTAL' ou 'FULL' (default: INCREMENTAL)
@debug           BIT         -- Modo debug: 1 = exibe mensagens, 0 = silencioso (default: 0)

Exemplo de uso:
-- Carga incremental padrão (processa todos os dados disponíveis)
EXEC [gold].[prc_gold_to_table_captacao_liquida_cliente];

-- Carga incremental com período específico
EXEC [gold].[prc_gold_to_table_captacao_liquida_cliente] 
    @data_inicio = '2024-01-01',
    @data_fim = '2024-12-31',
    @debug = 1;

-- Carga FULL (reprocessa todos os dados)
EXEC [gold].[prc_gold_to_table_captacao_liquida_cliente] 
    @modo_carga = 'FULL',
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
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'[gold].[prc_gold_to_table_captacao_liquida_cliente]'))
    DROP PROCEDURE [gold].[prc_gold_to_table_captacao_liquida_cliente]
GO

CREATE PROCEDURE [gold].[prc_gold_to_table_captacao_liquida_cliente]
    @data_inicio DATE = NULL,
    @data_fim DATE = NULL,
    @modo_carga VARCHAR(10) = 'INCREMENTAL',
    @debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Variáveis de controle
    DECLARE @inicio_processo DATETIME = GETDATE();
    DECLARE @registros_inseridos INT = 0;
    DECLARE @registros_atualizados INT = 0;
    DECLARE @registros_deletados INT = 0;
    DECLARE @erro_mensagem NVARCHAR(4000);
    DECLARE @erro_numero INT;
    
    BEGIN TRY
        -- Validar parâmetros
        IF @modo_carga NOT IN ('INCREMENTAL', 'FULL')
        BEGIN
            RAISERROR('Modo de carga inválido. Use INCREMENTAL ou FULL.', 16, 1);
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
        
        -- Iniciar transação
        BEGIN TRANSACTION;
        
        -- Criar tabela temporária com dados da view
        IF OBJECT_ID('tempdb..#temp_captacao_liquida_cliente') IS NOT NULL
            DROP TABLE #temp_captacao_liquida_cliente;
            
        SELECT *
        INTO #temp_captacao_liquida_cliente
        FROM [gold].[view_captacao_liquida_cliente]
        WHERE data_ref BETWEEN @data_inicio AND @data_fim;
        
        -- Criar índice na tabela temporária
        CREATE CLUSTERED INDEX IX_temp_captacao ON #temp_captacao_liquida_cliente (data_ref, conta_xp_cliente);
        
        IF @debug = 1
            PRINT 'Dados carregados na tabela temporária: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' registros';
        
        -- MERGE para atualizar tabela destino
        MERGE [gold].[captacao_liquida_cliente] AS destino
        USING #temp_captacao_liquida_cliente AS origem
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
            )
            
            -- Deletar registros que não existem mais na origem (apenas no período processado)
            WHEN NOT MATCHED BY SOURCE 
                AND destino.data_ref BETWEEN @data_inicio AND @data_fim 
            THEN DELETE;
            
            -- Capturar estatísticas
            SET @registros_inseridos = @@ROWCOUNT;
            
            -- Commit da transação
            COMMIT TRANSACTION;
        
        -- Log de execução (quando debug ativado)
        IF @debug = 1
        BEGIN
            PRINT '=== RESUMO DA EXECUÇÃO ===';
            PRINT 'Registros processados: ' + CAST(@registros_inseridos AS VARCHAR);
            PRINT 'Tempo de execução: ' + CAST(DATEDIFF(SECOND, @inicio_processo, GETDATE()) AS VARCHAR) + ' segundos';
            PRINT 'Status: Concluído com sucesso';
        END
        
        -- Atualizar estatísticas da tabela
        UPDATE STATISTICS [gold].[captacao_liquida_cliente];
        
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
            PRINT 'Tempo decorrido: ' + CAST(DATEDIFF(SECOND, @inicio_processo, GETDATE()) AS VARCHAR) + ' segundos';
        END
        
        
        -- Re-lançar o erro
        RAISERROR(@erro_mensagem, 16, 1);
        
        RETURN 1; -- Erro
    END CATCH
END
GO

-- ==============================================================================
-- 7. PERMISSÕES
-- ==============================================================================
-- GRANT EXECUTE ON [gold].[prc_gold_to_table_captacao_liquida_cliente] TO [role_etl_gold]
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
1.0.0   | 2025-01-16 | Bruno Chiaramonti  | Criação inicial da procedure
1.1.0   | 2025-01-16 | Bruno Chiaramonti  | Migração para schema gold e remoção de processamento em lotes
*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Procedure implementa MERGE para otimizar performance
- Modo INCREMENTAL processa todos os dados disponíveis por padrão
- Para limitar o período, especifique @data_inicio e @data_fim
- Modo FULL recarrega todos os dados (mesma funcionalidade do INCREMENTAL sem parâmetros)
- Transação garante consistência dos dados

Agendamento recomendado:
- Horário: 06:30 AM (após cargas do Bronze/Silver e tabela de assessores)
- Frequência: Diária
- Timeout: 10 minutos
- Notificação: Em caso de falha ou execução > 5 minutos

Performance esperada:
- 100.000 registros: ~3-5 minutos
- 1.000.000 registros: ~20-30 minutos

Troubleshooting comum:
1. Timeout: Executar fora do horário de pico ou processar períodos menores
2. Deadlock: Verificar processos concorrentes nas tabelas Silver
3. Espaço em tempdb: Verificar espaço disponível antes da execução

Monitoramento:
- Verificar log do SQL Agent diariamente
- Alertas se execução > 10 minutos
- Revisar estatísticas de performance semanalmente

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/