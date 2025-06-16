-- ==============================================================================
-- QRY-CAP-003-prc_gold_to_table_captacao_liquida_assessor
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-06
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [captação, assessor, procedure, etl, gold]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: dbo
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por atualizar a tabela materializada de captação
           líquida por assessor a partir da view correspondente. Implementa lógica
           de carga incremental baseada em períodos modificados.

Casos de uso:
- Execução diária via SQL Agent Job
- Recarga manual após correções em dados fonte
- Atualização após reprocessamento de períodos específicos

Frequência de execução: Diária (preferencialmente após 6h da manhã)
Tempo médio de execução: 30-60 segundos
Volume processado: ~500-1000 registros por execução
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
EXEC [gold].[prc_gold_to_table_captacao_liquida_assessor];

-- Carga incremental com período específico
EXEC [gold].[prc_gold_to_table_captacao_liquida_assessor] 
    @data_inicio = '2024-01-01',
    @data_fim = '2024-12-31',
    @debug = 1;

-- Carga FULL (reprocessa todos os dados)
EXEC [gold].[prc_gold_to_table_captacao_liquida_assessor] 
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
- Tempo de execução
- Períodos afetados
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Views utilizadas:
- [gold].[view_captacao_liquida_assessor]: Fonte dos dados

Tabelas atualizadas:
- [gold].[captacao_liquida_assessor]: Destino dos dados

Pré-requisitos:
- View deve existir e estar funcional
- Tabela destino deve existir
- Permissões de SELECT na view e INSERT/UPDATE/DELETE na tabela
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
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'[gold].[prc_gold_to_table_captacao_liquida_assessor]'))
    DROP PROCEDURE [gold].[prc_gold_to_table_captacao_liquida_assessor]
GO

CREATE PROCEDURE [gold].[prc_gold_to_table_captacao_liquida_assessor]
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
            -- Apenas aplicar filtro de data se explicitamente fornecido
            SET @data_inicio = ISNULL(@data_inicio, '2020-01-01');
            SET @data_fim = ISNULL(@data_fim, DATEADD(DAY, -1, CAST(GETDATE() AS DATE)));
            
            IF @debug = 1
                PRINT 'Modo INCREMENTAL: Processando período de ' + CONVERT(VARCHAR, @data_inicio, 103) + 
                      ' até ' + CONVERT(VARCHAR, @data_fim, 103);
        END
        
        -- Iniciar transação
        BEGIN TRANSACTION;
        
        -- Criar tabela temporária com dados da view
        IF OBJECT_ID('tempdb..#temp_captacao_liquida') IS NOT NULL
            DROP TABLE #temp_captacao_liquida;
            
        SELECT *
        INTO #temp_captacao_liquida
        FROM [gold].[view_captacao_liquida_assessor]
        WHERE data_ref BETWEEN @data_inicio AND @data_fim;
        
        -- Criar índice na tabela temporária
        CREATE CLUSTERED INDEX IX_temp_captacao ON #temp_captacao_liquida (data_ref, cod_assessor);
        
        IF @debug = 1
            PRINT 'Dados carregados na tabela temporária: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' registros';
        
        -- MERGE para atualizar tabela destino
        MERGE [gold].[captacao_liquida_assessor] AS destino
        USING #temp_captacao_liquida AS origem
        ON (destino.data_ref = origem.data_ref AND destino.cod_assessor = origem.cod_assessor)
        
        -- Atualizar registros existentes que mudaram
        WHEN MATCHED AND (
            ISNULL(destino.captacao_bruta_total, 0) <> ISNULL(origem.captacao_bruta_total, 0) OR
            ISNULL(destino.resgate_bruto_total, 0) <> ISNULL(origem.resgate_bruto_total, 0) OR
            ISNULL(destino.captacao_liquida_total, 0) <> ISNULL(origem.captacao_liquida_total, 0) OR
            ISNULL(destino.qtd_clientes_aportando, 0) <> ISNULL(origem.qtd_clientes_aportando, 0) OR
            ISNULL(destino.qtd_clientes_resgatando, 0) <> ISNULL(origem.qtd_clientes_resgatando, 0) OR
            ISNULL(destino.nome_assessor, '') <> ISNULL(origem.nome_assessor, '') OR
            ISNULL(destino.assessor_status, '') <> ISNULL(origem.assessor_status, '') OR
            ISNULL(destino.nome_estrutura, '') <> ISNULL(origem.nome_estrutura, '')
        )
        THEN UPDATE SET
            ano = origem.ano,
            mes = origem.mes,
            nome_mes = origem.nome_mes,
            trimestre = origem.trimestre,
            nome_assessor = origem.nome_assessor,
            assessor_nivel = origem.assessor_nivel,
            codigo_assessor_crm = origem.codigo_assessor_crm,
            assessor_status = origem.assessor_status,
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
            qtd_clientes_aportando = origem.qtd_clientes_aportando,
            qtd_clientes_resgatando = origem.qtd_clientes_resgatando,
            ticket_medio_aporte = origem.ticket_medio_aporte,
            ticket_medio_resgate = origem.ticket_medio_resgate,
            qtd_clientes_apenas_aportando = origem.qtd_clientes_apenas_aportando,
            qtd_clientes_apenas_resgatando = origem.qtd_clientes_apenas_resgatando,
            qtd_clientes_aporte_e_resgate = origem.qtd_clientes_aporte_e_resgate,
            data_carga = GETDATE(),
            hash_registro = HASHBYTES('SHA2_256', 
                CAST(origem.data_ref AS VARCHAR(10)) + '|' +
                ISNULL(origem.cod_assessor, '') + '|' +
                CAST(ISNULL(origem.captacao_bruta_total, 0) AS VARCHAR(20)) + '|' +
                CAST(ISNULL(origem.resgate_bruto_total, 0) AS VARCHAR(20)) + '|' +
                CAST(ISNULL(origem.qtd_clientes_aportando, 0) AS VARCHAR(10))
            )
        
        -- Inserir novos registros
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            data_ref, ano, mes, nome_mes, trimestre,
            cod_assessor, nome_assessor, assessor_nivel, codigo_assessor_crm, assessor_status,
            nome_estrutura,
            captacao_bruta_xp, captacao_bruta_transferencia, captacao_bruta_total,
            resgate_bruto_xp, resgate_bruto_transferencia, resgate_bruto_total,
            captacao_liquida_xp, captacao_liquida_transferencia, captacao_liquida_total,
            qtd_clientes_aportando, qtd_clientes_resgatando,
            ticket_medio_aporte, ticket_medio_resgate,
            qtd_clientes_apenas_aportando, qtd_clientes_apenas_resgatando, qtd_clientes_aporte_e_resgate,
            data_carga, hash_registro
        )
        VALUES (
            origem.data_ref, origem.ano, origem.mes, origem.nome_mes, origem.trimestre,
            origem.cod_assessor, origem.nome_assessor, origem.assessor_nivel, 
            origem.codigo_assessor_crm, origem.assessor_status,
            origem.nome_estrutura,
            origem.captacao_bruta_xp, origem.captacao_bruta_transferencia, origem.captacao_bruta_total,
            origem.resgate_bruto_xp, origem.resgate_bruto_transferencia, origem.resgate_bruto_total,
            origem.captacao_liquida_xp, origem.captacao_liquida_transferencia, origem.captacao_liquida_total,
            origem.qtd_clientes_aportando, origem.qtd_clientes_resgatando,
            origem.ticket_medio_aporte, origem.ticket_medio_resgate,
            origem.qtd_clientes_apenas_aportando, origem.qtd_clientes_apenas_resgatando, 
            origem.qtd_clientes_aporte_e_resgate,
            GETDATE(),
            HASHBYTES('SHA2_256', 
                CAST(origem.data_ref AS VARCHAR(10)) + '|' +
                ISNULL(origem.cod_assessor, '') + '|' +
                CAST(ISNULL(origem.captacao_bruta_total, 0) AS VARCHAR(20)) + '|' +
                CAST(ISNULL(origem.resgate_bruto_total, 0) AS VARCHAR(20)) + '|' +
                CAST(ISNULL(origem.qtd_clientes_aportando, 0) AS VARCHAR(10))
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
        UPDATE STATISTICS [gold].[captacao_liquida_assessor];
        
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
-- GRANT EXECUTE ON [gold].[prc_gold_to_table_captacao_liquida_assessor] TO [role_etl_gold]
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
    MAX(data_ref) as data_fim
FROM [gold].[captacao_liquida_assessor]
GROUP BY data_carga
ORDER BY data_carga DESC;

-- Comparar view vs tabela
SELECT 
    'View' as origem,
    COUNT(*) as total_registros,
    SUM(captacao_liquida_total) as captacao_liquida_total
FROM [gold].[view_captacao_liquida_assessor]
UNION ALL
SELECT 
    'Tabela' as origem,
    COUNT(*) as total_registros,
    SUM(captacao_liquida_total) as captacao_liquida_total
FROM [gold].[captacao_liquida_assessor];

-- Verificar períodos sem dados
SELECT 
    c.ano,
    c.mes,
    COUNT(DISTINCT t.cod_assessor) as qtd_assessores
FROM [silver].[dim_calendario] c
LEFT JOIN [gold].[captacao_liquida_assessor] t
    ON c.ano = t.ano AND c.mes = t.mes
WHERE c.data_ref >= '2024-01-01'
    AND c.dia = 1
GROUP BY c.ano, c.mes
ORDER BY c.ano, c.mes;
*/

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor              | Descrição
--------|------------|--------------------|-----------------------------------------
1.0.0   | 2025-01-06 | Bruno Chiaramonti  | Criação inicial da procedure
1.1.0   | 2025-01-10 | Bruno Chiaramonti  | Removida limitação de 3 meses no modo INCREMENTAL
1.2.0   | 2025-01-16 | Bruno Chiaramonti  | Migração para schema gold e remoção de 'performance' do nome
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
- Horário: 06:00 AM (após cargas do Bronze/Silver)
- Frequência: Diária
- Timeout: 5 minutos
- Notificação: Em caso de falha

Troubleshooting comum:
1. Timeout: Aumentar período incremental ou executar em lotes menores
2. Deadlock: Verificar processos concorrentes nas tabelas Silver
3. Espaço em tempdb: Verificar espaço disponível antes da execução

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/