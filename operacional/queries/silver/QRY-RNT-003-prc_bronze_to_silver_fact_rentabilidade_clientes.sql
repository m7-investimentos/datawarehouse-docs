-- ==============================================================================
-- QRY-RNT-003-prc_bronze_to_silver_fact_rentabilidade_clientes
-- ==============================================================================
-- Tipo: PROCEDURE
-- Versão: 1.0.0
-- Última atualização: 2024-11-28
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [etl, bronze-to-silver, rentabilidade, performance]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por processar e carregar dados de rentabilidade
da camada bronze para a camada silver. Utiliza uma view intermediária que já 
realiza todos os cálculos complexos de rentabilidade acumulada. Implementa 
mecanismo de preservação de dados históricos que não existem mais na fonte.

Casos de uso:
- Carga diária de dados de rentabilidade processados
- Preservação automática de dados históricos
- Cálculo de rentabilidades acumuladas via view
- Atualização completa com tratamento transacional
- Logging detalhado para auditoria

Frequência de execução: Diária (preferencialmente após 7h da manhã)
Tempo médio de execução: 30-60 segundos
Volume esperado de linhas: ~50.000 registros ativos + histórico
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Nenhum parâmetro de entrada - Procedure processa todos os dados disponíveis
na view vw_fact_rentabilidade_clientes realizando carga completa com preservação
de histórico
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela de destino: [silver].[fact_rentabilidade_clientes]

Fluxo de processamento:
1. Identifica e preserva dados históricos em tabela temporária
2. Trunca tabela silver (limpeza completa)
3. Carrega dados atuais da view com cálculos já processados
4. Restaura dados históricos preservados
5. Registra estatísticas e logs de execução

Dados preservados:
- Registros de períodos (ano_mes) que não existem mais na view
- Mantém data_carga original para rastreabilidade
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- silver.vw_fact_rentabilidade_clientes: View com cálculos de rentabilidade
- silver.fact_rentabilidade_clientes: Tabela de destino e fonte de histórico
- #temp_historico: Tabela temporária para preservação de dados

Funções/Procedures chamadas:
- Nenhuma

Pré-requisitos:
- View vw_fact_rentabilidade_clientes deve estar atualizada
- Tabela silver.fact_rentabilidade_clientes deve existir
- Permissões de TRUNCATE e INSERT na tabela de destino
- TempDB com espaço suficiente para tabela temporária
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ==============================================================================
-- 6. CRIAÇÃO DA PROCEDURE
-- ==============================================================================
CREATE   PROCEDURE [silver].[prc_bronze_to_silver_fact_rentabilidade_clientes]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- ==============================================================================
    -- 7. TRATAMENTO DE ERROS E TRANSAÇÕES
    -- ==============================================================================
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- ==============================================================================
        -- 8. VARIÁVEIS DE CONTROLE
        -- ==============================================================================
        -- Declaração de variáveis para controle
        DECLARE @RowsPreserved INT = 0;
        DECLARE @RowsInserted INT = 0;
        DECLARE @StartTime DATETIME = GETDATE();
        
        -- ==============================================================================
        -- 9. LÓGICA DE PROCESSAMENTO
        -- ==============================================================================
        
        -- 9.1. Criar tabela temporária para preservar dados históricos
        -- que não existem mais na view (dados de anos anteriores que saíram do relatório)
        DROP TABLE IF EXISTS #temp_historico;
        
        SELECT *
        INTO #temp_historico
        FROM [silver].[fact_rentabilidade_clientes]
        WHERE CAST(ano_mes AS VARCHAR(6)) NOT IN (
            SELECT DISTINCT CAST(ano * 100 + mes_num AS VARCHAR(6))
            FROM [silver].[vw_fact_rentabilidade_clientes]
        );
        
        SET @RowsPreserved = @@ROWCOUNT;
        
        -- Log da preservação
        PRINT 'Dados históricos preservados: ' + CAST(@RowsPreserved AS VARCHAR(10)) + ' registros';
        
        -- 2. Limpar a tabela silver
        TRUNCATE TABLE [silver].[fact_rentabilidade_clientes];
        
        -- 3. Inserir dados atualizados da VIEW
        INSERT INTO [silver].[fact_rentabilidade_clientes] (
            conta_xp_cliente,
            ano_mes,
            ano,
            semestre,
            trimestre,
            mes_num,
            mes,
            rentabilidade,
            rentabilidade_acumulada_3_meses,
            rentabilidade_acumulada_6_meses,
            rentabilidade_acumulada_12_meses,
            rentabilidade_acumulada_trimestre,
            rentabilidade_acumulada_semestre,
            rentabilidade_acumulada_ano,
            data_carga
        )
        SELECT 
            conta_xp_cliente,
            ano_mes,
            ano,
            semestre,
            trimestre,
            mes_num,
            mes,
            rentabilidade,
            rentabilidade_acumulada_3_meses,
            rentabilidade_acumulada_6_meses,
            rentabilidade_acumulada_12_meses,
            rentabilidade_acumulada_trimestre,
            rentabilidade_acumulada_semestre,
            rentabilidade_acumulada_ano,
            GETDATE() as data_carga
        FROM [silver].[vw_fact_rentabilidade_clientes];
        
        SET @RowsInserted = @@ROWCOUNT;
        
        -- Log da inserção principal
        PRINT 'Dados da VIEW inseridos: ' + CAST(@RowsInserted AS VARCHAR(10)) + ' registros';
        
        -- 4. Reinserir dados históricos preservados
        IF @RowsPreserved > 0
        BEGIN
            INSERT INTO [silver].[fact_rentabilidade_clientes] (
                conta_xp_cliente,
                ano_mes,
                ano,
                semestre,
                trimestre,
                mes_num,
                mes,
                rentabilidade,
                rentabilidade_acumulada_3_meses,
                rentabilidade_acumulada_6_meses,
                rentabilidade_acumulada_12_meses,
                rentabilidade_acumulada_trimestre,
                rentabilidade_acumulada_semestre,
                rentabilidade_acumulada_ano,
                data_carga
            )
            SELECT 
                conta_xp_cliente,
                ano_mes,
                ano,
                semestre,
                trimestre,
                mes_num,
                mes,
                rentabilidade,
                rentabilidade_acumulada_3_meses,
                rentabilidade_acumulada_6_meses,
                rentabilidade_acumulada_12_meses,
                rentabilidade_acumulada_trimestre,
                rentabilidade_acumulada_semestre,
                rentabilidade_acumulada_ano,
                data_carga -- Mantém a data_carga original dos dados históricos
            FROM #temp_historico;
            
            PRINT 'Dados históricos reinseridos: ' + CAST(@RowsPreserved AS VARCHAR(10)) + ' registros';
        END
        
        -- 5. Estatísticas finais
        DECLARE @TotalRows INT;
        SELECT @TotalRows = COUNT(*) FROM [silver].[fact_rentabilidade_clientes];
        
        PRINT '----------------------------------------';
        PRINT 'PROCESSO CONCLUÍDO COM SUCESSO';
        PRINT 'Total de registros na tabela: ' + CAST(@TotalRows AS VARCHAR(10));
        PRINT 'Tempo de execução: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR(10)) + ' segundos';
        PRINT '----------------------------------------';
        
        -- Limpar tabela temporária
        DROP TABLE IF EXISTS #temp_historico;
        
        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        -- Em caso de erro, fazer rollback
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Registrar erro
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        PRINT '----------------------------------------';
        PRINT 'ERRO NO PROCESSO';
        PRINT 'Mensagem: ' + @ErrorMessage;
        PRINT 'Severidade: ' + CAST(@ErrorSeverity AS VARCHAR(10));
        PRINT 'Estado: ' + CAST(@ErrorState AS VARCHAR(10));
        PRINT '----------------------------------------';
        
        -- Re-lançar o erro
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
    
END
GO
