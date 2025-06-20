SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [silver].[prc_bronze_to_silver_fact_rentabilidade_clientes]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Declaração de variáveis para controle
        DECLARE @RowsPreserved INT = 0;
        DECLARE @RowsInserted INT = 0;
        DECLARE @StartTime DATETIME = GETDATE();
        
        -- 1. Criar tabela temporária para preservar dados históricos
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
