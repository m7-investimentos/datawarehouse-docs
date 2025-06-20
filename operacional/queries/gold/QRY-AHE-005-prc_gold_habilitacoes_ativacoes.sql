SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [gold].[prc_gold_habilitacoes_ativacoes]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Inicia transação
        BEGIN TRANSACTION;
        
        -- Trunca a tabela para limpar dados antigos
        TRUNCATE TABLE gold.habilitacoes_ativacoes;
        
        -- Insere dados atualizados da view
        INSERT INTO gold.habilitacoes_ativacoes (
            ano_mes,
            ano,
            mes,
            nome_mes,
            trimestre,
            semestre,
            cod_assessor,
            crm_id_assessor,
            nome_assessor,
            nivel_assessor,
            estrutura_id,
            estrutura_nome,
            qtd_ativacoes_300k_mais,
            qtd_ativacoes_300k_menos,
            qtd_ativacoes_300k_mais_trimestre,
            qtd_ativacoes_300k_menos_trimestre,
            qtd_ativacoes_300k_mais_semestre,
            qtd_ativacoes_300k_menos_semestre,
            qtd_ativacoes_300k_mais_ano,
            qtd_ativacoes_300k_menos_ano,
            qtd_ativacoes_300k_mais_3_meses,
            qtd_ativacoes_300k_menos_3_meses,
            qtd_ativacoes_300k_mais_6_meses,
            qtd_ativacoes_300k_menos_6_meses,
            qtd_ativacoes_300k_mais_12_meses,
            qtd_ativacoes_300k_menos_12_meses,
            qtd_habilitacoes_300k_mais,
            qtd_habilitacoes_300k_menos,
            qtd_habilitacoes_300k_mais_trimestre,
            qtd_habilitacoes_300k_menos_trimestre,
            qtd_habilitacoes_300k_mais_semestre,
            qtd_habilitacoes_300k_menos_semestre,
            qtd_habilitacoes_300k_mais_ano,
            qtd_habilitacoes_300k_menos_ano,
            qtd_habilitacoes_300k_mais_3_meses,
            qtd_habilitacoes_300k_menos_3_meses,
            qtd_habilitacoes_300k_mais_6_meses,
            qtd_habilitacoes_300k_menos_6_meses,
            qtd_habilitacoes_300k_mais_12_meses,
            qtd_habilitacoes_300k_menos_12_meses
        )
        SELECT 
            ano_mes,
            ano,
            mes,
            nome_mes,
            trimestre,
            semestre,
            cod_assessor,
            crm_id_assessor,
            nome_assessor,
            nivel_assessor,
            estrutura_id,
            estrutura_nome,
            qtd_ativacoes_300k_mais,
            qtd_ativacoes_300k_menos,
            qtd_ativacoes_300k_mais_trimestre,
            qtd_ativacoes_300k_menos_trimestre,
            qtd_ativacoes_300k_mais_semestre,
            qtd_ativacoes_300k_menos_semestre,
            qtd_ativacoes_300k_mais_ano,
            qtd_ativacoes_300k_menos_ano,
            qtd_ativacoes_300k_mais_3_meses,
            qtd_ativacoes_300k_menos_3_meses,
            qtd_ativacoes_300k_mais_6_meses,
            qtd_ativacoes_300k_menos_6_meses,
            qtd_ativacoes_300k_mais_12_meses,
            qtd_ativacoes_300k_menos_12_meses,
            qtd_habilitacoes_300k_mais,
            qtd_habilitacoes_300k_menos,
            qtd_habilitacoes_300k_mais_trimestre,
            qtd_habilitacoes_300k_menos_trimestre,
            qtd_habilitacoes_300k_mais_semestre,
            qtd_habilitacoes_300k_menos_semestre,
            qtd_habilitacoes_300k_mais_ano,
            qtd_habilitacoes_300k_menos_ano,
            qtd_habilitacoes_300k_mais_3_meses,
            qtd_habilitacoes_300k_menos_3_meses,
            qtd_habilitacoes_300k_mais_6_meses,
            qtd_habilitacoes_300k_menos_6_meses,
            qtd_habilitacoes_300k_mais_12_meses,
            qtd_habilitacoes_300k_menos_12_meses
        FROM gold.vw_habilitacoes_ativacoes;
        
        -- Conta registros inseridos
        DECLARE @RowCount INT = @@ROWCOUNT;
        
        -- Confirma transação
        COMMIT TRANSACTION;
        
        -- Retorna mensagem de sucesso
        PRINT 'Procedure executada com sucesso!';
        PRINT 'Total de registros inseridos: ' + CAST(@RowCount AS VARCHAR(10));
        
    END TRY
    BEGIN CATCH
        -- Em caso de erro, desfaz a transação
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Captura informações do erro
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        -- Relança o erro
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO
