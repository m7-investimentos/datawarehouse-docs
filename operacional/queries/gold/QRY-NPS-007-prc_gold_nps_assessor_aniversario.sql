SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [gold].[prc_gold_nps_assessor_aniversario]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Truncate da tabela de destino
        TRUNCATE TABLE [gold].[nps_assessor_aniversario];
        
        -- Insert dos dados da view
        INSERT INTO [gold].[nps_assessor_aniversario] (
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
            qtd_pesquisas_enviadas,
            qtd_pesquisas_respondidas,
            qtd_convites_abertos,
            taxa_resposta,
            taxa_abertura,
            nps_score_assessor_mes,
            nps_score_assessor_trimestre,
            nps_score_assessor_semestre,
            nps_score_assessor_ano,
            nps_score_assessor_3_meses,
            nps_score_assessor_6_meses,
            nps_score_assessor_12_meses,
            qtd_promotores,
            qtd_neutros,
            qtd_detratores,
            qtd_recomendaria_sim,
            perc_recomendaria,
            razao_principal,
            data_carga
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
            qtd_pesquisas_enviadas,
            qtd_pesquisas_respondidas,
            qtd_convites_abertos,
            taxa_resposta,
            taxa_abertura,
            nps_score_assessor_mes,
            nps_score_assessor_trimestre,
            nps_score_assessor_semestre,
            nps_score_assessor_ano,
            nps_score_assessor_3_meses,
            nps_score_assessor_6_meses,
            nps_score_assessor_12_meses,
            qtd_promotores,
            qtd_neutros,
            qtd_detratores,
            qtd_recomendaria_sim,
            perc_recomendaria,
            razao_principal,
            CAST(GETDATE() AS DATE) AS data_carga -- Adiciona a data de carga como hoje
        FROM [gold].[vw_nps_assessor_aniversario];
        
        -- Log de sucesso
        PRINT 'Procedure executada com sucesso!';
        PRINT 'Registros inseridos: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
        
    END TRY
    BEGIN CATCH
        -- Tratamento de erro
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO
