SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Procedure: sp_carga_fact_nps_respostas_envios_aniversario
-- Descrição: Carrega dados da view para a tabela fato de NPS
-- Autor: Sistema M7 Soluções Financeiras
-- Data: 2025-06-17
-- =============================================

CREATE   PROCEDURE [silver].[prc_bronze_to_silver_fact_nps_respostas_envios_aniversario]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Limpa a tabela antes da carga
    TRUNCATE TABLE [silver].[fact_nps_respostas_envios_aniversario];
    
    -- Insere dados da view para a tabela fato
    INSERT INTO [silver].[fact_nps_respostas_envios_aniversario]
    (
        [survey_id],
        [conta_xp_cliente],
        [cod_assessor],
        [data_entrega],
        [data_resposta],
        [data_inicio_survey],
        [survey_status],
        [convite_aberto],
        [nps_assessor],
        [nps_xp],
        [recomendaria_assessor],
        [classificacao_nps_assessor],
        [classificacao_nps_xp],
        [comentario_assessor],
        [comentario_xp],
        [razao_nps],
        [razao_nps_assessor],
        [razao_nps_xp],
        [topicos_relevantes]
    )
    SELECT 
        [survey_id],
        [customer_id] AS [conta_xp_cliente],
        [cod_assessor],
        [data_entrega],
        [data_resposta],
        [survey_start_date] AS [data_inicio_survey],
        [survey_status],
        [invitation_opened] AS [convite_aberto],
        [xp_aniversario_nps_assessor] AS [nps_assessor],
        [xp_aniversario_nps_xp] AS [nps_xp],
        [xp_aniversario_recomendaria_assessor] AS [recomendaria_assessor],
        [classificacao_nps_assessor],
        [classificacao_nps_xp],
        [xp_aniversario_comentario_assessor] AS [comentario_assessor],
        [xp_aniversario_comentario_xp] AS [comentario_xp],
        [xp_razao_nps] AS [razao_nps],
        [xp_aniversario_razao_nps_assessor] AS [razao_nps_assessor],
        [xp_aniversario_razao_nps_xp] AS [razao_nps_xp],
        [topics_tagged_original] AS [topicos_relevantes]
    FROM [silver].[vw_nps_respostas_envios_aniversario];
    
END;
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Procedure para carga completa (truncate/insert) da tabela fact_nps_respostas_envios_aniversario a partir da view vw_nps_respostas_envios_aniversario' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'PROCEDURE',@level1name=N'prc_bronze_to_silver_fact_nps_respostas_envios_aniversario'
GO
