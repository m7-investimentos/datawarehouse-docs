SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [silver].[prc_bronze_to_silver_fact_ativacoes_habilitacoes_evasoes]
AS
BEGIN
    SET NOCOUNT ON;

    -- Elimina todos os registros anteriores para garantir consistência
    TRUNCATE TABLE [silver].[fact_ativacoes_habilitacoes_evasoes];

    -- Insere os dados atualizados diretamente da stage
    INSERT INTO [silver].[fact_ativacoes_habilitacoes_evasoes] (
        data_ref,
        cod_xp,
        crm_id,
        id_estrutura,
        faixa_pl,
        tipo_movimentacao
    )
    SELECT 
        s.data_ref,
        s.cod_xp,
        d.crm_id,
        f.id_estrutura,
        s.faixa_pl,
        s.tipo_movimentacao
    FROM [bronze].[xp_ativacoes_habilitacoes_evasoes] s
    LEFT JOIN [silver].[dim_pessoas] d
        ON s.cod_aai = d.cod_aai
    LEFT JOIN [silver].[fact_estrutura_pessoas] f
        ON d.crm_id = f.crm_id
        AND s.data_ref >= f.data_entrada
        AND (f.data_saida IS NULL OR s.data_ref <= f.data_saida)
    WHERE d.crm_id IS NOT NULL;
END;
GO
