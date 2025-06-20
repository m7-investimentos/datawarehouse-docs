SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [silver].[prc_load_fact_cliente_perfil_historico]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Limpa a tabela
    TRUNCATE TABLE [silver].[fact_cliente_perfil_historico];
    
    -- Insere todos os dados da view
    INSERT INTO [silver].[fact_cliente_perfil_historico] (
        conta_xp_cliente,
        data_ref,
        patrimonio_declarado,
        patrimonio_xp,
        patrimonio_open_investment,
        share_of_wallet,
        modelo_remuneracao,
        suitability,
        tipo_investidor,
        segmento_cliente,
        status_cliente,
        faixa_etaria,
        cod_assessor,
        meses_cliente_m7,
        safra_cliente_m7
    )
    SELECT 
        conta_xp_cliente,
        data_ref,
        patrimonio_declarado,
        patrimonio_xp,
        patrimonio_open_investment,
        share_of_wallet,
        modelo_remuneracao,
        suitability,
        tipo_investidor,
        segmento_cliente,
        status_cliente,
        faixa_etaria,
        cod_assessor,
        meses_cliente_m7,
        safra_cliente_m7
    FROM [silver].[vw_fact_cliente_perfil_historico];
    
END
GO
