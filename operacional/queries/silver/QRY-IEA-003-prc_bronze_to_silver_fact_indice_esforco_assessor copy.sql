SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [silver].[prc_bronze_to_silver_fact_indice_esforco_assessor]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Limpa toda a tabela
    TRUNCATE TABLE silver.fact_indice_esforco_assessor;
    
    -- Insere todos os dados da view
    INSERT INTO silver.fact_indice_esforco_assessor (
        ano_mes,
        cod_assessor,
        indice_esforco_assessor,
        indice_esforco_assessor_acum_3_meses,
        indice_esforco_assessor_acum_6_meses,
        indice_esforco_assessor_acum_12_meses,
        esforco_prospeccao,
        esforco_relacionamento,
        prospeccao_captacao_de_novos_clientes_por_aai,
        prospeccao_atingimento_lead_starts,
        prospeccao_atingimento_habilitacoes,
        prospeccao_atingimento_conversao,
        prospeccao_atingimento_carteiras_simuladas_novos,
        relacionamento_captacao_da_base,
        relacionamento_atingimento_contas_aportarem,
        relacionamento_atingimento_ordens_enviadas,
        relacionamento_atingimento_contas_acessadas_hub
    )
    SELECT 
        ano_mes,
        cod_assessor,
        indice_esforco_assessor,
        indice_esforco_assessor_acum_3_meses,
        indice_esforco_assessor_acum_6_meses,
        indice_esforco_assessor_acum_12_meses,
        esforco_prospeccao,
        esforco_relacionamento,
        prospeccao_captacao_de_novos_clientes_por_aai,
        prospeccao_atingimento_lead_starts,
        prospeccao_atingimento_habilitacoes,
        prospeccao_atingimento_conversao,
        prospeccao_atingimento_carteiras_simuladas_novos,
        relacionamento_captacao_da_base,
        relacionamento_atingimento_contas_aportarem,
        relacionamento_atingimento_ordens_enviadas,
        relacionamento_atingimento_contas_acessadas_hub
    FROM silver.vw_fact_indice_esforco_assessor;
    
    -- Retorna quantidade de registros inseridos
    SELECT @@ROWCOUNT AS registros_inseridos;
END;
GO
