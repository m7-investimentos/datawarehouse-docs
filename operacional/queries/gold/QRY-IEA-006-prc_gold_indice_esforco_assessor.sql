SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [gold].[prc_gold_indice_esforco_assessor]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Trunca a tabela gold
    TRUNCATE TABLE gold.indice_esforco_assessor;
    
    -- Insere os dados da view com todas as colunas explícitas
    INSERT INTO gold.indice_esforco_assessor (
        ano,
        ano_mes,
        mes,
        nome_mes,
        semestre,
        trimestre,
        cod_assessor,
        crm_id_assessor,
        nome_assessor,
        nivel_assessor,
        estrutura_id,
        estrutura_nome,
        esforco_prospeccao,
        esforco_relacionamento,
        indice_esforco_assessor,
        indice_esforco_assessor_3_meses,
        indice_esforco_assessor_6_meses,
        indice_esforco_assessor_12_meses,
        indice_esforco_assessor_ano,
        indice_esforco_assessor_semestre,
        indice_esforco_assessor_trimestre,
        prospeccao_atingimento_carteiras_simuladas_novos,
        prospeccao_atingimento_conversao,
        prospeccao_atingimento_habilitacoes,
        prospeccao_atingimento_lead_starts,
        prospeccao_captacao_de_novos_clientes_por_aai,
        relacionamento_atingimento_contas_acessadas_hub,
        relacionamento_atingimento_contas_aportarem,
        relacionamento_atingimento_ordens_enviadas,
        relacionamento_captacao_da_base
    )
    SELECT 
        ano,
        ano_mes,
        mes,
        nome_mes,
        semestre,
        trimestre,
        cod_assessor,
        crm_id_assessor,
        nome_assessor,
        nivel_assessor,
        estrutura_id,
        estrutura_nome,
        esforco_prospeccao,
        esforco_relacionamento,
        indice_esforco_assessor,
        indice_esforco_assessor_3_meses,
        indice_esforco_assessor_6_meses,
        indice_esforco_assessor_12_meses,
        indice_esforco_assessor_ano,
        indice_esforco_assessor_semestre,
        indice_esforco_assessor_trimestre,
        prospeccao_atingimento_carteiras_simuladas_novos,
        prospeccao_atingimento_conversao,
        prospeccao_atingimento_habilitacoes,
        prospeccao_atingimento_lead_starts,
        prospeccao_captacao_de_novos_clientes_por_aai,
        relacionamento_atingimento_contas_acessadas_hub,
        relacionamento_atingimento_contas_aportarem,
        relacionamento_atingimento_ordens_enviadas,
        relacionamento_captacao_da_base
    FROM gold.vw_indice_esforco_assessor;
    
END
GO
