SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE     VIEW [silver].[vw_nps_respostas_envios_aniversario] AS

WITH 
-- CTE para pegar a data_carga mais recente de cada survey_id em respostas
RespostasLatest AS (
    SELECT 
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY r.survey_id 
            ORDER BY r.data_carga DESC
        ) as rn_resposta
    FROM bronze.xp_nps_respostas r
),

-- CTE para pegar a data_carga mais recente de cada survey_id em envios
EnviosLatest AS (
    SELECT 
        e.*,
        ROW_NUMBER() OVER (
            PARTITION BY e.survey_id 
            ORDER BY e.data_carga DESC
        ) as rn_envio
    FROM bronze.xp_nps_envios e
)

-- Query principal: Join entre as versões mais recentes
SELECT 
    -- Identificadores principais
    COALESCE(r.survey_id, e.survey_id) as survey_id,
    COALESCE(r.customer_id, e.customer_id) as customer_id,
    COALESCE(r.cod_assessor, e.cod_assessor) as cod_assessor,
    
    -- Datas
    e.data_entrega,
    COALESCE(r.data_resposta, e.data_resposta) as data_resposta,
    r.delivered_on_date,
    e.invitation_opened_date,
    e.survey_start_date,
    
    -- Status do envio
    e.survey_status,
    e.invitation_opened,
    
    -- Notas NPS (apenas as que possuem dados)
    r.xp_aniversario_nps_assessor,
    r.xp_aniversario_nps_xp,
    
    -- Indicador se recomendaria assessor
    r.xp_aniversario_recomendaria_assessor,
    
    -- Classificação NPS
    CASE 
        WHEN r.xp_aniversario_nps_assessor >= 9 THEN 'Promotor'
        WHEN r.xp_aniversario_nps_assessor >= 7 THEN 'Neutro'
        WHEN r.xp_aniversario_nps_assessor >= 0 THEN 'Detrator'
        ELSE NULL
    END AS classificacao_nps_assessor,
    
    CASE 
        WHEN r.xp_aniversario_nps_xp >= 9 THEN 'Promotor'
        WHEN r.xp_aniversario_nps_xp >= 7 THEN 'Neutro'
        WHEN r.xp_aniversario_nps_xp >= 0 THEN 'Detrator'
        ELSE NULL
    END AS classificacao_nps_xp,
    
    -- Comentários (apenas os que possuem dados)
    r.xp_aniversario_comentario_assessor,
    r.xp_aniversario_comentario_xp,
    
    -- Razões NPS (apenas as que possuem dados)
    r.xp_razao_nps,
    r.xp_aniversario_razao_nps_assessor,
    r.xp_aniversario_razao_nps_xp,
    
    -- Atendimento
    r.topics_tagged_original

FROM RespostasLatest r
FULL OUTER JOIN EnviosLatest e
    ON r.survey_id = e.survey_id
    AND r.rn_resposta = 1  -- Apenas a versão mais recente da resposta
    AND e.rn_envio = 1     -- Apenas a versão mais recente do envio

WHERE 
    -- Garante que pegamos apenas os registros mais recentes
    (r.rn_resposta = 1 OR r.rn_resposta IS NULL)
    AND (e.rn_envio = 1 OR e.rn_envio IS NULL);

GO
