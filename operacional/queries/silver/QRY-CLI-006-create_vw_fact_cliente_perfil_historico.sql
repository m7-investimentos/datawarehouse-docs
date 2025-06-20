SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   VIEW [silver].[vw_fact_cliente_perfil_historico] AS

WITH 
-- CTE para pegar a primeira aparição do cliente no positivador
primeira_aparicao AS (
    SELECT 
        pos.cod_xp,
        MIN(pos.data_ref) AS primeira_data_ref,
        -- Se o cliente estava presente no primeiro dia (2023-08-09), pega a data_cadastro
        CASE 
            WHEN MIN(pos.data_ref) = '2023-08-09' THEN MAX(pos.data_cadastro)
            ELSE MIN(pos.data_ref)
        END AS data_base_safra
    FROM bronze.xp_positivador pos
    GROUP BY pos.cod_xp
),

-- CTE para pegar dados mais recentes do RPA por cliente e data
rpa_dados AS (
    SELECT 
        rpa.cod_xp,
        rpa.fee_based,
        rpa.suitability,
        rpa.tipo_investidor,
        rpa.segmento,
        rpa.cpf_cnpj,
        rpa.data_carga,
        ROW_NUMBER() OVER (PARTITION BY rpa.cod_xp ORDER BY rpa.data_carga DESC) AS rn
    FROM bronze.xp_rpa_clientes rpa
),

-- CTE para somar patrimônio Open Investment
patrimonio_open AS (
    SELECT 
        cod_conta,
        SUM(valor_bruto) AS total_patrimonio_open
    FROM bronze.xp_open_investment_extrato
    GROUP BY cod_conta
)

-- Query principal
SELECT 
    -- Identificação
    pos.cod_xp AS conta_xp_cliente,
    pos.data_ref,
    
    -- Patrimônio
    pos.aplicacao_financeira_declarada AS patrimonio_declarado,
    pos.net_em_M AS patrimonio_xp,
    ISNULL(po.total_patrimonio_open, 0) AS patrimonio_open_investment,
    
    -- Share of Wallet (proporção decimal com 4 casas)
    CASE 
        WHEN ISNULL(pos.aplicacao_financeira_declarada, 0) <= 0 THEN NULL
        WHEN ISNULL(pos.net_em_M, 0) <= 0 THEN 0
        WHEN pos.net_em_M > pos.aplicacao_financeira_declarada THEN 1.0000 -- Limita a 100% = 1
        ELSE 
            CAST(
                CAST(pos.net_em_M AS FLOAT) / 
                CAST(pos.aplicacao_financeira_declarada AS FLOAT)
                AS DECIMAL(5,4)
            )
    END AS share_of_wallet,
    
    -- Modelo de remuneração (da tabela RPA)
    CASE 
        WHEN UPPER(ISNULL(rpa.fee_based, 'NAO')) = 'ATIVO' THEN 'Fee Based'
        ELSE 'Commission Based'
    END AS modelo_remuneracao,
    
    -- Suitability (da tabela RPA)
    rpa.suitability,
    
    -- Tipo de investidor (da tabela RPA)
    rpa.tipo_investidor,
    
    -- Segmento cliente (só para PJ, PF fica NULL)
    CASE 
        WHEN rpa.cpf_cnpj LIKE '%/%' THEN rpa.segmento -- CNPJ tem barra
        WHEN LEN(REPLACE(REPLACE(REPLACE(rpa.cpf_cnpj, '.', ''), '-', ''), '/', '')) = 14 THEN rpa.segmento -- CNPJ sem formatação
        ELSE NULL -- CPF ou não informado
    END AS segmento_cliente,
    
    -- Status (convertendo bit para varchar)
    CASE 
        WHEN pos.status_cliente = 1 THEN 'ATIVO'
        ELSE 'INATIVO'
    END AS status_cliente,
    
    -- Assessor
    pos.cod_aai AS cod_assessor,
    
    -- Faixa etária
    CASE 
        WHEN pos.data_nascimento IS NULL THEN NULL
        WHEN DATEDIFF(YEAR, pos.data_nascimento, pos.data_ref) - 
             CASE 
                WHEN MONTH(pos.data_nascimento) > MONTH(pos.data_ref) OR 
                     (MONTH(pos.data_nascimento) = MONTH(pos.data_ref) AND 
                      DAY(pos.data_nascimento) > DAY(pos.data_ref))
                THEN 1 
                ELSE 0 
             END < 18 THEN 'Menor de 18'
        WHEN DATEDIFF(YEAR, pos.data_nascimento, pos.data_ref) - 
             CASE 
                WHEN MONTH(pos.data_nascimento) > MONTH(pos.data_ref) OR 
                     (MONTH(pos.data_nascimento) = MONTH(pos.data_ref) AND 
                      DAY(pos.data_nascimento) > DAY(pos.data_ref))
                THEN 1 
                ELSE 0 
             END BETWEEN 18 AND 25 THEN '18-25'
        WHEN DATEDIFF(YEAR, pos.data_nascimento, pos.data_ref) - 
             CASE 
                WHEN MONTH(pos.data_nascimento) > MONTH(pos.data_ref) OR 
                     (MONTH(pos.data_nascimento) = MONTH(pos.data_ref) AND 
                      DAY(pos.data_nascimento) > DAY(pos.data_ref))
                THEN 1 
                ELSE 0 
             END BETWEEN 26 AND 35 THEN '26-35'
        WHEN DATEDIFF(YEAR, pos.data_nascimento, pos.data_ref) - 
             CASE 
                WHEN MONTH(pos.data_nascimento) > MONTH(pos.data_ref) OR 
                     (MONTH(pos.data_nascimento) = MONTH(pos.data_ref) AND 
                      DAY(pos.data_nascimento) > DAY(pos.data_ref))
                THEN 1 
                ELSE 0 
             END BETWEEN 36 AND 45 THEN '36-45'
        WHEN DATEDIFF(YEAR, pos.data_nascimento, pos.data_ref) - 
             CASE 
                WHEN MONTH(pos.data_nascimento) > MONTH(pos.data_ref) OR 
                     (MONTH(pos.data_nascimento) = MONTH(pos.data_ref) AND 
                      DAY(pos.data_nascimento) > DAY(pos.data_ref))
                THEN 1 
                ELSE 0 
             END BETWEEN 46 AND 55 THEN '46-55'
        WHEN DATEDIFF(YEAR, pos.data_nascimento, pos.data_ref) - 
             CASE 
                WHEN MONTH(pos.data_nascimento) > MONTH(pos.data_ref) OR 
                     (MONTH(pos.data_nascimento) = MONTH(pos.data_ref) AND 
                      DAY(pos.data_nascimento) > DAY(pos.data_ref))
                THEN 1 
                ELSE 0 
             END BETWEEN 56 AND 65 THEN '56-65'
        ELSE 'Acima de 65'
    END AS faixa_etaria,
    
    -- Meses cliente M7 (arredondado para cima)
    CAST(
        CEILING(
            CAST(DATEDIFF(DAY, pa.data_base_safra, pos.data_ref) AS FLOAT) / 30.0
        ) AS INT
    ) AS meses_cliente_m7,
    
    -- Safra cliente M7 (formato YYYYMM)
    FORMAT(pa.data_base_safra, 'yyyyMM') AS safra_cliente_m7

FROM bronze.xp_positivador pos

-- Join com primeira aparição
INNER JOIN primeira_aparicao pa 
    ON pos.cod_xp = pa.cod_xp

-- Left join com RPA para pegar informações complementares
LEFT JOIN rpa_dados rpa
    ON pos.cod_xp = rpa.cod_xp
    AND rpa.rn = 1

-- Left join com patrimônio Open Investment
LEFT JOIN patrimonio_open po
    ON pos.cod_xp = po.cod_conta
GO
