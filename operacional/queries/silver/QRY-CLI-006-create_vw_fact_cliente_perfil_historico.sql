-- ==============================================================================
-- QRY-CLI-006-CREATE_VW_FACT_CLIENTE_PERFIL_HISTORICO
-- ==============================================================================
-- Tipo: View
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [view, clientes, perfil, histórico, share_of_wallet, silver]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: View que realiza cálculos complexos de perfil histórico de clientes,
           incluindo patrimônio, share of wallet, faixa etária, tempo de cliente
           e categorização. Une dados de múltiplas fontes bronze para criar
           uma visão consolidada mensal dos clientes.

Casos de uso:
- Base para tabela fact_cliente_perfil_historico
- Análise de evolução patrimonial dos clientes
- Cálculo de share of wallet entre XP e Open Investment
- Segmentação de clientes por perfil e comportamento
- Análise de safras (coortes) de clientes

Frequência de execução: Sob demanda (materializada mensalmente)
Tempo médio de execução: 3-5 minutos para consulta completa
Volume esperado de linhas: ~7M registros (histórico diário todos clientes)
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Não aplicável - View sem parâmetros

Para filtrar períodos específicos, usar WHERE na consulta:
SELECT * FROM silver.vw_fact_cliente_perfil_historico 
WHERE data_ref = EOMONTH(GETDATE(), -1) -- Último dia do mês anterior
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Colunas retornadas:

| Coluna                      | Tipo          | Descrição                                           | Exemplo           |
|-----------------------------|---------------|-----------------------------------------------------|-------------------|
| conta_xp_cliente            | INT           | Código único do cliente                             | 123456            |
| data_ref                    | DATE          | Data de referência do registro                      | 2024-12-31        |
| patrimonio_declarado        | DECIMAL(18,2) | Patrimônio em outras instituições                   | 500000.00         |
| patrimonio_xp               | DECIMAL(18,2) | Patrimônio na XP                                    | 300000.00         |
| patrimonio_open_investment  | DECIMAL(18,2) | Patrimônio no Open Investment                       | 200000.00         |
| share_of_wallet             | DECIMAL(5,4)  | % do patrimônio na XP (0-1)                        | 0.6000            |
| modelo_remuneracao          | VARCHAR(20)   | Fee Based ou Commission Based                       | Commission Based  |
| suitability                 | VARCHAR(50)   | Perfil de risco                                     | Moderado          |
| tipo_investidor             | VARCHAR(100)  | Classificação CVM                                   | Investidor Regular|
| segmento_cliente            | VARCHAR(50)   | Segmento (apenas PJ)                                | Corporate         |
| status_cliente              | VARCHAR(20)   | ATIVO ou INATIVO                                    | ATIVO             |
| cod_assessor                | VARCHAR(20)   | Código do assessor                                  | AAI123            |
| faixa_etaria                | VARCHAR(50)   | Classificação etária                                | 36-45             |
| meses_cliente_m7            | INT           | Tempo como cliente M7                               | 24                |
| safra_cliente_m7            | VARCHAR(6)    | Ano-mês de entrada (YYYYMM)                        | 202301            |

Ordenação padrão: Nenhuma (usar ORDER BY na consulta)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas de origem (bronze):
- [bronze].[xp_positivador]: Dados diários de patrimônio e status
  * Campos: cod_xp, data_ref, aplicacao_financeira_declarada, net_em_M, 
            status_cliente, cod_aai, data_nascimento, data_cadastro
  * Volume: ~7M registros
  * Granularidade: Um registro por cliente por dia
  
- [bronze].[xp_rpa_clientes]: Dados cadastrais e perfil
  * Campos: cod_xp, fee_based, suitability, tipo_investidor, segmento, cpf_cnpj
  * Volume: ~50.000 registros
  * Uso: Dados mais recentes por cliente (ROW_NUMBER)
  
- [bronze].[xp_open_investment_extrato]: Patrimônio Open Investment
  * Campos: cod_conta, valor_bruto
  * Volume: Variável
  * Uso: Soma por cliente

Funções/CTEs utilizadas:
- primeira_aparicao: Identifica entrada do cliente no positivador
- rpa_dados: Dados mais recentes do RPA por cliente
- patrimonio_open: Soma patrimônio Open Investment
- ROW_NUMBER(): Para pegar registro mais recente do RPA
- DATEDIFF/CEILING: Cálculo de meses como cliente
- FORMAT: Formatação de datas

Pré-requisitos:
- Tabelas bronze devem estar atualizadas
- Índices recomendados:
  * bronze.xp_positivador (cod_xp, data_ref)
  * bronze.xp_rpa_clientes (cod_xp, data_carga)
  * bronze.xp_open_investment_extrato (cod_conta)
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ==============================================================================
-- 6. CRIAÇÃO DA VIEW
-- ==============================================================================
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

-- ==============================================================================
-- 7. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================
/*
-- Query para verificar volume de dados
SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT conta_xp_cliente) as total_clientes,
    MIN(data_ref) as data_inicial,
    MAX(data_ref) as data_final
FROM silver.vw_fact_cliente_perfil_historico;

-- Query para verificar share of wallet médio
SELECT 
    CASE 
        WHEN share_of_wallet = 0 THEN '0%'
        WHEN share_of_wallet <= 0.25 THEN '1-25%'
        WHEN share_of_wallet <= 0.50 THEN '26-50%'
        WHEN share_of_wallet <= 0.75 THEN '51-75%'
        WHEN share_of_wallet <= 1.00 THEN '76-100%'
        ELSE 'Acima de 100%'
    END as faixa_share_wallet,
    COUNT(*) as quantidade,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentual
FROM silver.vw_fact_cliente_perfil_historico
WHERE share_of_wallet IS NOT NULL
    AND data_ref = EOMONTH(GETDATE(), -1)
GROUP BY 
    CASE 
        WHEN share_of_wallet = 0 THEN '0%'
        WHEN share_of_wallet <= 0.25 THEN '1-25%'
        WHEN share_of_wallet <= 0.50 THEN '26-50%'
        WHEN share_of_wallet <= 0.75 THEN '51-75%'
        WHEN share_of_wallet <= 1.00 THEN '76-100%'
        ELSE 'Acima de 100%'
    END
ORDER BY 
    CASE 
        WHEN share_of_wallet = 0 THEN 1
        WHEN share_of_wallet <= 0.25 THEN 2
        WHEN share_of_wallet <= 0.50 THEN 3
        WHEN share_of_wallet <= 0.75 THEN 4
        WHEN share_of_wallet <= 1.00 THEN 5
        ELSE 6
    END;

-- Query para análise de safras
SELECT 
    safra_cliente_m7,
    COUNT(DISTINCT conta_xp_cliente) as qtd_clientes,
    AVG(meses_cliente_m7) as media_meses_cliente,
    SUM(CASE WHEN status_cliente = 'ATIVO' THEN 1 ELSE 0 END) as clientes_ativos,
    ROUND(100.0 * SUM(CASE WHEN status_cliente = 'ATIVO' THEN 1 ELSE 0 END) / COUNT(*), 2) as taxa_retencao
FROM silver.vw_fact_cliente_perfil_historico
WHERE data_ref = EOMONTH(GETDATE(), -1)
GROUP BY safra_cliente_m7
ORDER BY safra_cliente_m7 DESC;
*/

-- ==============================================================================
-- 8. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-20 | [Nome]         | Criação inicial da view

*/

-- ==============================================================================
-- 9. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- View une dados de 3 tabelas bronze para criar visão consolidada
- Share of wallet limitado a 100% quando patrimônio XP > declarado
- Segmento apenas para PJ (identifica por CNPJ)
- Faixa etária calcula idade precisa considerando mês/dia
- Safra baseada na primeira aparição no positivador
- Para clientes do primeiro dia (2023-08-09), usa data_cadastro como safra
- Meses como cliente arredondados para cima (CEILING)
- Performance pode ser impactada pelo volume do positivador (~7M registros)

Regras de negócio:
1. Share of Wallet = patrimonio_xp / patrimonio_declarado
2. Se patrimônio declarado <= 0, share é NULL
3. Se patrimônio XP = 0, share é 0
4. Se patrimônio XP > declarado, share é 1 (100%)
5. Fee Based identificado por rpa.fee_based = 'ATIVO'
6. Segmento apenas para CNPJ (14 dígitos)
7. Status ATIVO quando status_cliente = 1 (bit)

Possíveis melhorias:
1. Criar índices sugeridos nas tabelas bronze
2. Materializar em tabela física para melhor performance
3. Adicionar filtros de data para reduzir volume processado
4. Considerar particionamento do positivador por data
5. Adicionar tratamento para casos especiais de share > 100%

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
