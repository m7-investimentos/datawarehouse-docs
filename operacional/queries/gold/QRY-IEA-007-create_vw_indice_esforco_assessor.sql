-- ==============================================================================
-- QRY-IEA-007-create_vw_indice_esforco_assessor
-- ==============================================================================
-- Tipo: CREATE VIEW
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [equipe.dados@m7investimentos.com.br]
-- Revisor: [revisor@m7investimentos.com.br]
-- Tags: [indice_esforco, assessor, consolidacao, gold]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: View que consolida o Índice de Esforço do Assessor (IEA) a partir dos
dados da silver, garantindo que apenas o registro mais recente por assessor/mês
seja considerado. Calcula médias acumuladas para diferentes períodos.

Casos de uso:
- Base para tabela materializada gold.indice_esforco_assessor
- Consultas ad-hoc para análise de tendências do IEA
- Validação de cálculos antes da materialização
- Fonte para dashboards em tempo real de performance

Frequência de consulta: Várias vezes ao dia
Tempo médio de execução: 30-60 segundos
Volume de dados: ~24.000 registros
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Não aplicável - View processa todos os dados disponíveis
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
| Coluna                                        | Tipo          | Descrição                                     |
|-----------------------------------------------|---------------|-----------------------------------------------|
| ano                                           | INT           | Ano de referência                            |
| ano_mes                                       | INT           | Período no formato AAAAMM                    |
| mes                                           | INT           | Mês de referência (1-12)                    |
| nome_mes                                      | VARCHAR(20)   | Nome do mês em inglês                       |
| semestre                                      | VARCHAR(2)    | Semestre (S1-S2)                              |
| trimestre                                     | VARCHAR(2)    | Trimestre (Q1-Q4)                             |
| cod_assessor                                  | VARCHAR(20)   | Código do assessor                           |
| crm_id_assessor                               | VARCHAR(20)   | ID do assessor no CRM                        |
| nome_assessor                                 | VARCHAR(200)  | Nome completo do assessor                    |
| nivel_assessor                                | VARCHAR(50)   | Nível hierárquico                            |
| estrutura_id                                  | INT           | ID da estrutura organizacional               |
| estrutura_nome                                | VARCHAR(100)  | Nome da estrutura                            |
| esforco_prospeccao                            | DECIMAL(18,8) | Índice de esforço em prospecção              |
| esforco_relacionamento                        | DECIMAL(18,8) | Índice de esforço em relacionamento          |
| indice_esforco_assessor                       | DECIMAL(18,8) | IEA do mês                                   |
| indice_esforco_assessor_[periodo]             | DECIMAL(18,8) | IEA médio acumulado por período             |
| [métricas de prospecção e relacionamento]     | Vários        | Indicadores detalhados                       |
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- silver.fact_indice_esforco_assessor: Fatos do IEA com histórico de cargas
- silver.dim_pessoas: Cadastro de pessoas (assessores)
- silver.fact_estrutura_pessoas: Histórico de alocação em estruturas
- silver.dim_estruturas: Cadastro de estruturas organizacionais
- silver.dim_calendario: Dimensão de datas

Pré-requisitos:
- Dados atualizados nas tabelas silver
- Índices adequados para performance dos JOINs
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ==============================================================================
-- 6. CRIAÇÃO DA VIEW COM CTEs
-- ==============================================================================
-- View para a tabela gold.indice_esforco_assessor
-- Esta view busca dados da silver e calcula corretamente os acumulados
-- Considera apenas o registro mais recente por assessor/mês baseado na data_carga

CREATE   VIEW [gold].[vw_indice_esforco_assessor] AS
WITH 
-- CTE para pegar apenas os registros mais recentes por assessor/mês
registros_mais_recentes AS (
    SELECT 
        cod_assessor,
        ano_mes,
        esforco_prospeccao,
        esforco_relacionamento,
        indice_esforco_assessor,
        indice_esforco_assessor_acum_3_meses,
        indice_esforco_assessor_acum_6_meses,
        indice_esforco_assessor_acum_12_meses,
        prospeccao_atingimento_carteiras_simuladas_novos,
        prospeccao_atingimento_conversao,
        prospeccao_atingimento_habilitacoes,
        prospeccao_atingimento_lead_starts,
        prospeccao_captacao_de_novos_clientes_por_aai,
        relacionamento_atingimento_contas_acessadas_hub,
        relacionamento_atingimento_contas_aportarem,
        relacionamento_atingimento_ordens_enviadas,
        relacionamento_captacao_da_base,
        ROW_NUMBER() OVER (
            PARTITION BY cod_assessor, ano_mes 
            ORDER BY data_carga DESC
        ) as rn
    FROM silver.fact_indice_esforco_assessor
),

-- CTE base com apenas registros mais recentes
base_dados AS (
    SELECT * FROM registros_mais_recentes WHERE rn = 1
),

-- CTE para calcular média acumulada ANUAL (sempre desde janeiro)
metricas_anuais AS (
    SELECT 
        f.cod_assessor,
        f.ano_mes,
        AVG(f2.indice_esforco_assessor) as indice_esforco_assessor_ano
    FROM base_dados f
    INNER JOIN base_dados f2
        ON f.cod_assessor = f2.cod_assessor
        AND LEFT(f.ano_mes, 4) = LEFT(f2.ano_mes, 4)  -- Mesmo ano
        AND f2.ano_mes <= f.ano_mes  -- Desde janeiro até o mês atual
    GROUP BY f.cod_assessor, f.ano_mes
),

-- CTE para calcular média acumulada SEMESTRAL (reinicia em janeiro e julho)
metricas_semestrais AS (
    SELECT 
        f.cod_assessor,
        f.ano_mes,
        AVG(f2.indice_esforco_assessor) as indice_esforco_assessor_semestre
    FROM base_dados f
    INNER JOIN base_dados f2
        ON f.cod_assessor = f2.cod_assessor
        AND LEFT(f.ano_mes, 4) = LEFT(f2.ano_mes, 4)  -- Mesmo ano
        AND f2.ano_mes <= f.ano_mes  -- Até o mês atual
        AND (
            -- Se estamos no S1 (meses 01-06), pega desde janeiro
            (RIGHT(f.ano_mes, 2) BETWEEN '01' AND '06' 
             AND RIGHT(f2.ano_mes, 2) BETWEEN '01' AND '06')
            OR
            -- Se estamos no S2 (meses 07-12), pega desde julho
            (RIGHT(f.ano_mes, 2) BETWEEN '07' AND '12' 
             AND RIGHT(f2.ano_mes, 2) BETWEEN '07' AND '12')
        )
    GROUP BY f.cod_assessor, f.ano_mes
),

-- CTE para calcular média acumulada TRIMESTRAL (reinicia a cada trimestre)
metricas_trimestrais AS (
    SELECT 
        f.cod_assessor,
        f.ano_mes,
        AVG(f2.indice_esforco_assessor) as indice_esforco_assessor_trimestre
    FROM base_dados f
    INNER JOIN base_dados f2
        ON f.cod_assessor = f2.cod_assessor
        AND LEFT(f.ano_mes, 4) = LEFT(f2.ano_mes, 4)  -- Mesmo ano
        AND f2.ano_mes <= f.ano_mes  -- Até o mês atual
        AND (
            -- Q1: Janeiro a Março
            (RIGHT(f.ano_mes, 2) BETWEEN '01' AND '03' 
             AND RIGHT(f2.ano_mes, 2) BETWEEN '01' AND '03')
            OR
            -- Q2: Abril a Junho
            (RIGHT(f.ano_mes, 2) BETWEEN '04' AND '06' 
             AND RIGHT(f2.ano_mes, 2) BETWEEN '04' AND '06')
            OR
            -- Q3: Julho a Setembro
            (RIGHT(f.ano_mes, 2) BETWEEN '07' AND '09' 
             AND RIGHT(f2.ano_mes, 2) BETWEEN '07' AND '09')
            OR
            -- Q4: Outubro a Dezembro
            (RIGHT(f.ano_mes, 2) BETWEEN '10' AND '12' 
             AND RIGHT(f2.ano_mes, 2) BETWEEN '10' AND '12')
        )
    GROUP BY f.cod_assessor, f.ano_mes
)

SELECT 
    -- Campos temporais
    c.ano,
    CAST(f.ano_mes AS INT) as ano_mes,
    c.mes,
    c.nome_mes,
    CASE 
        WHEN c.mes BETWEEN 1 AND 6 THEN 'S1'
        ELSE 'S2'
    END as semestre,
    c.trimestre,
    
    -- Identificadores do assessor
    f.cod_assessor,
    p.crm_id as crm_id_assessor,
    p.nome_pessoa as nome_assessor,
    p.assessor_nivel as nivel_assessor,
    
    -- Estrutura organizacional
    fep.id_estrutura as estrutura_id,
    e.nome_estrutura as estrutura_nome,
    
    -- Métricas de esforço
    f.esforco_prospeccao,
    f.esforco_relacionamento,
    f.indice_esforco_assessor,
    
    -- Métricas acumuladas
    f.indice_esforco_assessor_acum_3_meses as indice_esforco_assessor_3_meses,
    f.indice_esforco_assessor_acum_6_meses as indice_esforco_assessor_6_meses,
    f.indice_esforco_assessor_acum_12_meses as indice_esforco_assessor_12_meses,
    ma.indice_esforco_assessor_ano,
    ms.indice_esforco_assessor_semestre,
    mt.indice_esforco_assessor_trimestre,
    
    -- Métricas de prospecção
    f.prospeccao_atingimento_carteiras_simuladas_novos,
    f.prospeccao_atingimento_conversao,
    f.prospeccao_atingimento_habilitacoes,
    f.prospeccao_atingimento_lead_starts,
    f.prospeccao_captacao_de_novos_clientes_por_aai,
    
    -- Métricas de relacionamento
    f.relacionamento_atingimento_contas_acessadas_hub,
    f.relacionamento_atingimento_contas_aportarem,
    f.relacionamento_atingimento_ordens_enviadas,
    f.relacionamento_captacao_da_base
    
   
FROM base_dados f

-- Join com dimensão pessoas
INNER JOIN silver.dim_pessoas p 
    ON f.cod_assessor = p.cod_aai

-- Join com estrutura de pessoas (pegar estrutura vigente no período)
LEFT JOIN silver.fact_estrutura_pessoas fep 
    ON p.crm_id = fep.crm_id
    AND CAST(CONCAT(f.ano_mes, '01') AS DATE) >= fep.data_entrada
    AND (fep.data_saida IS NULL OR CAST(CONCAT(f.ano_mes, '01') AS DATE) <= fep.data_saida)

-- Join com dimensão estruturas
LEFT JOIN silver.dim_estruturas e 
    ON fep.id_estrutura = e.id_estrutura

-- Join com calendário para obter informações temporais
INNER JOIN silver.dim_calendario c 
    ON f.ano_mes = c.ano_mes
    AND c.dia = 1  -- Pegar primeiro dia do mês

-- Joins com métricas acumuladas
INNER JOIN metricas_anuais ma 
    ON f.cod_assessor = ma.cod_assessor 
    AND f.ano_mes = ma.ano_mes

INNER JOIN metricas_semestrais ms 
    ON f.cod_assessor = ms.cod_assessor 
    AND f.ano_mes = ms.ano_mes

INNER JOIN metricas_trimestrais mt 
    ON f.cod_assessor = mt.cod_assessor 
    AND f.ano_mes = mt.ano_mes;
GO
