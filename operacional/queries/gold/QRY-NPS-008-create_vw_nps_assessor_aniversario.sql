-- ==============================================================================
-- QRY-NPS-008-create_vw_nps_assessor_aniversario
-- ==============================================================================
-- Tipo: CREATE VIEW
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [equipe.dados@m7investimentos.com.br]
-- Revisor: [revisor@m7investimentos.com.br]
-- Tags: [nps, assessor, aniversario, consolidacao, gold]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: View que consolida dados de NPS por assessor, agregando métricas mensais
baseadas em pesquisas enviadas em aniversários de clientes. Separa métricas de envio
(data_entrega) e resposta (data_resposta) para análise precisa.

Casos de uso:
- Base para tabela materializada gold.nps_assessor_aniversario
- Consultas ad-hoc para análise de satisfação
- Validação de cálculos de NPS antes da materialização
- Fonte para dashboards em tempo real de NPS

Frequência de consulta: Várias vezes ao dia
Tempo médio de execução: 1-2 minutos
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
| Coluna                        | Tipo          | Descrição                                     |
|-------------------------------|---------------|-----------------------------------------------|
| ano_mes                       | INT           | Período no formato AAAAMM                    |
| ano                           | INT           | Ano de referência                            |
| mes                           | INT           | Mês de referência (1-12)                    |
| nome_mes                      | VARCHAR(20)   | Nome do mês em português                    |
| trimestre                     | VARCHAR(2)    | Trimestre (Q1-Q4)                             |
| semestre                      | VARCHAR(2)    | Semestre (S1-S2)                              |
| cod_assessor                  | VARCHAR(20)   | Código do assessor                           |
| crm_id_assessor               | VARCHAR(20)   | ID do assessor no CRM                         |
| nome_assessor                 | VARCHAR(200)  | Nome completo do assessor                     |
| nivel_assessor                | VARCHAR(50)   | Nível hierárquico                            |
| estrutura_id                  | INT           | ID da estrutura organizacional                |
| estrutura_nome                | VARCHAR(100)  | Nome da estrutura                             |
| qtd_pesquisas_enviadas        | INT           | Total de pesquisas enviadas                   |
| qtd_pesquisas_respondidas     | INT           | Total de pesquisas respondidas                |
| taxa_resposta                 | FLOAT         | Taxa de resposta (decimal)                    |
| nps_score_assessor_[periodo]  | FLOAT         | NPS Score por período                        |
| [métricas detalhadas]         | Vários        | Promotores, neutros, detratores, etc         |
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- silver.fact_nps_respostas_envios_aniversario: Fatos de pesquisas NPS
- silver.dim_pessoas: Cadastro de pessoas (assessores)
- silver.fact_estrutura_pessoas: Histórico de alocação em estruturas
- silver.dim_estruturas: Cadastro de estruturas organizacionais

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
CREATE   VIEW [gold].[vw_nps_assessor_aniversario] AS

WITH base_periodo AS (
    -- Gera todos os períodos (ano/mês) e assessores para garantir que todos apareçam
    SELECT DISTINCT
        p.cod_aai as cod_assessor,
        c.ano,
        c.mes
    FROM silver.dim_pessoas p
    CROSS JOIN (
        SELECT DISTINCT 
            YEAR(data_entrega) as ano,
            MONTH(data_entrega) as mes
        FROM silver.fact_nps_respostas_envios_aniversario
        WHERE data_entrega IS NOT NULL
        UNION
        SELECT DISTINCT 
            YEAR(data_resposta) as ano,
            MONTH(data_resposta) as mes
        FROM silver.fact_nps_respostas_envios_aniversario
        WHERE data_resposta IS NOT NULL
    ) c
    WHERE p.cod_aai IS NOT NULL
),

metricas_envio AS (
    -- Métricas baseadas na data de ENTREGA (envio) - COM FILTRO DE STATUS
    SELECT 
        n.cod_assessor,
        YEAR(n.data_entrega) as ano,
        MONTH(n.data_entrega) as mes,
        
        -- Conta apenas pesquisas com status válidos (exclui delivery_bounced, not_sampled, expired)
        COUNT(DISTINCT CASE 
            WHEN n.survey_status NOT IN ('delivery_bounced', 'not_sampled', 'expired')
            THEN n.survey_id 
        END) as qtd_pesquisas_enviadas,
        
        COUNT(DISTINCT CASE 
            WHEN n.convite_aberto = 'sim' 
            AND n.survey_status NOT IN ('delivery_bounced', 'not_sampled', 'expired')
            THEN n.survey_id 
        END) as qtd_convites_abertos
        
    FROM silver.fact_nps_respostas_envios_aniversario n
    WHERE n.data_entrega IS NOT NULL
        AND n.cod_assessor IS NOT NULL
    GROUP BY n.cod_assessor, YEAR(n.data_entrega), MONTH(n.data_entrega)
),

metricas_resposta AS (
    -- Métricas baseadas na data de RESPOSTA
    SELECT 
        n.cod_assessor,
        YEAR(n.data_resposta) as ano,
        MONTH(n.data_resposta) as mes,
        COUNT(DISTINCT n.survey_id) as qtd_pesquisas_respondidas,
        
        -- Quantidades para cálculo do NPS
        COUNT(CASE WHEN n.classificacao_nps_assessor = 'Promotor' THEN 1 END) as qtd_promotores,
        COUNT(CASE WHEN n.classificacao_nps_assessor = 'Neutro' THEN 1 END) as qtd_neutros,
        COUNT(CASE WHEN n.classificacao_nps_assessor = 'Detrator' THEN 1 END) as qtd_detratores,
        COUNT(CASE WHEN n.recomendaria_assessor = 'sim' THEN 1 END) as qtd_recomendaria_sim,
        
        -- NPS Score do mês
        CASE 
            WHEN COUNT(CASE WHEN n.classificacao_nps_assessor IS NOT NULL THEN 1 END) > 0
            THEN (CAST(COUNT(CASE WHEN n.classificacao_nps_assessor = 'Promotor' THEN 1 END) AS FLOAT) - 
                  CAST(COUNT(CASE WHEN n.classificacao_nps_assessor = 'Detrator' THEN 1 END) AS FLOAT)) / 
                 COUNT(CASE WHEN n.classificacao_nps_assessor IS NOT NULL THEN 1 END)
            ELSE NULL
        END as nps_score_mes
        
    FROM silver.fact_nps_respostas_envios_aniversario n
    WHERE n.data_resposta IS NOT NULL
        AND n.cod_assessor IS NOT NULL
    GROUP BY n.cod_assessor, YEAR(n.data_resposta), MONTH(n.data_resposta)
),

razao_principal AS (
    -- Identifica a razão mais citada por assessor/mês baseado na data de RESPOSTA
    SELECT 
        cod_assessor,
        ano,
        mes,
        razao_nps_assessor as razao_principal
    FROM (
        SELECT 
            n.cod_assessor,
            YEAR(n.data_resposta) as ano,
            MONTH(n.data_resposta) as mes,
            n.razao_nps_assessor,
            COUNT(*) as qtd_razao,
            ROW_NUMBER() OVER (
                PARTITION BY n.cod_assessor, YEAR(n.data_resposta), MONTH(n.data_resposta) 
                ORDER BY COUNT(*) DESC, n.razao_nps_assessor
            ) as rn
        FROM silver.fact_nps_respostas_envios_aniversario n
        WHERE n.data_resposta IS NOT NULL
            AND n.cod_assessor IS NOT NULL
            AND n.razao_nps_assessor IS NOT NULL
        GROUP BY n.cod_assessor, YEAR(n.data_resposta), MONTH(n.data_resposta), n.razao_nps_assessor
    ) t
    WHERE rn = 1
),

scores_acumulados AS (
    -- Calcula scores acumulados (trimestre, semestre, ano) baseado em data de resposta
    SELECT 
        m.cod_assessor,
        m.ano,
        m.mes,
        m.nps_score_mes,
        
        -- Score acumulado do trimestre (reinicia a cada trimestre)
        AVG(m.nps_score_mes) OVER (
            PARTITION BY m.cod_assessor, m.ano, 
                CASE 
                    WHEN m.mes IN (1,2,3) THEN 1
                    WHEN m.mes IN (4,5,6) THEN 2
                    WHEN m.mes IN (7,8,9) THEN 3
                    WHEN m.mes IN (10,11,12) THEN 4
                END
            ORDER BY m.mes
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as nps_score_assessor_trimestre,
        
        -- Score acumulado do semestre (reinicia a cada semestre)
        AVG(m.nps_score_mes) OVER (
            PARTITION BY m.cod_assessor, m.ano,
                CASE 
                    WHEN m.mes <= 6 THEN 1
                    ELSE 2
                END
            ORDER BY m.mes
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as nps_score_assessor_semestre,
        
        -- Score acumulado do ano (reinicia todo janeiro)
        AVG(m.nps_score_mes) OVER (
            PARTITION BY m.cod_assessor, m.ano
            ORDER BY m.mes
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as nps_score_assessor_ano
        
    FROM metricas_resposta m
),

scores_moveis AS (
    -- Calcula médias móveis (3, 6, 12 meses) baseado em data de resposta
    SELECT 
        m.cod_assessor,
        m.ano,
        m.mes,
        
        -- Média móvel 3 meses
        AVG(m.nps_score_mes) OVER (
            PARTITION BY m.cod_assessor
            ORDER BY m.ano, m.mes
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as nps_score_assessor_3_meses,
        
        -- Média móvel 6 meses
        AVG(m.nps_score_mes) OVER (
            PARTITION BY m.cod_assessor
            ORDER BY m.ano, m.mes
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) as nps_score_assessor_6_meses,
        
        -- Média móvel 12 meses
        AVG(m.nps_score_mes) OVER (
            PARTITION BY m.cod_assessor
            ORDER BY m.ano, m.mes
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) as nps_score_assessor_12_meses
        
    FROM metricas_resposta m
)

-- Query final unindo todas as informações
SELECT 
    -- Dimensões Temporais
    (bp.ano * 100 + bp.mes) as ano_mes,
    bp.ano,
    bp.mes,
    CASE bp.mes
        WHEN 1 THEN 'Janeiro'
        WHEN 2 THEN 'Fevereiro'
        WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Maio'
        WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro'
        WHEN 11 THEN 'Novembro'
        WHEN 12 THEN 'Dezembro'
    END as nome_mes,
    CASE 
        WHEN bp.mes IN (1,2,3) THEN 'Q1'
        WHEN bp.mes IN (4,5,6) THEN 'Q2'
        WHEN bp.mes IN (7,8,9) THEN 'Q3'
        WHEN bp.mes IN (10,11,12) THEN 'Q4'
    END as trimestre,
    CASE 
        WHEN bp.mes <= 6 THEN 'S1'
        ELSE 'S2'
    END as semestre,
    
    -- Dimensões do Assessor
    bp.cod_assessor,
    p.crm_id as crm_id_assessor,
    p.nome_pessoa as nome_assessor,
    p.assessor_nivel as nivel_assessor,
    ep.id_estrutura as estrutura_id,
    e.nome_estrutura as estrutura_nome,
    
    -- Métricas de Volume
    COALESCE(me.qtd_pesquisas_enviadas, 0) as qtd_pesquisas_enviadas,
    COALESCE(mr.qtd_pesquisas_respondidas, 0) as qtd_pesquisas_respondidas,
    COALESCE(me.qtd_convites_abertos, 0) as qtd_convites_abertos,
    CASE 
        WHEN me.qtd_pesquisas_enviadas > 0 
        THEN CAST(mr.qtd_pesquisas_respondidas AS FLOAT) / me.qtd_pesquisas_enviadas
        ELSE NULL 
    END as taxa_resposta,
    CASE 
        WHEN me.qtd_pesquisas_enviadas > 0 
        THEN CAST(me.qtd_convites_abertos AS FLOAT) / me.qtd_pesquisas_enviadas
        ELSE NULL 
    END as taxa_abertura,
    
    -- Métricas NPS do Assessor (7 tipos de scores)
    mr.nps_score_mes as nps_score_assessor_mes,
    sa.nps_score_assessor_trimestre,
    sa.nps_score_assessor_semestre,
    sa.nps_score_assessor_ano,
    sm.nps_score_assessor_3_meses,
    sm.nps_score_assessor_6_meses,
    sm.nps_score_assessor_12_meses,
    
    -- Quantidades NPS
    COALESCE(mr.qtd_promotores, 0) as qtd_promotores,
    COALESCE(mr.qtd_neutros, 0) as qtd_neutros,
    COALESCE(mr.qtd_detratores, 0) as qtd_detratores,
    
    -- Indicadores de Recomendação
    COALESCE(mr.qtd_recomendaria_sim, 0) as qtd_recomendaria_sim,
    CASE 
        WHEN mr.qtd_pesquisas_respondidas > 0
        THEN CAST(mr.qtd_recomendaria_sim AS FLOAT) / mr.qtd_pesquisas_respondidas
        ELSE NULL
    END as perc_recomendaria,
    
    -- Análise de Razões
    r.razao_principal

FROM base_periodo bp
    LEFT JOIN metricas_envio me
        ON bp.cod_assessor = me.cod_assessor 
        AND bp.ano = me.ano 
        AND bp.mes = me.mes
    LEFT JOIN metricas_resposta mr
        ON bp.cod_assessor = mr.cod_assessor 
        AND bp.ano = mr.ano 
        AND bp.mes = mr.mes
    LEFT JOIN scores_acumulados sa
        ON mr.cod_assessor = sa.cod_assessor 
        AND mr.ano = sa.ano 
        AND mr.mes = sa.mes
    LEFT JOIN scores_moveis sm
        ON mr.cod_assessor = sm.cod_assessor 
        AND mr.ano = sm.ano 
        AND mr.mes = sm.mes
    LEFT JOIN razao_principal r
        ON bp.cod_assessor = r.cod_assessor 
        AND bp.ano = r.ano 
        AND bp.mes = r.mes
    LEFT JOIN silver.dim_pessoas p 
        ON bp.cod_assessor = p.cod_aai
    LEFT JOIN silver.fact_estrutura_pessoas ep 
        ON p.crm_id = ep.crm_id 
        AND ep.data_saida IS NULL  -- Apenas estrutura atual
    LEFT JOIN silver.dim_estruturas e 
        ON ep.id_estrutura = e.id_estrutura

WHERE p.cod_aai IS NOT NULL  -- Garante que só aparecem assessores válidos
GO

-- ==============================================================================
-- 7. CONSIDERAÇÕES TÉCNICAS
-- ==============================================================================
/*
- CTEs utilizadas para modularizar a lógica complexa:
  1. base_periodo: Gera matriz completa assessor x período
  2. metricas_envio: Agrega dados por data de envio (filtro de status)
  3. metricas_resposta: Agrega dados por data de resposta
  4. razao_principal: Identifica razão mais citada
  5. scores_acumulados: Calcula NPS acumulado por período
  6. scores_moveis: Calcula médias móveis

- Separação entre data_entrega e data_resposta é crítica
- Filtros de status excluem: delivery_bounced, not_sampled, expired
- Window functions usadas para cálculos de médias acumuladas
*/

-- ==============================================================================
-- 8. QUERIES AUXILIARES PARA VALIDAÇÃO
-- ==============================================================================
/*
-- Verificar quantidade de registros por período
SELECT 
    ano_mes,
    COUNT(DISTINCT cod_assessor) as qtd_assessores,
    SUM(qtd_pesquisas_enviadas) as total_enviadas,
    SUM(qtd_pesquisas_respondidas) as total_respondidas,
    AVG(taxa_resposta) as taxa_resposta_media
FROM gold.vw_nps_assessor_aniversario
GROUP BY ano_mes
ORDER BY ano_mes DESC;

-- Validar cálculo do NPS
SELECT TOP 10
    cod_assessor,
    nome_assessor,
    ano_mes,
    qtd_promotores,
    qtd_neutros,
    qtd_detratores,
    qtd_pesquisas_respondidas,
    nps_score_assessor_mes,
    -- Validação manual do cálculo
    CAST(qtd_promotores - qtd_detratores AS FLOAT) / NULLIF(qtd_pesquisas_respondidas, 0) as nps_calculado
FROM gold.vw_nps_assessor_aniversario
WHERE ano_mes = (SELECT MAX(ano_mes) FROM gold.vw_nps_assessor_aniversario)
    AND qtd_pesquisas_respondidas > 0
ORDER BY nps_score_assessor_mes DESC;

-- Analisar razões principais
SELECT 
    razao_principal,
    COUNT(*) as qtd_assessores,
    AVG(nps_score_assessor_mes) as nps_medio
FROM gold.vw_nps_assessor_aniversario
WHERE razao_principal IS NOT NULL
    AND ano_mes >= (SELECT MAX(ano_mes) - 300 FROM gold.vw_nps_assessor_aniversario)
GROUP BY razao_principal
ORDER BY qtd_assessores DESC;
*/

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-20 | equipe.dados   | Criação inicial da view

*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- View separa métricas por data de envio vs data de resposta
- Status inválidos são filtrados: delivery_bounced, not_sampled, expired
- base_periodo garante que todos os assessores aparecem em todos os períodos
- NPS Score calculado como (Promotores - Detratores) / Total Respondidas
- Estrutura atual do assessor determinada por data_saida IS NULL

Cálculos de scores acumulados:
- Trimestre: Reinicia a cada trimestre (Q1-Q4)
- Semestre: Reinicia a cada semestre (S1-S2)
- Ano: Acumulado desde janeiro
- Médias móveis: Janelas de 3, 6 e 12 meses

Otimizações recomendadas:
- Índice em fact_nps_respostas_envios_aniversario (cod_assessor, data_entrega)
- Índice em fact_nps_respostas_envios_aniversario (cod_assessor, data_resposta)
- Estatísticas atualizadas nas tabelas base

Limitações conhecidas:
- Performance pode degradar com grandes volumes históricos
- Assessores sem registro em dim_pessoas não aparecem
- Diferença temporal entre envio e resposta pode causar distorções

Contato para dúvidas: equipe-dados@m7investimentos.com.br
*/
