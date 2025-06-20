-- ==============================================================================
-- QRY-AHE-006-create_vw_habilitacoes_ativacoes
-- ==============================================================================
-- Tipo: CREATE VIEW
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [equipe.dados@m7investimentos.com.br]
-- Revisor: [revisor@m7investimentos.com.br]
-- Tags: [habilitacoes, ativacoes, assessor, consolidacao, gold]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: View que consolida dados de habilitações e ativações por assessor,
calculando métricas mensais e acumulados por diferentes períodos (trimestre, 
semestre, ano) e janelas móveis (3, 6, 12 meses). Segmenta por valor de 
patrimônio (acima/abaixo de R$ 300k).

Casos de uso:
- Base para tabela materializada gold.habilitacoes_ativacoes
- Consultas ad-hoc para análise de tendências
- Validação de dados antes da materialização
- Fonte para dashboards em tempo real

Frequência de consulta: Várias vezes ao dia
Tempo médio de execução: 2-3 minutos (sem materialização)
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
| Coluna                                   | Tipo         | Descrição                                           |
|------------------------------------------|--------------|-----------------------------------------------------|
| ano_mes                                  | INT          | Período no formato AAAAMM                          |
| ano                                      | INT          | Ano de referência                                  |
| mes                                      | INT          | Mês de referência (1-12)                          |
| nome_mes                                 | VARCHAR(20)  | Nome do mês em inglês                             |
| trimestre                                | VARCHAR(2)   | Trimestre (Q1-Q4)                                   |
| semestre                                 | VARCHAR(2)   | Semestre (S1-S2)                                    |
| cod_assessor                             | VARCHAR(20)  | Código do assessor (cod_aai)                       |
| crm_id_assessor                          | VARCHAR(20)  | ID do assessor no CRM                              |
| nome_assessor                            | VARCHAR(200) | Nome completo do assessor                          |
| nivel_assessor                           | VARCHAR(50)  | Nível hierárquico                                  |
| estrutura_id                             | INT          | ID da estrutura organizacional                     |
| estrutura_nome                           | VARCHAR(100) | Nome da estrutura                                  |
| qtd_ativacoes_300k_mais                  | INT          | Ativações mensais > 300k                           |
| qtd_ativacoes_300k_menos                 | INT          | Ativações mensais <= 300k                          |
| qtd_habilitacoes_300k_mais               | INT          | Habilitações mensais > 300k                        |
| qtd_habilitacoes_300k_menos              | INT          | Habilitações mensais <= 300k                       |
| [métricas acumuladas e janelas móveis]   | INT          | Totais por período e janelas                       |
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- silver.fact_ativacoes_habilitacoes_evasoes: Fatos de movimentações de clientes
- silver.fact_estrutura_pessoas: Histórico de alocação de pessoas em estruturas
- silver.dim_calendario: Dimensão de datas
- silver.dim_pessoas: Cadastro de pessoas (assessores)
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
CREATE   VIEW [gold].[vw_habilitacoes_ativacoes] AS

WITH 
-- CTE1: Agregação mensal base com segmentação por valor e estrutura vigente
base_mensal AS (
    SELECT 
        -- Chaves de agrupamento
        CAST(FORMAT(f.data_ref, 'yyyyMM') AS INT) as ano_mes,
        f.crm_id, -- Mantém o crm_id para fazer o join com dim_pessoas
        -- Pega a estrutura vigente na data do evento
        COALESCE(ep.id_estrutura, f.id_estrutura) as id_estrutura,
        
        -- Métricas de Ativações
        COUNT(CASE 
            WHEN f.tipo_movimentacao = 'ativacao' 
            AND f.faixa_pl != 'ate 300k' 
            THEN 1 
        END) as qtd_ativacoes_300k_mais,
        
        COUNT(CASE 
            WHEN f.tipo_movimentacao = 'ativacao' 
            AND f.faixa_pl = 'ate 300k' 
            THEN 1 
        END) as qtd_ativacoes_300k_menos,
        
        -- Métricas de Habilitações
        COUNT(CASE 
            WHEN f.tipo_movimentacao = 'habilitacao' 
            AND f.faixa_pl != 'ate 300k' 
            THEN 1 
        END) as qtd_habilitacoes_300k_mais,
        
        COUNT(CASE 
            WHEN f.tipo_movimentacao = 'habilitacao' 
            AND f.faixa_pl = 'ate 300k' 
            THEN 1 
        END) as qtd_habilitacoes_300k_menos
        
    FROM silver.fact_ativacoes_habilitacoes_evasoes f
    -- Join para pegar a estrutura vigente do assessor na data do evento
    LEFT JOIN silver.fact_estrutura_pessoas ep
        ON ep.crm_id = f.crm_id
        AND f.data_ref >= ep.data_entrada
        AND (ep.data_saida IS NULL OR f.data_ref <= ep.data_saida)
    WHERE f.tipo_movimentacao IN ('ativacao', 'habilitacao')
    GROUP BY 
        CAST(FORMAT(f.data_ref, 'yyyyMM') AS INT),
        f.crm_id,
        COALESCE(ep.id_estrutura, f.id_estrutura)
),

-- CTE2: Enriquecimento com dados do calendário e dimensões
dados_enriquecidos AS (
    SELECT DISTINCT
        b.ano_mes,
        c.ano,
        c.mes,
        c.nome_mes,
        c.trimestre,
        CASE 
            WHEN c.mes BETWEEN 1 AND 6 THEN 'S1'
            ELSE 'S2'
        END as semestre,
        p.cod_aai as cod_assessor, -- ALTERADO: Agora usa cod_aai da dim_pessoas
        b.crm_id as crm_id_assessor, -- Mantendo o crm_id original
        COALESCE(p.nome_pessoa, 'Assessor não identificado') as nome_assessor,
        p.assessor_nivel as nivel_assessor,
        b.id_estrutura as estrutura_id,
        COALESCE(e.nome_estrutura, 'Estrutura não identificada') as estrutura_nome,
        b.qtd_ativacoes_300k_mais,
        b.qtd_ativacoes_300k_menos,
        b.qtd_habilitacoes_300k_mais,
        b.qtd_habilitacoes_300k_menos
    FROM base_mensal b
    -- Join com calendário usando o primeiro dia do mês
    INNER JOIN silver.dim_calendario c 
        ON c.ano_mes = CAST(b.ano_mes AS CHAR(6))
        AND c.dia = 1
    -- Join com pessoas para dados do assessor
    LEFT JOIN silver.dim_pessoas p 
        ON p.crm_id = b.crm_id
    -- Join com estruturas usando id_estrutura correto
    LEFT JOIN silver.dim_estruturas e 
        ON e.id_estrutura = b.id_estrutura
),

-- CTE3: Cálculo de acumulados por período (trimestre, semestre, ano)
acumulados_periodo AS (
    SELECT 
        d1.ano_mes,
        d1.ano,
        d1.mes,
        d1.nome_mes,
        d1.trimestre,
        d1.semestre,
        d1.cod_assessor,
        d1.crm_id_assessor,
        d1.nome_assessor,
        d1.nivel_assessor,
        d1.estrutura_id,
        d1.estrutura_nome,
        
        -- Métricas mensais
        d1.qtd_ativacoes_300k_mais,
        d1.qtd_ativacoes_300k_menos,
        d1.qtd_habilitacoes_300k_mais,
        d1.qtd_habilitacoes_300k_menos,
        
        -- Acumulados do trimestre
        SUM(d2.qtd_ativacoes_300k_mais) as qtd_ativacoes_300k_mais_trimestre,
        SUM(d2.qtd_ativacoes_300k_menos) as qtd_ativacoes_300k_menos_trimestre,
        SUM(d2.qtd_habilitacoes_300k_mais) as qtd_habilitacoes_300k_mais_trimestre,
        SUM(d2.qtd_habilitacoes_300k_menos) as qtd_habilitacoes_300k_menos_trimestre,
        
        -- Acumulados do semestre
        SUM(d3.qtd_ativacoes_300k_mais) as qtd_ativacoes_300k_mais_semestre,
        SUM(d3.qtd_ativacoes_300k_menos) as qtd_ativacoes_300k_menos_semestre,
        SUM(d3.qtd_habilitacoes_300k_mais) as qtd_habilitacoes_300k_mais_semestre,
        SUM(d3.qtd_habilitacoes_300k_menos) as qtd_habilitacoes_300k_menos_semestre,
        
        -- Acumulados do ano
        SUM(d4.qtd_ativacoes_300k_mais) as qtd_ativacoes_300k_mais_ano,
        SUM(d4.qtd_ativacoes_300k_menos) as qtd_ativacoes_300k_menos_ano,
        SUM(d4.qtd_habilitacoes_300k_mais) as qtd_habilitacoes_300k_mais_ano,
        SUM(d4.qtd_habilitacoes_300k_menos) as qtd_habilitacoes_300k_menos_ano
        
    FROM dados_enriquecidos d1
    
    -- Join para acumulado trimestre
    LEFT JOIN dados_enriquecidos d2 
        ON d2.cod_assessor = d1.cod_assessor 
        AND d2.ano = d1.ano 
        AND d2.trimestre = d1.trimestre
        AND d2.ano_mes <= d1.ano_mes
    
    -- Join para acumulado semestre
    LEFT JOIN dados_enriquecidos d3 
        ON d3.cod_assessor = d1.cod_assessor 
        AND d3.ano = d1.ano 
        AND d3.semestre = d1.semestre
        AND d3.ano_mes <= d1.ano_mes
    
    -- Join para acumulado ano
    LEFT JOIN dados_enriquecidos d4 
        ON d4.cod_assessor = d1.cod_assessor 
        AND d4.ano = d1.ano
        AND d4.ano_mes <= d1.ano_mes
        
    GROUP BY 
        d1.ano_mes, d1.ano, d1.mes, d1.nome_mes, d1.trimestre, d1.semestre,
        d1.cod_assessor, d1.crm_id_assessor, d1.nome_assessor, d1.nivel_assessor,
        d1.estrutura_id, d1.estrutura_nome,
        d1.qtd_ativacoes_300k_mais, d1.qtd_ativacoes_300k_menos,
        d1.qtd_habilitacoes_300k_mais, d1.qtd_habilitacoes_300k_menos
),

-- CTE4: Cálculo de janelas móveis (3, 6, 12 meses)
janelas_moveis AS (
    SELECT 
        a1.*,
        
        -- Janela móvel 3 meses
        SUM(a2.qtd_ativacoes_300k_mais) as qtd_ativacoes_300k_mais_3_meses,
        SUM(a2.qtd_ativacoes_300k_menos) as qtd_ativacoes_300k_menos_3_meses,
        SUM(a2.qtd_habilitacoes_300k_mais) as qtd_habilitacoes_300k_mais_3_meses,
        SUM(a2.qtd_habilitacoes_300k_menos) as qtd_habilitacoes_300k_menos_3_meses,
        
        -- Janela móvel 6 meses
        SUM(a3.qtd_ativacoes_300k_mais) as qtd_ativacoes_300k_mais_6_meses,
        SUM(a3.qtd_ativacoes_300k_menos) as qtd_ativacoes_300k_menos_6_meses,
        SUM(a3.qtd_habilitacoes_300k_mais) as qtd_habilitacoes_300k_mais_6_meses,
        SUM(a3.qtd_habilitacoes_300k_menos) as qtd_habilitacoes_300k_menos_6_meses,
        
        -- Janela móvel 12 meses
        SUM(a4.qtd_ativacoes_300k_mais) as qtd_ativacoes_300k_mais_12_meses,
        SUM(a4.qtd_ativacoes_300k_menos) as qtd_ativacoes_300k_menos_12_meses,
        SUM(a4.qtd_habilitacoes_300k_mais) as qtd_habilitacoes_300k_mais_12_meses,
        SUM(a4.qtd_habilitacoes_300k_menos) as qtd_habilitacoes_300k_menos_12_meses
        
    FROM acumulados_periodo a1
    
    -- Join para janela 3 meses
    LEFT JOIN dados_enriquecidos a2 
        ON a2.cod_assessor = a1.cod_assessor
        AND a2.ano_mes BETWEEN 
            CASE 
                WHEN a1.mes <= 3 THEN (a1.ano - 1) * 100 + (12 + a1.mes - 2)
                ELSE a1.ano * 100 + (a1.mes - 2)
            END
            AND a1.ano_mes
    
    -- Join para janela 6 meses
    LEFT JOIN dados_enriquecidos a3 
        ON a3.cod_assessor = a1.cod_assessor
        AND a3.ano_mes BETWEEN 
            CASE 
                WHEN a1.mes <= 6 THEN (a1.ano - 1) * 100 + (12 + a1.mes - 5)
                ELSE a1.ano * 100 + (a1.mes - 5)
            END
            AND a1.ano_mes
    
    -- Join para janela 12 meses
    LEFT JOIN dados_enriquecidos a4 
        ON a4.cod_assessor = a1.cod_assessor
        AND a4.ano_mes BETWEEN ((a1.ano - 1) * 100 + a1.mes + 1) AND a1.ano_mes
        
    GROUP BY 
        a1.ano_mes, a1.ano, a1.mes, a1.nome_mes, a1.trimestre, a1.semestre,
        a1.cod_assessor, a1.crm_id_assessor, a1.nome_assessor, a1.nivel_assessor,
        a1.estrutura_id, a1.estrutura_nome,
        a1.qtd_ativacoes_300k_mais, a1.qtd_ativacoes_300k_menos,
        a1.qtd_habilitacoes_300k_mais, a1.qtd_habilitacoes_300k_menos,
        a1.qtd_ativacoes_300k_mais_trimestre, a1.qtd_ativacoes_300k_menos_trimestre,
        a1.qtd_habilitacoes_300k_mais_trimestre, a1.qtd_habilitacoes_300k_menos_trimestre,
        a1.qtd_ativacoes_300k_mais_semestre, a1.qtd_ativacoes_300k_menos_semestre,
        a1.qtd_habilitacoes_300k_mais_semestre, a1.qtd_habilitacoes_300k_menos_semestre,
        a1.qtd_ativacoes_300k_mais_ano, a1.qtd_ativacoes_300k_menos_ano,
        a1.qtd_habilitacoes_300k_mais_ano, a1.qtd_habilitacoes_300k_menos_ano
)

-- Query final
SELECT 
    ano_mes,
    ano,
    mes,
    nome_mes,
    trimestre,
    semestre,
    cod_assessor,
    crm_id_assessor,
    nome_assessor,
    nivel_assessor,
    estrutura_id,
    estrutura_nome,
    
    -- Ativações Mensal
    COALESCE(qtd_ativacoes_300k_mais, 0) as qtd_ativacoes_300k_mais,
    COALESCE(qtd_ativacoes_300k_menos, 0) as qtd_ativacoes_300k_menos,
    
    -- Ativações Acumuladas Período
    COALESCE(qtd_ativacoes_300k_mais_trimestre, 0) as qtd_ativacoes_300k_mais_trimestre,
    COALESCE(qtd_ativacoes_300k_menos_trimestre, 0) as qtd_ativacoes_300k_menos_trimestre,
    COALESCE(qtd_ativacoes_300k_mais_semestre, 0) as qtd_ativacoes_300k_mais_semestre,
    COALESCE(qtd_ativacoes_300k_menos_semestre, 0) as qtd_ativacoes_300k_menos_semestre,
    COALESCE(qtd_ativacoes_300k_mais_ano, 0) as qtd_ativacoes_300k_mais_ano,
    COALESCE(qtd_ativacoes_300k_menos_ano, 0) as qtd_ativacoes_300k_menos_ano,
    
    -- Ativações Janela Móvel
    COALESCE(qtd_ativacoes_300k_mais_3_meses, 0) as qtd_ativacoes_300k_mais_3_meses,
    COALESCE(qtd_ativacoes_300k_menos_3_meses, 0) as qtd_ativacoes_300k_menos_3_meses,
    COALESCE(qtd_ativacoes_300k_mais_6_meses, 0) as qtd_ativacoes_300k_mais_6_meses,
    COALESCE(qtd_ativacoes_300k_menos_6_meses, 0) as qtd_ativacoes_300k_menos_6_meses,
    COALESCE(qtd_ativacoes_300k_mais_12_meses, 0) as qtd_ativacoes_300k_mais_12_meses,
    COALESCE(qtd_ativacoes_300k_menos_12_meses, 0) as qtd_ativacoes_300k_menos_12_meses,
    
    -- Habilitações Mensal
    COALESCE(qtd_habilitacoes_300k_mais, 0) as qtd_habilitacoes_300k_mais,
    COALESCE(qtd_habilitacoes_300k_menos, 0) as qtd_habilitacoes_300k_menos,
    
    -- Habilitações Acumuladas Período
    COALESCE(qtd_habilitacoes_300k_mais_trimestre, 0) as qtd_habilitacoes_300k_mais_trimestre,
    COALESCE(qtd_habilitacoes_300k_menos_trimestre, 0) as qtd_habilitacoes_300k_menos_trimestre,
    COALESCE(qtd_habilitacoes_300k_mais_semestre, 0) as qtd_habilitacoes_300k_mais_semestre,
    COALESCE(qtd_habilitacoes_300k_menos_semestre, 0) as qtd_habilitacoes_300k_menos_semestre,
    COALESCE(qtd_habilitacoes_300k_mais_ano, 0) as qtd_habilitacoes_300k_mais_ano,
    COALESCE(qtd_habilitacoes_300k_menos_ano, 0) as qtd_habilitacoes_300k_menos_ano,
    
    -- Habilitações Janela Móvel
    COALESCE(qtd_habilitacoes_300k_mais_3_meses, 0) as qtd_habilitacoes_300k_mais_3_meses,
    COALESCE(qtd_habilitacoes_300k_menos_3_meses, 0) as qtd_habilitacoes_300k_menos_3_meses,
    COALESCE(qtd_habilitacoes_300k_mais_6_meses, 0) as qtd_habilitacoes_300k_mais_6_meses,
    COALESCE(qtd_habilitacoes_300k_menos_6_meses, 0) as qtd_habilitacoes_300k_menos_6_meses,
    COALESCE(qtd_habilitacoes_300k_mais_12_meses, 0) as qtd_habilitacoes_300k_mais_12_meses,
    COALESCE(qtd_habilitacoes_300k_menos_12_meses, 0) as qtd_habilitacoes_300k_menos_12_meses
    
    
FROM janelas_moveis;
GO

-- ==============================================================================
-- 7. CONSIDERAÇÕES TÉCNICAS
-- ==============================================================================
/*
- CTEs utilizadas para modularizar a lógica complexa:
  1. base_mensal: Agregação inicial por mês/assessor/estrutura
  2. dados_enriquecidos: Adiciona dimensões e metadados
  3. acumulados_periodo: Calcula totais por trimestre/semestre/ano
  4. janelas_moveis: Calcula janelas de 3/6/12 meses

- JOINs com estrutura vigente consideram data_entrada/data_saida
- COALESCE utilizado para garantir valores default (0) em métricas
- Window functions evitadas em favor de self-joins para melhor performance
*/

-- ==============================================================================
-- 8. QUERIES AUXILIARES PARA VALIDAÇÃO
-- ==============================================================================
/*
-- Verificar quantidade de registros por período
SELECT 
    ano_mes,
    COUNT(DISTINCT cod_assessor) as qtd_assessores,
    SUM(qtd_ativacoes_300k_mais + qtd_ativacoes_300k_menos) as total_ativacoes,
    SUM(qtd_habilitacoes_300k_mais + qtd_habilitacoes_300k_menos) as total_habilitacoes
FROM gold.vw_habilitacoes_ativacoes
GROUP BY ano_mes
ORDER BY ano_mes DESC;

-- Validar consistência de acumulados
SELECT TOP 10
    cod_assessor,
    nome_assessor,
    ano_mes,
    qtd_ativacoes_300k_mais,
    qtd_ativacoes_300k_mais_trimestre,
    qtd_ativacoes_300k_mais_ano
FROM gold.vw_habilitacoes_ativacoes
WHERE ano_mes = (SELECT MAX(ano_mes) FROM gold.vw_habilitacoes_ativacoes)
ORDER BY qtd_ativacoes_300k_mais_ano DESC;
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
- View utiliza cod_aai da dim_pessoas como cod_assessor (não o crm_id)
- Estrutura vigente é determinada pela data do evento (data_ref)
- Segmentação por patrimônio usa faixa_pl = 'ate 300k' como critério
- Janelas móveis incluem o mês atual no cálculo
- Performance pode ser impactada com grandes volumes - usar tabela materializada

Otimizações recomendadas:
- Índice em fact_ativacoes_habilitacoes_evasoes (crm_id, data_ref, tipo_movimentacao)
- Índice em fact_estrutura_pessoas (crm_id, data_entrada, data_saida)
- Estatísticas atualizadas nas tabelas base

Limitações conhecidas:
- Não processa assessores sem nenhuma movimentação no período
- Janelas móveis podem ter dados incompletos nos primeiros meses

Contato para dúvidas: equipe-dados@m7investimentos.com.br
*/
