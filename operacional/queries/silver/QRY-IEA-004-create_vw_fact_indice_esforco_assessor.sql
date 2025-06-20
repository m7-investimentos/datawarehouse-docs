-- ==============================================================================
-- QRY-IEA-004-CREATE_VW_FACT_INDICE_ESFORCO_ASSESSOR
-- ==============================================================================
-- Tipo: View
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [view, indice_esforco, assessor, medias_moveis, silver]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: View que processa dados do Índice de Esforço do Assessor (IEA) da bronze,
           calculando médias móveis de 3, 6 e 12 meses e realizando transformações
           de tipos de dados para garantir precisão decimal adequada.

Casos de uso:
- Base para carga da tabela fact_indice_esforco_assessor
- Cálculo de médias móveis para análise de tendências
- Transformação e padronização de tipos de dados
- Consolidação de métricas de prospecção e relacionamento

Frequência de execução: Sob demanda (materializada mensalmente)
Tempo médio de execução: 30-60 segundos
Volume esperado de linhas: Todos registros históricos da bronze.xp_iea
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Não aplicável - View sem parâmetros

Para filtrar períodos específicos, usar WHERE na consulta:
SELECT * FROM silver.vw_fact_indice_esforco_assessor 
WHERE ano_mes = '202412' -- Dezembro/2024
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Colunas retornadas:

| Coluna                                           | Tipo          | Descrição                                           |
|--------------------------------------------------|---------------|-----------------------------------------------------|
| ano_mes                                          | VARCHAR(6)    | Período de referência (YYYYMM)                     |
| cod_assessor                                     | VARCHAR(20)   | Código do assessor                                 |
| esforco_prospeccao                               | DECIMAL(18,8) | Índice de esforço em prospecção                    |
| esforco_relacionamento                           | DECIMAL(18,8) | Índice de esforço em relacionamento               |
| indice_esforco_assessor                          | DECIMAL(18,8) | IEA principal                                      |
| indice_esforco_assessor_acum_3_meses             | DECIMAL(18,8) | Média móvel simples 3 meses                       |
| indice_esforco_assessor_acum_6_meses             | DECIMAL(18,8) | Média móvel simples 6 meses                       |
| indice_esforco_assessor_acum_12_meses            | DECIMAL(18,8) | Média móvel simples 12 meses                      |
| prospeccao_atingimento_carteiras_simuladas_novos | DECIMAL(18,8) | % atingimento carteiras simuladas                 |
| prospeccao_atingimento_conversao                 | DECIMAL(18,8) | % atingimento conversão                           |
| prospeccao_atingimento_habilitacoes              | DECIMAL(18,8) | % atingimento habilitações                        |
| prospeccao_atingimento_lead_starts               | DECIMAL(18,8) | % atingimento lead starts                         |
| prospeccao_captacao_de_novos_clientes_por_aai    | DECIMAL(16,2) | Valor captado novos clientes                      |
| relacionamento_atingimento_contas_acessadas_hub  | DECIMAL(18,8) | % atingimento contas acessadas                    |
| relacionamento_atingimento_contas_aportarem      | DECIMAL(18,8) | % atingimento contas com aporte                   |
| relacionamento_atingimento_ordens_enviadas       | DECIMAL(18,8) | % atingimento ordens enviadas                     |
| relacionamento_captacao_da_base                  | DECIMAL(18,2) | Valor captado da base                             |

Ordenação padrão: Nenhuma (usar ORDER BY na consulta)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas de origem (bronze):
- [bronze].[xp_iea]: Dados brutos do Índice de Esforço do Assessor
  * Campos utilizados: Todos os campos da tabela
  * Volume: Histórico completo de métricas mensais
  * Granularidade: Um registro por assessor por mês

Funções/CTEs utilizadas:
- CTE_Base: Realiza CAST dos tipos de dados para DECIMAL apropriado
- AVG() OVER: Calcula médias móveis com window functions
- PARTITION BY: Agrupa por assessor para cálculos individuais
- ROWS BETWEEN: Define janelas móveis de 3, 6 e 12 meses

Pré-requisitos:
- Tabela bronze.xp_iea deve existir e estar atualizada
- Dados devem estar ordenados por ano_mes para cálculos corretos
- Assessores devem ter histórico suficiente para médias móveis
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
CREATE   VIEW [silver].[vw_fact_indice_esforco_assessor]
AS
WITH CTE_Base AS (
    SELECT 
        ano_mes,
        cod_assessor,
        -- Métricas principais
        CAST(iea_final AS DECIMAL(18,8)) AS indice_esforco_assessor,
        CAST(esforco_prospeccao AS DECIMAL(18,8)) AS esforco_prospeccao,
        CAST(esforco_relacionamento AS DECIMAL(18,8)) AS esforco_relacionamento,
        
        -- Métricas de prospecção
        CAST(atingimento_lead_starts AS DECIMAL(18,8)) AS prospeccao_atingimento_lead_starts,
        CAST(atingimento_habilitacoes AS DECIMAL(18,8)) AS prospeccao_atingimento_habilitacoes,
        CAST(atingimento_conversao AS DECIMAL(18,8)) AS prospeccao_atingimento_conversao,
        CAST(atingimento_carteiras_simuladas_novos AS DECIMAL(18,8)) AS prospeccao_atingimento_carteiras_simuladas_novos,
        captacao_de_novos_clientes_por_aai AS prospeccao_captacao_de_novos_clientes_por_aai,
        
        -- Métricas de relacionamento
        CAST(atingimento_contas_aportarem AS DECIMAL(18,8)) AS relacionamento_atingimento_contas_aportarem,
        CAST(atingimento_ordens_enviadas AS DECIMAL(18,8)) AS relacionamento_atingimento_ordens_enviadas,
        CAST(atingimento_contas_acessadas_hub AS DECIMAL(18,8)) AS relacionamento_atingimento_contas_acessadas_hub,
        captacao_da_base AS relacionamento_captacao_da_base
    FROM bronze.xp_iea
)
SELECT 
    ano_mes,
    cod_assessor,
    esforco_prospeccao,
    esforco_relacionamento,
    indice_esforco_assessor,
    
    -- Média simples dos últimos 3 meses
    CAST(
        AVG(indice_esforco_assessor) OVER (
            PARTITION BY cod_assessor 
            ORDER BY ano_mes 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS DECIMAL(18,8)
    ) AS indice_esforco_assessor_acum_3_meses,
    
    -- Média simples dos últimos 6 meses
    CAST(
        AVG(indice_esforco_assessor) OVER (
            PARTITION BY cod_assessor 
            ORDER BY ano_mes 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS DECIMAL(18,8)
    ) AS indice_esforco_assessor_acum_6_meses,
    
    -- Média simples dos últimos 12 meses
    CAST(
        AVG(indice_esforco_assessor) OVER (
            PARTITION BY cod_assessor 
            ORDER BY ano_mes 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS DECIMAL(18,8)
    ) AS indice_esforco_assessor_acum_12_meses,
    
    -- Métricas de prospecção
    prospeccao_atingimento_carteiras_simuladas_novos,
    prospeccao_atingimento_conversao,
    prospeccao_atingimento_habilitacoes,
    prospeccao_atingimento_lead_starts,
    prospeccao_captacao_de_novos_clientes_por_aai,
    
    -- Métricas de relacionamento
    relacionamento_atingimento_contas_acessadas_hub,
    relacionamento_atingimento_contas_aportarem,
    relacionamento_atingimento_ordens_enviadas,
    relacionamento_captacao_da_base
    
FROM CTE_Base;
GO

-- ==============================================================================
-- 7. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================
/*
-- Query para verificar médias móveis
SELECT 
    cod_assessor,
    ano_mes,
    indice_esforco_assessor,
    indice_esforco_assessor_acum_3_meses,
    indice_esforco_assessor_acum_6_meses,
    indice_esforco_assessor_acum_12_meses,
    -- Verifica se médias estão sendo calculadas corretamente
    CASE 
        WHEN indice_esforco_assessor_acum_3_meses IS NULL THEN 'Sem histórico'
        WHEN indice_esforco_assessor > indice_esforco_assessor_acum_3_meses THEN 'Acima da média'
        WHEN indice_esforco_assessor < indice_esforco_assessor_acum_3_meses THEN 'Abaixo da média'
        ELSE 'Na média'
    END as comparacao_3m
FROM silver.vw_fact_indice_esforco_assessor
WHERE cod_assessor = 'AAI001'
ORDER BY ano_mes DESC;

-- Query para verificar distribuição de esforço
SELECT 
    ano_mes,
    AVG(esforco_prospeccao) as media_prospeccao,
    AVG(esforco_relacionamento) as media_relacionamento,
    AVG(indice_esforco_assessor) as media_iea,
    COUNT(DISTINCT cod_assessor) as qtd_assessores
FROM silver.vw_fact_indice_esforco_assessor
GROUP BY ano_mes
ORDER BY ano_mes DESC;

-- Query para validar cálculo de janelas móveis
WITH Validacao AS (
    SELECT 
        cod_assessor,
        ano_mes,
        indice_esforco_assessor,
        indice_esforco_assessor_acum_3_meses,
        COUNT(*) OVER (PARTITION BY cod_assessor ORDER BY ano_mes ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as qtd_meses_3m,
        COUNT(*) OVER (PARTITION BY cod_assessor ORDER BY ano_mes ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) as qtd_meses_6m,
        COUNT(*) OVER (PARTITION BY cod_assessor ORDER BY ano_mes ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as qtd_meses_12m
    FROM silver.vw_fact_indice_esforco_assessor
)
SELECT * FROM Validacao WHERE cod_assessor = 'AAI001' ORDER BY ano_mes;
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
- View não persiste dados, calcula em tempo real
- Médias móveis são simples (não ponderadas)
- CAST para DECIMAL(18,8) garante precisão em cálculos
- Window functions usam ROWS BETWEEN para janelas móveis
- Performance depende do volume de dados na bronze.xp_iea

Cálculo das médias móveis:
- 3 meses: Média do mês atual + 2 meses anteriores
- 6 meses: Média do mês atual + 5 meses anteriores
- 12 meses: Média do mês atual + 11 meses anteriores

Limitações:
- Assessores com histórico < 3 meses terão média parcial
- Não há tratamento para gaps (meses faltantes)
- Assume que dados estão completos e consistentes

Recomendações:
1. Criar índice em bronze.xp_iea (cod_assessor, ano_mes)
2. Considerar materializar view se performance for crítica
3. Implementar tratamento para assessores com histórico incompleto
4. Adicionar validações de qualidade na CTE_Base
5. Considerar médias ponderadas se necessário

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
