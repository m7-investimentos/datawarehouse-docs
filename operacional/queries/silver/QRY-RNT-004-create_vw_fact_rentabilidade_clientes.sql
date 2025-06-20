-- ==============================================================================
-- QRY-RNT-004-create_vw_fact_rentabilidade_clientes
-- ==============================================================================
-- Tipo: VIEW
-- Versão: 1.0.0
-- Última atualização: 2024-11-28
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [view, rentabilidade, performance, silver]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: View responsável por processar e calcular todas as métricas de 
rentabilidade dos clientes, incluindo rentabilidades acumuladas em múltiplas
janelas temporais. Realiza conversão de percentual para decimal e implementa
cálculos complexos de juros compostos para períodos móveis e fiscais.

Casos de uso:
- Cálculo automático de rentabilidades acumuladas (3, 6, 12 meses)
- Cálculo de rentabilidades por período fiscal (trimestre, semestre, ano)
- Deduplicatação de dados usando data_relatorio mais recente
- Conversão de valores percentuais para decimais
- Base para carga da tabela fact_rentabilidade_clientes

Frequência de execução: Consumida diariamente pela procedure de carga
Tempo médio de execução: 15-30 segundos (depende do volume)
Volume esperado de linhas: ~50.000 registros (clientes ativos x meses)
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Não aplicável - View não recebe parâmetros
Filtros devem ser aplicados nas queries que consomem a view
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Colunas retornadas:

| Coluna                             | Tipo         | Descrição                                      | Exemplo      |
|------------------------------------|--------------|------------------------------------------------|--------------|  
| conta_xp_cliente                   | INT          | Código único do cliente                        | 123456       |
| ano_mes                            | INT          | Período YYYYMM                                 | 202401       |
| ano                                | INT          | Ano de referência                              | 2024         |
| semestre                           | VARCHAR(2)   | Semestre (S1, S2)                              | 'S1'         |
| trimestre                          | VARCHAR(2)   | Trimestre (Q1-Q4)                              | 'Q1'         |
| mes_num                            | INT          | Número do mês (1-12)                          | 3            |
| mes                                | VARCHAR(5)   | Nome abreviado do mês                          | 'Mar'        |
| rentabilidade                      | DECIMAL(18,8)| Rentabilidade mensal em decimal                | 0.01580000   |
| rentabilidade_acumulada_3_meses    | DECIMAL(18,8)| Rentab. acum. 3 meses (janela móvel)          | 0.04852300   |
| rentabilidade_acumulada_6_meses    | DECIMAL(18,8)| Rentab. acum. 6 meses (janela móvel)          | 0.10234500   |
| rentabilidade_acumulada_12_meses   | DECIMAL(18,8)| Rentab. acum. 12 meses (janela móvel)         | 0.22457800   |
| rentabilidade_acumulada_trimestre  | DECIMAL(18,8)| Rentab. acum. trimestre atual                  | 0.04852300   |
| rentabilidade_acumulada_semestre   | DECIMAL(18,8)| Rentab. acum. semestre atual                   | 0.10234500   |
| rentabilidade_acumulada_ano        | DECIMAL(18,8)| Rentab. acum. ano atual (YTD)                  | 0.10234500   |

Ordenação padrão: Não definida (recomenda-se ORDER BY conta_xp_cliente, ano_mes)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- bronze.xperformance_rentabilidade_cliente: Fonte de dados brutos de rentabilidade

Funções/Procedures chamadas:
- LAG(): Window function para acessar valores de meses anteriores
- ROW_NUMBER(): Para deduplicação por data_relatorio mais recente
- COALESCE(): Tratamento de valores NULL

Pré-requisitos:
- Tabela bronze deve estar atualizada
- Índices recomendados: (conta_xp_cliente, ano, mes_num, data_relatorio)
- Espaço em tempdb para operações de window functions
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
CREATE   VIEW [silver].[vw_fact_rentabilidade_clientes]
AS
-- ==============================================================================
-- 7. CTEs (COMMON TABLE EXPRESSIONS)
-- ==============================================================================
WITH CTE_prioridade AS (
    -- Primeiro, vamos identificar qual é a data_relatorio mais recente para cada ano_mes
    SELECT 
        conta_xp_cliente,
        ano,
        mes_num,
        mes,
        portfolio_rentabilidade,
        acumulado_ano,
        data_relatorio,
        data_carga,
        -- Ranking por data_relatorio mais recente para cada cliente/ano/mes
        ROW_NUMBER() OVER (
            PARTITION BY conta_xp_cliente, ano, mes_num 
            ORDER BY data_relatorio DESC, data_carga DESC
        ) AS rn
    FROM [M7Medallion].[bronze].[xperformance_rentabilidade_cliente]
),
CTE_base AS (
    SELECT 
        -- Identificação do cliente
        conta_xp_cliente,
        
        -- Dimensões temporais básicas
        ano,
        mes_num,
        mes,
        
        -- Campos calculados de período
        CAST(ano * 100 + mes_num AS INT) AS ano_mes,  -- Formato YYYYMM (6 dígitos)
        CASE 
            WHEN mes_num BETWEEN 1 AND 6 THEN 'S1'
            WHEN mes_num BETWEEN 7 AND 12 THEN 'S2'
        END AS semestre,
        CASE 
            WHEN mes_num BETWEEN 1 AND 3 THEN 'Q1'
            WHEN mes_num BETWEEN 4 AND 6 THEN 'Q2'
            WHEN mes_num BETWEEN 7 AND 9 THEN 'Q3'
            WHEN mes_num BETWEEN 10 AND 12 THEN 'Q4'
        END AS trimestre,
        
        -- Métricas de rentabilidade (convertendo de percentual para decimal)
        CAST(portfolio_rentabilidade / 100.0 AS DECIMAL(18,8)) AS rentabilidade,
        
        -- Metadados para auditoria
        data_relatorio,
        data_carga
        
    FROM CTE_prioridade
    WHERE rn = 1  -- Apenas o registro mais recente para cada cliente/ano/mes
),
CTE_com_acumulados AS (
    SELECT 
        *,
        -- Rentabilidade acumulada do trimestre (produto dos meses do trimestre atual)
        CASE 
            WHEN mes_num IN (1,4,7,10) THEN rentabilidade
            WHEN mes_num IN (2,5,8,11) THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num IN (3,6,9,12) THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
        END AS rent_acum_trimestre,
        
        -- Rentabilidade acumulada do semestre (produto dos meses do semestre atual)
        CASE 
            WHEN mes_num IN (1,7) THEN rentabilidade
            WHEN mes_num = 2 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 3 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 4 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 5 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 6 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            -- Repete a lógica para o segundo semestre
            WHEN mes_num = 8 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 9 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 10 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 11 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
            WHEN mes_num = 12 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
                ) - 1
        END AS rent_acum_semestre
    FROM CTE_base
)
SELECT 
    conta_xp_cliente,
    ano_mes,
    ano,
    semestre,
    trimestre,
    mes_num,
    mes,
    rentabilidade,
    
    -- Rentabilidade acumulada 3 meses (janela móvel)
    CAST(
        ((1 + COALESCE(rentabilidade, 0))
         * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
        ) - 1 
    AS DECIMAL(18,8)) AS rentabilidade_acumulada_3_meses,
    
    -- Rentabilidade acumulada 6 meses (janela móvel)
    CAST(
        ((1 + COALESCE(rentabilidade, 0))
         * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
        ) - 1 
    AS DECIMAL(18,8)) AS rentabilidade_acumulada_6_meses,
    
    -- Rentabilidade acumulada 12 meses (janela móvel)
    CAST(
        ((1 + COALESCE(rentabilidade, 0))
         * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 7) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 8) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 9) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 10) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
         * (1 + COALESCE(LAG(rentabilidade, 11) OVER (PARTITION BY conta_xp_cliente ORDER BY ano_mes), 0))
        ) - 1 
    AS DECIMAL(18,8)) AS rentabilidade_acumulada_12_meses,
    
    -- Rentabilidades acumuladas de período fiscal
    CAST(rent_acum_trimestre AS DECIMAL(18,8)) AS rentabilidade_acumulada_trimestre,
    CAST(rent_acum_semestre AS DECIMAL(18,8)) AS rentabilidade_acumulada_semestre,
    
    -- Rentabilidade acumulada do ano (calculada, não da bronze)
    CAST(
        CASE 
            WHEN mes_num = 1 THEN rentabilidade
            WHEN mes_num = 2 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 3 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 4 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 5 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 6 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 7 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 8 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 7) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 9 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 7) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 8) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 10 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 7) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 8) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 9) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 11 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 7) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 8) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 9) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 10) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 12 THEN 
                ((1 + COALESCE(rentabilidade, 0)) 
                 * (1 + COALESCE(LAG(rentabilidade, 1) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 2) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 3) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 4) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 5) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 6) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 7) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 8) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 9) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 10) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(rentabilidade, 11) OVER (PARTITION BY conta_xp_cliente, ano ORDER BY mes_num), 0))
                ) - 1
        END
    AS DECIMAL(18,8)) AS rentabilidade_acumulada_ano
    
FROM CTE_com_acumulados
GO

-- ==============================================================================
-- 8. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2024-11-28 | [Nome]         | Criação inicial da view

*/

-- ==============================================================================
-- 9. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- CONVERSÃO DECIMAL: portfolio_rentabilidade / 100.0 para converter de % para decimal
- DEDUPLICATAÇÃO: ROW_NUMBER() garante apenas o registro mais recente por cliente/mês
- JUROS COMPOSTOS: Fórmula ((1+r1)*(1+r2)...(1+rn))-1 aplicada consistentemente
- LAG FUNCTIONS: Usadas extensivamente para acessar meses anteriores
- COALESCE: Tratamento de NULL com default 0 para evitar quebra nos cálculos

Diferenças entre acumulados:
- Janelas móveis (3,6,12): Sempre consideram N meses anteriores + mês atual
- Períodos fiscais (Q,S,Y): Resetam no início do período fiscal
- YTD (Year-to-Date): Calculado dinamicamente, não confia no campo da bronze

Performance:
- CTEs múltiplas podem impactar performance em grandes volumes
- Considerar materializar em tabela física se necessário
- Window functions requerem ordenação e podem usar tempdb

Troubleshooting comum:
1. Rentabilidades zeradas: Verificar conversão de % para decimal
2. Acumulados incorretos: Validar particionamento das window functions
3. Duplicações: Verificar lógica de ROW_NUMBER() e data_relatorio
4. Performance lenta: Analisar plano de execução e criar índices

Limitações conhecidas:
- Máximo de 12 meses para janela móvel (hardcoded)
- Assume que todos os meses têm dados (gaps podem causar distorções)
- Código extenso devido ao CASE statement para cada mês

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
