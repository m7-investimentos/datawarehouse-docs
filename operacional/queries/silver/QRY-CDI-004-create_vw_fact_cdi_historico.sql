-- ==============================================================================
-- QRY-CDI-004-create_vw_fact_cdi_historico
-- ==============================================================================
-- Tipo: DDL - CREATE VIEW
-- Versão: 1.0.0
-- Última atualização: 2024-11-28
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [view, cdi, taxa, histórico, janela móvel, silver]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: View que calcula taxas CDI acumuladas em diferentes períodos (mensal, 
trimestral, semestral, anual e janelas móveis de 3, 6 e 12 meses). Utiliza 
cálculo composto considerando os dias úteis dentro de cada período.

Casos de uso:
- Análise de rentabilidade comparada ao CDI
- Cálculo de benchmarks para produtos financeiros
- Relatórios de performance de investimentos
- Análises de séries temporais de taxas

Frequência de execução: Sob demanda
Tempo médio de execução: ~2-3 segundos
Volume esperado de linhas: ~7.000 registros (dados diários)
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Não aplicável - View sem parâmetros

Para filtrar períodos específicos, usar WHERE na consulta:
SELECT * FROM silver.vw_fact_cdi_historico 
WHERE data_ref BETWEEN '2024-01-01' AND '2024-12-31'
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Colunas retornadas:

| Coluna              | Tipo          | Descrição                           | Exemplo           |
|---------------------|---------------|-------------------------------------|-------------------|
| data_ref            | DATE          | Data de referência                  | 2024-03-15        |
| ano_mes             | VARCHAR(6)    | Ano e mês (YYYYMM)                  | 202403            |
| ano                 | INT           | Ano                                 | 2024              |
| mes_num             | INT           | Número do mês                       | 3                 |
| trimestre           | CHAR(2)       | Trimestre (Q1-Q4)                   | Q1                |
| semestre            | CHAR(2)       | Semestre (S1-S2)                    | S1                |
| taxa_cdi_dia        | DECIMAL(18,8) | Taxa CDI do dia (decimal)           | 0.00039890        |
| taxa_cdi_mes        | DECIMAL(18,8) | Taxa acumulada do mês               | 0.00850000        |
| taxa_cdi_3_meses    | DECIMAL(18,8) | Taxa acumulada 3 meses (móvel)      | 0.02550000        |
| taxa_cdi_6_meses    | DECIMAL(18,8) | Taxa acumulada 6 meses (móvel)      | 0.05100000        |
| taxa_cdi_12_meses   | DECIMAL(18,8) | Taxa acumulada 12 meses (móvel)     | 0.10250000        |
| taxa_cdi_trimestre  | DECIMAL(18,8) | Taxa acumulada do trimestre         | 0.02550000        |
| taxa_cdi_semestre   | DECIMAL(18,8) | Taxa acumulada do semestre          | 0.05100000        |
| taxa_cdi_ano        | DECIMAL(18,8) | Taxa acumulada do ano               | 0.10250000        |

Ordenação padrão: data_ref ASC
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- bronze.bc_cdi_historico: Tabela fonte com taxas CDI diárias do Banco Central

Funções/Procedures chamadas:
- LAG(): Para acessar valores anteriores no cálculo acumulado
- ROW_NUMBER(): Para numerar dias dentro do mês
- COALESCE(): Tratamento de valores nulos

Pré-requisitos:
- Tabela bronze.bc_cdi_historico deve estar atualizada
- Índice em bronze.bc_cdi_historico.data_ref para performance
- Dados devem estar completos (sem gaps de dias úteis)
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
CREATE VIEW [silver].[vw_fact_cdi_historico]
AS
WITH 
-- -----------------------------------------------------------------------------
-- CTE: dias_numerados
-- Descrição: Numera os dias dentro de cada mês para controle do cálculo
-- -----------------------------------------------------------------------------
CTE_dias_numerados AS (
    SELECT 
        data_ref,
        YEAR(data_ref) AS ano,
        MONTH(data_ref) AS mes_num,
        taxa_cdi / 100.0 AS taxa_cdi_dia,  -- Converte percentual para decimal
        ROW_NUMBER() OVER (PARTITION BY YEAR(data_ref), MONTH(data_ref) ORDER BY data_ref) AS dia_no_mes
    FROM [bronze].[bc_cdi_historico]
),

-- -----------------------------------------------------------------------------
-- CTE: com_lags_diarios
-- Descrição: Cria LAGs para cada dia do mês (até 22 dias úteis)
-- -----------------------------------------------------------------------------
CTE_com_lags_diarios AS (
    SELECT 
        *,
        LAG(taxa_cdi_dia, 1) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag1,
        LAG(taxa_cdi_dia, 2) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag2,
        LAG(taxa_cdi_dia, 3) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag3,
        LAG(taxa_cdi_dia, 4) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag4,
        LAG(taxa_cdi_dia, 5) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag5,
        LAG(taxa_cdi_dia, 6) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag6,
        LAG(taxa_cdi_dia, 7) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag7,
        LAG(taxa_cdi_dia, 8) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag8,
        LAG(taxa_cdi_dia, 9) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag9,
        LAG(taxa_cdi_dia, 10) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag10,
        LAG(taxa_cdi_dia, 11) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag11,
        LAG(taxa_cdi_dia, 12) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag12,
        LAG(taxa_cdi_dia, 13) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag13,
        LAG(taxa_cdi_dia, 14) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag14,
        LAG(taxa_cdi_dia, 15) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag15,
        LAG(taxa_cdi_dia, 16) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag16,
        LAG(taxa_cdi_dia, 17) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag17,
        LAG(taxa_cdi_dia, 18) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag18,
        LAG(taxa_cdi_dia, 19) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag19,
        LAG(taxa_cdi_dia, 20) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag20,
        LAG(taxa_cdi_dia, 21) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag21,
        LAG(taxa_cdi_dia, 22) OVER (PARTITION BY ano, mes_num ORDER BY data_ref) AS lag22
    FROM CTE_dias_numerados
),

-- -----------------------------------------------------------------------------
-- CTE: dias_para_mes
-- Descrição: Calcula taxa acumulada do mês usando produto dos fatores diários
-- -----------------------------------------------------------------------------
CTE_dias_para_mes AS (
    SELECT 
        data_ref,
        ano,
        mes_num,
        taxa_cdi_dia,
        
        -- Cálculo acumulado baseado no dia do mês
        CASE dia_no_mes
            WHEN 1 THEN taxa_cdi_dia
            WHEN 2 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0))) - 1
            WHEN 3 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0))) - 1
            WHEN 4 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0))) - 1
            WHEN 5 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0))) - 1
            WHEN 6 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0))) - 1
            WHEN 7 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0))) - 1
            WHEN 8 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0))) - 1
            WHEN 9 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0))) - 1
            WHEN 10 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0))) - 1
            WHEN 11 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0))) - 1
            WHEN 12 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0))) - 1
            WHEN 13 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0))) - 1
            WHEN 14 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0))) - 1
            WHEN 15 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0))) - 1
            WHEN 16 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0))) - 1
            WHEN 17 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0))) - 1
            WHEN 18 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0)) * (1 + COALESCE(lag17, 0))) - 1
            WHEN 19 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0)) * (1 + COALESCE(lag17, 0)) * (1 + COALESCE(lag18, 0))) - 1
            WHEN 20 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0)) * (1 + COALESCE(lag17, 0)) * (1 + COALESCE(lag18, 0)) * (1 + COALESCE(lag19, 0))) - 1
            WHEN 21 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0)) * (1 + COALESCE(lag17, 0)) * (1 + COALESCE(lag18, 0)) * (1 + COALESCE(lag19, 0)) * (1 + COALESCE(lag20, 0))) - 1
            WHEN 22 THEN ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0)) * (1 + COALESCE(lag17, 0)) * (1 + COALESCE(lag18, 0)) * (1 + COALESCE(lag19, 0)) * (1 + COALESCE(lag20, 0)) * (1 + COALESCE(lag21, 0))) - 1
            ELSE ((1 + taxa_cdi_dia) * (1 + COALESCE(lag1, 0)) * (1 + COALESCE(lag2, 0)) * (1 + COALESCE(lag3, 0)) * (1 + COALESCE(lag4, 0)) * (1 + COALESCE(lag5, 0)) * (1 + COALESCE(lag6, 0)) * (1 + COALESCE(lag7, 0)) * (1 + COALESCE(lag8, 0)) * (1 + COALESCE(lag9, 0)) * (1 + COALESCE(lag10, 0)) * (1 + COALESCE(lag11, 0)) * (1 + COALESCE(lag12, 0)) * (1 + COALESCE(lag13, 0)) * (1 + COALESCE(lag14, 0)) * (1 + COALESCE(lag15, 0)) * (1 + COALESCE(lag16, 0)) * (1 + COALESCE(lag17, 0)) * (1 + COALESCE(lag18, 0)) * (1 + COALESCE(lag19, 0)) * (1 + COALESCE(lag20, 0)) * (1 + COALESCE(lag21, 0)) * (1 + COALESCE(lag22, 0))) - 1
        END AS taxa_cdi_mes_acum
        
    FROM CTE_com_lags_diarios
),

-- -----------------------------------------------------------------------------
-- CTE: mes_final
-- Descrição: Extrai apenas o último dia de cada mês (taxa mensal completa)
-- -----------------------------------------------------------------------------
CTE_mes_final AS (
    SELECT 
        ano,
        mes_num,
        MAX(data_ref) AS ultimo_dia_mes,
        MAX(taxa_cdi_mes_acum) AS taxa_cdi_mes
    FROM CTE_dias_para_mes
    GROUP BY ano, mes_num
),

-- -----------------------------------------------------------------------------
-- CTE: base_mensal
-- Descrição: Prepara base mensal com identificadores de período
-- -----------------------------------------------------------------------------
CTE_base_mensal AS (
    SELECT 
        ano,
        mes_num,
        taxa_cdi_mes,
        
        -- Criando ano_mes no formato YYYYMM (6 dígitos)
        CAST(ano * 100 + mes_num AS INT) AS ano_mes,
        
        -- Trimestre
        CASE 
            WHEN mes_num BETWEEN 1 AND 3 THEN 'Q1'
            WHEN mes_num BETWEEN 4 AND 6 THEN 'Q2'
            WHEN mes_num BETWEEN 7 AND 9 THEN 'Q3'
            WHEN mes_num BETWEEN 10 AND 12 THEN 'Q4'
        END AS trimestre,
        
        -- Semestre
        CASE 
            WHEN mes_num BETWEEN 1 AND 6 THEN 'S1'
            WHEN mes_num BETWEEN 7 AND 12 THEN 'S2'
        END AS semestre
        
    FROM CTE_mes_final
),

-- -----------------------------------------------------------------------------
-- CTE: com_acumulados
-- Descrição: Calcula taxas acumuladas para diferentes períodos
-- -----------------------------------------------------------------------------
CTE_com_acumulados AS (
    SELECT 
        *,
        -- Taxa acumulada 3 meses (janela móvel)
        CAST(
            ((1 + COALESCE(taxa_cdi_mes, 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (ORDER BY ano_mes), 0))
            ) - 1 
        AS DECIMAL(18,8)) AS taxa_cdi_3_meses,
        
        -- Taxa acumulada 6 meses (janela móvel)
        CAST(
            ((1 + COALESCE(taxa_cdi_mes, 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (ORDER BY ano_mes), 0))
            ) - 1 
        AS DECIMAL(18,8)) AS taxa_cdi_6_meses,
        
        -- Taxa acumulada 12 meses (janela móvel)
        CAST(
            ((1 + COALESCE(taxa_cdi_mes, 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 7) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 8) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 9) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 10) OVER (ORDER BY ano_mes), 0))
             * (1 + COALESCE(LAG(taxa_cdi_mes, 11) OVER (ORDER BY ano_mes), 0))
            ) - 1 
        AS DECIMAL(18,8)) AS taxa_cdi_12_meses,
        
        -- Taxa acumulada do trimestre
        CASE 
            WHEN mes_num IN (1,4,7,10) THEN taxa_cdi_mes
            WHEN mes_num IN (2,5,8,11) THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num IN (3,6,9,12) THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
        END AS taxa_cdi_trimestre,
        
        -- Taxa acumulada do semestre
        CASE 
            WHEN mes_num IN (1,7) THEN taxa_cdi_mes
            WHEN mes_num = 2 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 3 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 4 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 5 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 6 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 8 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 9 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 10 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 11 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 12 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
        END AS taxa_cdi_semestre,
        
        -- Taxa acumulada do ano
        CASE 
            WHEN mes_num = 1 THEN taxa_cdi_mes
            WHEN mes_num = 2 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 3 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 4 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 5 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 6 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 7 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 8 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 7) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 9 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 7) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 8) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 10 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 7) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 8) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 9) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 11 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 7) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 8) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 9) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 10) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
            WHEN mes_num = 12 THEN 
                ((1 + COALESCE(taxa_cdi_mes, 0)) 
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 1) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 2) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 3) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 4) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 5) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 6) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 7) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 8) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 9) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 10) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                 * (1 + COALESCE(LAG(taxa_cdi_mes, 11) OVER (PARTITION BY ano ORDER BY mes_num), 0))
                ) - 1
        END AS taxa_cdi_ano
        
    FROM CTE_base_mensal
)

-- ==============================================================================
-- 7. QUERY PRINCIPAL
-- ==============================================================================
-- JOIN final para trazer os dados diários junto com os cálculos mensais
SELECT 
    d.data_ref,
    FORMAT(d.data_ref, 'yyyyMM') AS ano_mes,
    YEAR(d.data_ref) AS ano,
    MONTH(d.data_ref) AS mes_num,
    m.trimestre,
    m.semestre,
    
    -- Taxa do dia
    CAST(d.taxa_cdi / 100.0 AS DECIMAL(18,8)) AS taxa_cdi_dia,
    
    -- Taxas acumuladas (todas baseadas nos cálculos mensais)
    CAST(m.taxa_cdi_mes AS DECIMAL(18,8)) AS taxa_cdi_mes,
    CAST(m.taxa_cdi_3_meses AS DECIMAL(18,8)) AS taxa_cdi_3_meses,
    CAST(m.taxa_cdi_6_meses AS DECIMAL(18,8)) AS taxa_cdi_6_meses,
    CAST(m.taxa_cdi_12_meses AS DECIMAL(18,8)) AS taxa_cdi_12_meses,
    CAST(m.taxa_cdi_trimestre AS DECIMAL(18,8)) AS taxa_cdi_trimestre,
    CAST(m.taxa_cdi_semestre AS DECIMAL(18,8)) AS taxa_cdi_semestre,
    CAST(m.taxa_cdi_ano AS DECIMAL(18,8)) AS taxa_cdi_ano
    
FROM [bronze].[bc_cdi_historico] d
INNER JOIN CTE_com_acumulados m
    ON YEAR(d.data_ref) = m.ano 
    AND MONTH(d.data_ref) = m.mes_num
GO

-- ==============================================================================
-- 8. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================
/*
-- Query para verificar últimas taxas CDI
SELECT TOP 10
    data_ref,
    taxa_cdi_dia,
    taxa_cdi_mes,
    taxa_cdi_12_meses
FROM silver.vw_fact_cdi_historico
ORDER BY data_ref DESC;

-- Query para validar cálculo acumulado mensal
SELECT 
    ano_mes,
    COUNT(*) as dias_uteis,
    MIN(data_ref) as primeiro_dia,
    MAX(data_ref) as ultimo_dia,
    MAX(taxa_cdi_mes) as taxa_mes_acumulada
FROM silver.vw_fact_cdi_historico
WHERE data_ref >= '2024-01-01'
GROUP BY ano_mes
ORDER BY ano_mes DESC;

-- Query para comparar janelas móveis
SELECT 
    data_ref,
    taxa_cdi_3_meses,
    taxa_cdi_6_meses,
    taxa_cdi_12_meses
FROM silver.vw_fact_cdi_historico
WHERE DAY(data_ref) = DAY(EOMONTH(data_ref))  -- Último dia do mês
    AND data_ref >= '2023-01-01'
ORDER BY data_ref DESC;
*/

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2024-11-28 | [Nome]         | Criação inicial da view

*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- A view utiliza cálculo composto (juros sobre juros) para taxas acumuladas
- Considera apenas dias úteis (dados presentes na tabela bronze)
- LAG functions limitadas a 22 dias úteis por mês (máximo histórico)
- Performance pode ser impactada em consultas de períodos muito longos
- Janelas móveis (3, 6, 12 meses) calculadas sobre meses completos

Troubleshooting comum:
1. Valores NULL em taxas acumuladas: Verificar se há dados suficientes no período anterior
2. Performance lenta: Criar índice em bronze.bc_cdi_historico.data_ref
3. Divergência de cálculo: Verificar se todos os dias úteis estão presentes na bronze

Fórmula de cálculo:
Taxa Acumulada = [(1 + Taxa1) × (1 + Taxa2) × ... × (1 + TaxaN)] - 1

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/