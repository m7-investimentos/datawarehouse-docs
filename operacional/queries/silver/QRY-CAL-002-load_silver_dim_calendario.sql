-- ==============================================================================
-- QRY-CAL-002-load_silver_dim_calendario
-- ==============================================================================
-- Tipo: DML - INSERT
-- Versão: 1.0.0
-- Última atualização: 2024-11-28
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [carga, dimensão, calendário, temporal, silver]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Script de carga inicial da tabela dimensão calendário (silver.dim_calendario).
Popula a tabela com datas de 2023-01-01 até 2025-12-31, incluindo identificação
de feriados nacionais brasileiros e classificação de dias úteis.

Casos de uso:
- Carga inicial da dimensão calendário
- Reprocessamento completo do calendário
- Extensão do período coberto (ajustar datas no CTE)

Frequência de execução: Única ou quando necessário estender período
Tempo médio de execução: < 5 segundos
Volume esperado de linhas: ~1.095 registros (3 anos)
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros configuráveis no código:

@data_inicio     DATE        -- Data inicial do calendário (hardcoded: 2023-01-01)
@data_fim        DATE        -- Data final do calendário (hardcoded: 2025-12-31)

Exemplo de modificação:
Alterar as datas diretamente no CTE_Datas para estender o período
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Dados inseridos em silver.dim_calendario:

| Coluna              | Tipo         | Descrição                           | Exemplo           |
|---------------------|--------------|-------------------------------------|-------------------|
| data_ref            | DATE         | Data de referência                  | 2024-03-15        |
| dia                 | TINYINT      | Dia do mês                          | 15                |
| mes                 | SMALLINT     | Mês do ano                          | 3                 |
| ano                 | SMALLINT     | Ano                                 | 2024              |
| ano_mes             | CHAR(6)      | Ano e mês concatenados              | 202403            |
| nome_mes            | VARCHAR(20)  | Nome do mês em português            | março             |
| trimestre           | CHAR(2)      | Trimestre do ano                    | Q1                |
| numero_da_semana    | TINYINT      | Número da semana ISO                | 11                |
| dia_da_semana       | VARCHAR(20)  | Nome do dia em português            | sexta-feira       |
| dia_da_semana_num   | TINYINT      | Número do dia (1=seg, 7=dom)        | 6                 |
| tipo_dia            | VARCHAR(15)  | Classificação (Útil/Feriado/etc)   | Útil              |
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- silver.dim_calendario: Tabela destino (deve existir)

Funções/Procedures chamadas:
- DATEADD: Incremento de datas
- DATENAME: Nomes de mês e dia da semana
- DATEPART: Extração de partes da data

Pré-requisitos:
- Tabela silver.dim_calendario deve existir
- Tabela deve estar vazia ou aceitar duplicatas (considerar TRUNCATE antes)
- Permissões INSERT no schema silver
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
SET DATEFORMAT YMD;  -- Garante formato de data consistente

-- ==============================================================================
-- 6. CARGA DOS DADOS
-- ==============================================================================
WITH CTE_Datas AS (
    -- Gera sequência de datas usando recursão
    SELECT CAST('2023-01-01' AS DATE) AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt)
    FROM CTE_Datas
    WHERE dt < '2025-12-31'
)
INSERT INTO [silver].[dim_calendario]
(
    data_ref,
    dia,
    mes,
    ano,
    ano_mes,
    nome_mes,
    trimestre,
    numero_da_semana,
    dia_da_semana,
    dia_da_semana_num,
    tipo_dia,
    observacoes
)
SELECT
    dt AS data_ref,
    DAY(dt) AS dia,
    MONTH(dt) AS mes,
    YEAR(dt) AS ano,
    CONVERT(CHAR(6), YEAR(dt)*100 + MONTH(dt)) AS ano_mes,
    DATENAME(MONTH, dt) AS nome_mes,     
    CASE 
        WHEN MONTH(dt) IN (1,2,3)    THEN 'Q1'
        WHEN MONTH(dt) IN (4,5,6)    THEN 'Q2'
        WHEN MONTH(dt) IN (7,8,9)    THEN 'Q3'
        WHEN MONTH(dt) IN (10,11,12) THEN 'Q4'
    END AS trimestre,
    DATEPART(WEEK, dt) AS numero_da_semana,
    DATENAME(WEEKDAY, dt) AS dia_da_semana,
    -- Conversão: SQL Server considera domingo=1, ajustamos para segunda=1, domingo=7
    CASE DATEPART(WEEKDAY, dt)
        WHEN 1 THEN 7  -- Domingo
        ELSE DATEPART(WEEKDAY, dt) - 1
    END AS dia_da_semana_num,
    -- Classificação do tipo de dia
    CASE
        WHEN dt IN (
            -- Feriados Nacionais 2025
            '2025-01-01',  -- Confraternização Universal
            '2025-03-03',  -- Carnaval (segunda)
            '2025-03-04',  -- Carnaval (terça)
            '2025-04-18',  -- Sexta-feira Santa
            '2025-04-21',  -- Tiradentes
            '2025-05-01',  -- Dia do Trabalho
            '2025-09-07',  -- Independência do Brasil
            '2025-10-12',  -- Nossa Senhora Aparecida
            '2025-11-02',  -- Finados
            '2025-11-15',  -- Proclamação da República
            '2025-12-25'   -- Natal
        )
            THEN 'Feriado'
        WHEN DATENAME(WEEKDAY, dt) = 'Saturday' THEN 'Sábado'
        WHEN DATENAME(WEEKDAY, dt) = 'Sunday'   THEN 'Domingo'
        ELSE 'Útil'
    END AS tipo_dia,
    -- Observações para feriados
    CASE dt
        WHEN '2025-01-01' THEN 'Confraternização Universal'
        WHEN '2025-03-03' THEN 'Carnaval'
        WHEN '2025-03-04' THEN 'Carnaval'
        WHEN '2025-04-18' THEN 'Sexta-feira Santa'
        WHEN '2025-04-21' THEN 'Tiradentes'
        WHEN '2025-05-01' THEN 'Dia do Trabalho'
        WHEN '2025-09-07' THEN 'Independência do Brasil'
        WHEN '2025-10-12' THEN 'Nossa Senhora Aparecida'
        WHEN '2025-11-02' THEN 'Finados'
        WHEN '2025-11-15' THEN 'Proclamação da República'
        WHEN '2025-12-25' THEN 'Natal'
        ELSE NULL
    END AS observacoes
FROM CTE_Datas
OPTION (MAXRECURSION 0);  -- Remove limite de recursão

-- ==============================================================================
-- 7. VALIDAÇÕES PÓS-CARGA
-- ==============================================================================
/*
-- Verificar total de registros inseridos
SELECT COUNT(*) AS total_registros FROM silver.dim_calendario;

-- Verificar distribuição por tipo de dia
SELECT 
    tipo_dia, 
    COUNT(*) AS quantidade,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS percentual
FROM silver.dim_calendario
GROUP BY tipo_dia
ORDER BY quantidade DESC;

-- Verificar anos cobertos
SELECT 
    ano,
    COUNT(*) AS dias_no_ano,
    COUNT(DISTINCT mes) AS meses_no_ano
FROM silver.dim_calendario
GROUP BY ano
ORDER BY ano;
*/

-- ==============================================================================
-- 8. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2024-11-28 | [Nome]         | Criação inicial do script de carga

*/

-- ==============================================================================
-- 9. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Feriados móveis (Carnaval, Corpus Christi) precisam ajuste manual por ano
- Script considera apenas feriados nacionais, não estaduais/municipais
- MAXRECURSION 0 é necessário para períodos > 100 dias
- Considerar TRUNCATE TABLE antes da carga para evitar duplicatas

Troubleshooting comum:
1. Erro de recursão máxima: Verificar OPTION (MAXRECURSION 0)
2. Duplicação de chaves: Executar TRUNCATE TABLE silver.dim_calendario antes
3. Nomes em inglês: Verificar configuração de idioma do SQL Server

Para adicionar novos anos:
1. Ajustar data final no CTE_Datas
2. Adicionar feriados do novo ano no CASE de tipo_dia
3. Executar script completo

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/