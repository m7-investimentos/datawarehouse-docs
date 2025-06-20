-- ==============================================================================
-- QRY-RNT-006-create_vw_rentabilidade_assessor
-- ==============================================================================
-- Tipo: CREATE VIEW
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [equipe.dados@m7investimentos.com.br]
-- Revisor: [revisor@m7investimentos.com.br]
-- Tags: [rentabilidade, assessor, consolidacao, cdi, gold]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: View que consolida métricas de rentabilidade por assessor, comparando
a performance das carteiras dos clientes PF (patrimônio >= R$ 300k) com o CDI.
Inclui métricas para diferentes períodos: mensal, 3m, 6m, 12m, trimestre, semestre e ano.

Casos de uso:
- Base para tabela materializada gold.rentabilidade_assessor
- Consultas ad-hoc para análise de performance vs CDI
- Validação de cálculos antes da materialização
- Fonte para dashboards de rentabilidade em tempo real

Frequência de consulta: Várias vezes ao dia
Tempo médio de execução: 1-3 minutos
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
| Coluna                                    | Tipo          | Descrição                                           |
|-------------------------------------------|---------------|-----------------------------------------------------|
| ano_mes                                   | INT           | Período no formato AAAAMM                          |
| ano                                       | INT           | Ano de referência                                  |
| mes                                       | INT           | Mês de referência (1-12)                          |
| nome_mes                                  | VARCHAR(20)   | Nome do mês em português                          |
| trimestre                                 | VARCHAR(2)    | Trimestre (Q1-Q4)                                   |
| semestre                                  | VARCHAR(2)    | Semestre (S1-S2)                                    |
| cod_assessor                              | VARCHAR(20)   | Código do assessor                                 |
| codigo_crm_assessor                       | VARCHAR(20)   | ID do assessor no CRM                              |
| nome_assessor                             | VARCHAR(200)  | Nome completo do assessor                          |
| nivel_assessor                            | VARCHAR(50)   | Nível hierárquico                                  |
| estrutura_id                              | INT           | ID da estrutura organizacional                     |
| estrutura_nome                            | VARCHAR(100)  | Nome da estrutura                                  |
| qtd_clientes_300k_mais                    | INT           | Total de clientes PF >= 300k                       |
| qtd_clientes_acima_cdi                    | INT           | Clientes com rentabilidade > CDI                   |
| qtd_clientes_faixa_80_cdi                 | INT           | Clientes com rentabilidade >= 80% CDI              |
| qtd_clientes_faixa_50_cdi                 | INT           | Clientes com rentabilidade >= 50% CDI              |
| qtd_clientes_rentabilidade_positiva       | INT           | Clientes com rentabilidade > 0%                    |
| perc_clientes_acima_cdi                   | DECIMAL(12,6) | % de clientes acima do CDI                         |
| [métricas para outros períodos]           | Vários        | Métricas similares para 3m, 6m, 12m, etc          |

Observação: A view retorna métricas completas para todos os períodos,
mas a tabela materializada armazena apenas as métricas mensais.
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- silver.fact_rentabilidade_clientes: Rentabilidade por cliente/período
- silver.fact_cdi_historico: Histórico do CDI por período
- silver.fact_patrimonio: Patrimônio dos clientes (filtro >= 300k)
- silver.fact_cliente_perfil_historico: Histórico do perfil do cliente
- silver.dim_clientes: Cadastro de clientes (filtro PF)
- silver.dim_pessoas: Cadastro de pessoas (assessores)
- silver.fact_estrutura_pessoas: Histórico de estrutura
- silver.dim_estruturas: Cadastro de estruturas

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
-- 6. CRIAÇÃO DA VIEW
-- ==============================================================================
CREATE   VIEW [gold].[vw_rentabilidade_assessor] AS
SELECT 
    r.ano_mes,
    r.ano,
    r.mes_num as mes,
    r.mes as nome_mes,
    r.trimestre,
    r.semestre,
    h.cod_assessor,
    p.crm_id as codigo_crm_assessor,
    p.nome_pessoa as nome_assessor,
    p.assessor_nivel as nivel_assessor,
    e.id_estrutura as estrutura_id,
    est.nome_estrutura as estrutura_nome,
    
    -- Quantidade total de clientes 300k+
    COUNT(DISTINCT r.conta_xp_cliente) as qtd_clientes_300k_mais,
    
    -- ===== MÉTRICAS MENSAIS (Original) =====
    -- Clientes acima do CDI (rentabilidade > 100% CDI)
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade > c.taxa_cdi_mes 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi,
    
    -- Clientes com rentabilidade >= 80% do CDI
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade >= (c.taxa_cdi_mes * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi,
    
    -- Clientes com rentabilidade >= 50% do CDI
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade >= (c.taxa_cdi_mes * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi,
    
    -- Clientes com rentabilidade positiva
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva,
    
    -- Percentual de clientes acima do CDI
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade > c.taxa_cdi_mes THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi,

    -- ===== MÉTRICAS 3 MESES =====
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_3_meses > c.taxa_cdi_3_meses 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi_3m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_3_meses >= (c.taxa_cdi_3_meses * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi_3m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_3_meses >= (c.taxa_cdi_3_meses * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi_3m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_3_meses > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva_3m,
    
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade_acumulada_3_meses > c.taxa_cdi_3_meses THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi_3m,

    -- ===== MÉTRICAS 6 MESES =====
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_6_meses > c.taxa_cdi_6_meses 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi_6m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_6_meses >= (c.taxa_cdi_6_meses * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi_6m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_6_meses >= (c.taxa_cdi_6_meses * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi_6m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_6_meses > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva_6m,
    
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade_acumulada_6_meses > c.taxa_cdi_6_meses THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi_6m,

    -- ===== MÉTRICAS 12 MESES =====
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_12_meses > c.taxa_cdi_12_meses 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi_12m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_12_meses >= (c.taxa_cdi_12_meses * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi_12m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_12_meses >= (c.taxa_cdi_12_meses * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi_12m,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_12_meses > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva_12m,
    
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade_acumulada_12_meses > c.taxa_cdi_12_meses THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi_12m,

    -- ===== MÉTRICAS TRIMESTRE =====
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_trimestre > c.taxa_cdi_trimestre 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi_trimestre,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_trimestre >= (c.taxa_cdi_trimestre * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi_trimestre,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_trimestre >= (c.taxa_cdi_trimestre * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi_trimestre,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_trimestre > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva_trimestre,
    
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade_acumulada_trimestre > c.taxa_cdi_trimestre THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi_trimestre,

    -- ===== MÉTRICAS SEMESTRE =====
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_semestre > c.taxa_cdi_semestre 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi_semestre,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_semestre >= (c.taxa_cdi_semestre * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi_semestre,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_semestre >= (c.taxa_cdi_semestre * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi_semestre,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_semestre > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva_semestre,
    
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade_acumulada_semestre > c.taxa_cdi_semestre THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi_semestre,

    -- ===== MÉTRICAS ANO =====
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_ano > c.taxa_cdi_ano 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_acima_cdi_ano,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_ano >= (c.taxa_cdi_ano * 0.80) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_80_cdi_ano,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_ano >= (c.taxa_cdi_ano * 0.50) 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_faixa_50_cdi_ano,
    
    COUNT(DISTINCT CASE 
        WHEN r.rentabilidade_acumulada_ano > 0 
        THEN r.conta_xp_cliente 
    END) as qtd_clientes_rentabilidade_positiva_ano,
    
    CAST(
        COUNT(DISTINCT CASE WHEN r.rentabilidade_acumulada_ano > c.taxa_cdi_ano THEN r.conta_xp_cliente END) 
        AS DECIMAL(12,6)
    ) / NULLIF(COUNT(DISTINCT r.conta_xp_cliente), 0) as perc_clientes_acima_cdi_ano

FROM silver.fact_rentabilidade_clientes r

-- Join com CDI
INNER JOIN silver.fact_cdi_historico c 
    ON r.ano_mes = CAST(c.ano_mes AS INT)

-- Join com patrimônio (filtrando >= 300k)
INNER JOIN silver.fact_patrimonio pat 
    ON r.conta_xp_cliente = pat.conta_xp_cliente 
    AND YEAR(pat.data_ref) * 100 + MONTH(pat.data_ref) = r.ano_mes
    AND pat.patrimonio_xp >= 300000

-- Join com histórico de clientes para pegar o assessor
INNER JOIN silver.fact_cliente_perfil_historico h
    ON r.conta_xp_cliente = h.conta_xp_cliente
    AND YEAR(h.data_ref) * 100 + MONTH(h.data_ref) = r.ano_mes

-- Join com dim_clientes para filtrar apenas PF
INNER JOIN silver.dim_clientes cli
    ON r.conta_xp_cliente = cli.cod_xp
    AND cli.cpf IS NOT NULL  -- Apenas pessoas físicas

-- Join com pessoas para dados do assessor
INNER JOIN silver.dim_pessoas p
    ON h.cod_assessor = p.cod_aai

-- Join com estrutura (opcional - assessor pode não estar em estrutura)
LEFT JOIN silver.fact_estrutura_pessoas e
    ON p.crm_id = e.crm_id
    AND DATEFROMPARTS(r.ano, r.mes_num, 1) >= e.data_entrada
    AND DATEFROMPARTS(r.ano, r.mes_num, 1) <= ISNULL(e.data_saida, '9999-12-31')

-- Join com dimensão de estruturas
LEFT JOIN silver.dim_estruturas est
    ON e.id_estrutura = est.id_estrutura

WHERE h.cod_assessor IS NOT NULL
  AND r.ano >= 2024

GROUP BY 
    r.ano_mes,
    r.ano,
    r.mes_num,
    r.mes,
    r.trimestre,
    r.semestre,
    h.cod_assessor,
    p.crm_id,
    p.nome_pessoa,
    p.assessor_nivel,
    e.id_estrutura,
    est.nome_estrutura;
GO

-- ==============================================================================
-- 7. CONSIDERAÇÕES TÉCNICAS
-- ==============================================================================
/*
- JOINs complexos requerem índices adequados para performance
- Filtros principais: PF (CPF não nulo), patrimônio >= 300k, ano >= 2024
- Estrutura vigente determinada pela data do período (data_entrada/data_saida)
- Cálculos de percentual usam NULLIF para evitar divisão por zero
- Métricas calculadas para múltiplos períodos (aumenta complexidade)
*/

-- ==============================================================================
-- 8. QUERIES AUXILIARES PARA VALIDAÇÃO
-- ==============================================================================
/*
-- Verificar distribuição de clientes por faixa de rentabilidade
SELECT 
    ano_mes,
    COUNT(DISTINCT cod_assessor) as qtd_assessores,
    SUM(qtd_clientes_300k_mais) as total_clientes,
    SUM(qtd_clientes_acima_cdi) as clientes_acima_cdi,
    AVG(perc_clientes_acima_cdi) as perc_medio_acima_cdi
FROM gold.vw_rentabilidade_assessor
GROUP BY ano_mes
ORDER BY ano_mes DESC;

-- Comparar métricas entre períodos para um assessor
SELECT 
    cod_assessor,
    nome_assessor,
    ano_mes,
    -- Métricas mensais
    perc_clientes_acima_cdi as perc_mensal,
    -- Métricas janelas móveis
    perc_clientes_acima_cdi_3m as perc_3m,
    perc_clientes_acima_cdi_6m as perc_6m,
    perc_clientes_acima_cdi_12m as perc_12m
FROM gold.vw_rentabilidade_assessor
WHERE cod_assessor = 'AAI123' -- Substituir pelo código desejado
ORDER BY ano_mes DESC;

-- Validar joins e filtros
SELECT 
    'Total Rentabilidade' as origem,
    COUNT(DISTINCT conta_xp_cliente) as qtd_clientes
FROM silver.fact_rentabilidade_clientes
WHERE ano >= 2024
UNION ALL
SELECT 
    'Após filtro 300k+',
    COUNT(DISTINCT r.conta_xp_cliente)
FROM silver.fact_rentabilidade_clientes r
INNER JOIN silver.fact_patrimonio p 
    ON r.conta_xp_cliente = p.conta_xp_cliente
    AND YEAR(p.data_ref) * 100 + MONTH(p.data_ref) = r.ano_mes
    AND p.patrimonio_xp >= 300000
WHERE r.ano >= 2024
UNION ALL
SELECT 
    'Após filtro PF',
    COUNT(DISTINCT r.conta_xp_cliente)
FROM silver.fact_rentabilidade_clientes r
INNER JOIN silver.fact_patrimonio p 
    ON r.conta_xp_cliente = p.conta_xp_cliente
    AND YEAR(p.data_ref) * 100 + MONTH(p.data_ref) = r.ano_mes
    AND p.patrimonio_xp >= 300000
INNER JOIN silver.dim_clientes c
    ON r.conta_xp_cliente = c.cod_xp
    AND c.cpf IS NOT NULL
WHERE r.ano >= 2024;
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
- View retorna métricas para todos os períodos, mas tabela armazena apenas mensal
- Filtro de patrimônio >= 300k aplicado via JOIN com fact_patrimonio
- Apenas clientes PF (CPF não nulo) são considerados
- Estrutura vigente do assessor determinada pela data do período
- Dados limitados a partir de 2024 (WHERE r.ano >= 2024)

Cálculos de rentabilidade vs CDI:
- Acima CDI: rentabilidade > taxa_cdi_[periodo]
- 80% CDI: rentabilidade >= (taxa_cdi_[periodo] * 0.80)
- 50% CDI: rentabilidade >= (taxa_cdi_[periodo] * 0.50)
- Positiva: rentabilidade > 0

Otimizações recomendadas:
- Índice em fact_rentabilidade_clientes (ano_mes, conta_xp_cliente)
- Índice em fact_patrimonio (data_ref, conta_xp_cliente, patrimonio_xp)
- Índice em fact_cliente_perfil_historico (conta_xp_cliente, data_ref)
- Estatísticas atualizadas em todas as tabelas

Limitações conhecidas:
- Performance pode degradar com grandes volumes históricos
- Múltiplos JOINs impactam tempo de execução
- Cálculos de múltiplos períodos aumentam complexidade

Contato para dúvidas: equipe-dados@m7investimentos.com.br
*/
