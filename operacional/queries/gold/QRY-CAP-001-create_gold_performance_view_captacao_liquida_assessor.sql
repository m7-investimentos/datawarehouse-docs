-- ==============================================================================
-- QRY-CAP-001-create_gold_performance_view_captacao_liquida_assessor
-- ==============================================================================
-- Tipo: View
-- Versão: 1.0.0
-- Última atualização: 2025-01-06
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [captação, assessor, mensal, análise]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: gold_performance
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: View que consolida dados de captação líquida por assessor, agregando
           valores de captação bruta e resgates para calcular a captação líquida
           mensal de cada assessor.

Casos de uso:
- Análise de performance mensal de assessores
- Dashboards executivos de captação líquida
- Relatórios de acompanhamento de metas
- Identificação de assessores com maior captação/resgate

Frequência de execução: View materializada - atualização diária
Tempo médio de execução: < 5 segundos
Volume esperado de linhas: ~500-1000 registros por mês
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Esta é uma view sem parâmetros diretos. Filtros devem ser aplicados na consulta:

Exemplos de uso:
-- Captação de um assessor específico
SELECT * FROM [gold_performance].[view_captacao_liquida_assessor] 
WHERE cod_assessor = 'AAI123' AND ano = 2024;

-- Top 10 assessores por captação líquida no mês
SELECT TOP 10 * FROM [gold_performance].[view_captacao_liquida_assessor]
WHERE ano = 2024 AND mes = 12
ORDER BY captacao_liquida_total DESC;
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Colunas retornadas:

| Coluna                        | Tipo           | Descrição                                    | Exemplo      |
|-------------------------------|----------------|----------------------------------------------|--------------|
| data_ref                      | DATE           | Data de referência (último dia do mês)       | 2024-12-31   |
| ano                          | INT            | Ano de referência                            | 2024         |
| mes                          | INT            | Mês de referência                            | 12           |
| nome_mes                      | VARCHAR(20)    | Nome do mês por extenso                      | 'Dezembro'   |
| trimestre                     | CHAR(2)        | Trimestre do ano                             | 'Q4'         |
| cod_assessor                 | VARCHAR(50)    | Código do assessor                           | 'AAI123'     |
| nome_assessor                | VARCHAR(200)   | Nome completo do assessor                    | 'João Silva' |
| assessor_nivel               | VARCHAR(50)    | Nível do assessor                            | 'Senior'     |
| codigo_assessor_crm          | VARCHAR(20)    | Código CRM do assessor                       | 'CRM123'     |
| assessor_status              | VARCHAR(50)    | Status do assessor (ativo/inativo)           | 'Ativo'      |
| nome_estrutura               | VARCHAR(100)   | Nome da estrutura do assessor                | 'Equipe SP'  |
| captacao_bruta_xp            | DECIMAL(18,2)  | Captação bruta via XP                        | 150000.00    |
| captacao_bruta_transferencia | DECIMAL(18,2)  | Captação bruta via transferência             | 50000.00     |
| captacao_bruta_total         | DECIMAL(18,2)  | Total de captação bruta                      | 200000.00    |
| resgate_bruto_xp             | DECIMAL(18,2)  | Resgates via XP                              | -30000.00    |
| resgate_bruto_transferencia  | DECIMAL(18,2)  | Resgates via transferência                   | -10000.00    |
| resgate_bruto_total          | DECIMAL(18,2)  | Total de resgates                            | -40000.00    |
| captacao_liquida_xp          | DECIMAL(18,2)  | Captação líquida XP                          | 120000.00    |
| captacao_liquida_transferencia| DECIMAL(18,2)  | Captação líquida transferência               | 40000.00     |
| captacao_liquida_total       | DECIMAL(18,2)  | Captação líquida total                       | 160000.00    |
| qtd_clientes_aportando       | INT            | Clientes únicos que fizeram aportes          | 45           |
| qtd_clientes_resgatando      | INT            | Clientes únicos que fizeram resgates         | 12           |
| ticket_medio_aporte          | DECIMAL(18,2)  | Valor médio por operação de aporte           | 3500.00      |
| ticket_medio_resgate         | DECIMAL(18,2)  | Valor médio por operação de resgate          | -2500.00     |
| qtd_clientes_apenas_aportando| INT            | Clientes que só aportaram                    | 35           |
| qtd_clientes_apenas_resgatando| INT           | Clientes que só resgataram                   | 5            |
| qtd_clientes_aporte_e_resgate| INT            | Clientes que fizeram ambos                   | 10           |

Ordenação padrão: Nenhuma (aplicar ORDER BY na consulta)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- [silver].[fact_captacao_bruta]: Tabela fato com dados de captação bruta diária
- [silver].[fact_resgates]: Tabela fato com dados de resgates e transferências de saída
- [silver].[dim_calendario]: Dimensão calendário com informações temporais
- [silver].[dim_pessoas]: Dimensão pessoas com dados cadastrais dos assessores
- [silver].[dim_estruturas]: Dimensão estruturas organizacionais
- [silver].[fact_estrutura_pessoas]: Fato com histórico de estrutura por pessoa

Funções/Procedures chamadas:
- Nenhuma

Pré-requisitos:
- Índices em fact_captacao_bruta: (data_ref, cod_assessor)
- Índices em fact_resgates: (data_ref, cod_assessor)
- Dados atualizados nas tabelas silver
- Permissões de leitura no schema silver
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- View otimizada para agregações mensais
-- Considera apenas o último dia disponível de cada mês para evitar duplicações

-- ==============================================================================
-- 6. IMPLEMENTAÇÃO DA VIEW
-- ==============================================================================

-- Remover view existente se necessário
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[gold_performance].[view_captacao_liquida_assessor]'))
    DROP VIEW [gold_performance].[view_captacao_liquida_assessor]
GO

CREATE VIEW [gold_performance].[view_captacao_liquida_assessor] AS
WITH 
-- -----------------------------------------------------------------------------
-- CTE: ultimo_dia_mes
-- Descrição: Identifica o último dia com dados disponíveis em cada mês
-- -----------------------------------------------------------------------------
ultimo_dia_mes AS (
    SELECT 
        YEAR(data_ref) AS ano,
        MONTH(data_ref) AS mes,
        MAX(data_ref) AS ultimo_dia_disponivel
    FROM 
        [silver].[fact_captacao_bruta]
    GROUP BY 
        YEAR(data_ref), 
        MONTH(data_ref)
),

-- -----------------------------------------------------------------------------
-- CTE: estrutura_assessor
-- Descrição: Busca a estrutura vigente do assessor na data de referência
-- -----------------------------------------------------------------------------
estrutura_assessor AS (
    SELECT 
        p.cod_aai,
        p.crm_id,
        p.nome_pessoa,
        p.assessor_nivel,
        CASE 
            WHEN p.data_fim_vigencia IS NULL THEN 'Ativo'
            ELSE 'Inativo'
        END AS assessor_status,
        fep.id_estrutura,
        e.nome_estrutura,
        e.estrutura_pai,
        ep.nome_estrutura AS nome_estrutura_pai,
        fep.data_entrada,
        fep.data_saida
    FROM 
        [silver].[dim_pessoas] p
        LEFT JOIN [silver].[fact_estrutura_pessoas] fep 
            ON p.crm_id = fep.crm_id
        LEFT JOIN [silver].[dim_estruturas] e 
            ON fep.id_estrutura = e.id_estrutura
        LEFT JOIN [silver].[dim_estruturas] ep 
            ON e.estrutura_pai = ep.id_estrutura
    WHERE 
        p.cod_aai IS NOT NULL
),

-- -----------------------------------------------------------------------------
-- CTE: metricas_captacao
-- Descrição: Agrega métricas de captação por assessor no último dia do mês
-- -----------------------------------------------------------------------------
metricas_captacao AS (
    SELECT 
        fcb.data_ref,
        fcb.cod_assessor,
        COUNT(DISTINCT fcb.conta_xp_cliente) AS qtd_clientes_aportando,
        SUM(fcb.captacao_bruta_xp) AS captacao_bruta_xp_total,
        SUM(fcb.captacao_bruta_transferencia) AS captacao_bruta_transferencia_total,
        SUM(fcb.captacao_bruta_total) AS captacao_bruta_total,
        AVG(fcb.captacao_bruta_total) AS ticket_medio_aporte
    FROM 
        [silver].[fact_captacao_bruta] fcb
        INNER JOIN ultimo_dia_mes udm
            ON fcb.data_ref = udm.ultimo_dia_disponivel
    GROUP BY 
        fcb.data_ref,
        fcb.cod_assessor
),

-- -----------------------------------------------------------------------------
-- CTE: metricas_resgate
-- Descrição: Agrega métricas de resgate por assessor no último dia do mês
-- -----------------------------------------------------------------------------
metricas_resgate AS (
    SELECT 
        fr.data_ref,
        fr.cod_assessor,
        COUNT(DISTINCT fr.conta_xp_cliente) AS qtd_clientes_resgatando,
        SUM(fr.resgate_bruto_xp) AS resgate_bruto_xp_total,
        SUM(fr.resgate_bruto_transferencia) AS resgate_bruto_transferencia_total,
        SUM(fr.resgate_bruto_total) AS resgate_bruto_total,
        AVG(fr.resgate_bruto_total) AS ticket_medio_resgate
    FROM 
        [silver].[fact_resgates] fr
        INNER JOIN ultimo_dia_mes udm
            ON fr.data_ref = udm.ultimo_dia_disponivel
    GROUP BY 
        fr.data_ref,
        fr.cod_assessor
),

-- -----------------------------------------------------------------------------
-- CTE: analise_clientes
-- Descrição: Analisa comportamento dos clientes (apenas aporte, apenas resgate, ambos)
-- -----------------------------------------------------------------------------
analise_clientes AS (
    SELECT 
        COALESCE(c.data_ref, r.data_ref) AS data_ref,
        COALESCE(c.cod_assessor, r.cod_assessor) AS cod_assessor,
        COUNT(DISTINCT c.conta_xp_cliente) AS clientes_captacao,
        COUNT(DISTINCT r.conta_xp_cliente) AS clientes_resgate,
        COUNT(DISTINCT CASE WHEN c.conta_xp_cliente = r.conta_xp_cliente THEN c.conta_xp_cliente END) AS clientes_ambos
    FROM 
        (SELECT DISTINCT data_ref, cod_assessor, conta_xp_cliente 
         FROM [silver].[fact_captacao_bruta] fcb
         INNER JOIN ultimo_dia_mes udm ON fcb.data_ref = udm.ultimo_dia_disponivel) c
    FULL OUTER JOIN 
        (SELECT DISTINCT data_ref, cod_assessor, conta_xp_cliente 
         FROM [silver].[fact_resgates] fr
         INNER JOIN ultimo_dia_mes udm ON fr.data_ref = udm.ultimo_dia_disponivel) r
    ON c.data_ref = r.data_ref 
        AND c.cod_assessor = r.cod_assessor 
        AND c.conta_xp_cliente = r.conta_xp_cliente
    GROUP BY 
        COALESCE(c.data_ref, r.data_ref),
        COALESCE(c.cod_assessor, r.cod_assessor)
)

-- ==============================================================================
-- 7. QUERY PRINCIPAL
-- ==============================================================================
SELECT 
    -- Dimensões temporais
    COALESCE(mc.data_ref, mr.data_ref) AS data_ref,
    YEAR(COALESCE(mc.data_ref, mr.data_ref)) AS ano,
    MONTH(COALESCE(mc.data_ref, mr.data_ref)) AS mes,
    cal.nome_mes,
    cal.trimestre,
    
    -- Dimensão assessor
    COALESCE(mc.cod_assessor, mr.cod_assessor) AS cod_assessor,
    COALESCE(ea.nome_pessoa, 'Assessor não identificado') AS nome_assessor,
    ea.assessor_nivel,
    ea.crm_id AS codigo_assessor_crm,
    COALESCE(ea.assessor_status, 'Não definido') AS assessor_status,
    
    -- Dimensão estrutura
    COALESCE(ea.nome_estrutura, 'Sem estrutura') AS nome_estrutura,
    
    -- Métricas de Captação Bruta
    COALESCE(mc.captacao_bruta_xp_total, 0) AS captacao_bruta_xp,
    COALESCE(mc.captacao_bruta_transferencia_total, 0) AS captacao_bruta_transferencia,
    COALESCE(mc.captacao_bruta_total, 0) AS captacao_bruta_total,
    
    -- Métricas de Resgate Bruto
    COALESCE(mr.resgate_bruto_xp_total, 0) AS resgate_bruto_xp,
    COALESCE(mr.resgate_bruto_transferencia_total, 0) AS resgate_bruto_transferencia,
    COALESCE(mr.resgate_bruto_total, 0) AS resgate_bruto_total,
    
    -- Métricas de Captação Líquida (Captação + Resgate, pois resgates já são negativos)
    COALESCE(mc.captacao_bruta_xp_total, 0) + COALESCE(mr.resgate_bruto_xp_total, 0) AS captacao_liquida_xp,
    COALESCE(mc.captacao_bruta_transferencia_total, 0) + COALESCE(mr.resgate_bruto_transferencia_total, 0) AS captacao_liquida_transferencia,
    COALESCE(mc.captacao_bruta_total, 0) + COALESCE(mr.resgate_bruto_total, 0) AS captacao_liquida_total,
    
    -- Métricas de Clientes e Tickets
    COALESCE(mc.qtd_clientes_aportando, 0) AS qtd_clientes_aportando,
    COALESCE(mr.qtd_clientes_resgatando, 0) AS qtd_clientes_resgatando,
    COALESCE(mc.ticket_medio_aporte, 0) AS ticket_medio_aporte,
    COALESCE(mr.ticket_medio_resgate, 0) AS ticket_medio_resgate,
    
    -- Análise de comportamento de clientes
    COALESCE(ac.clientes_captacao, 0) - COALESCE(ac.clientes_ambos, 0) AS qtd_clientes_apenas_aportando,
    COALESCE(ac.clientes_resgate, 0) - COALESCE(ac.clientes_ambos, 0) AS qtd_clientes_apenas_resgatando,
    COALESCE(ac.clientes_ambos, 0) AS qtd_clientes_aporte_e_resgate
    
FROM 
    metricas_captacao mc
    FULL OUTER JOIN metricas_resgate mr
        ON mc.data_ref = mr.data_ref
        AND mc.cod_assessor = mr.cod_assessor
    LEFT JOIN analise_clientes ac
        ON COALESCE(mc.data_ref, mr.data_ref) = ac.data_ref
        AND COALESCE(mc.cod_assessor, mr.cod_assessor) = ac.cod_assessor
    LEFT JOIN [silver].[dim_calendario] cal
        ON COALESCE(mc.data_ref, mr.data_ref) = cal.data_ref
    LEFT JOIN estrutura_assessor ea
        ON COALESCE(mc.cod_assessor, mr.cod_assessor) = ea.cod_aai
        AND COALESCE(mc.data_ref, mr.data_ref) BETWEEN ea.data_entrada AND COALESCE(ea.data_saida, '9999-12-31')
GO

-- ==============================================================================
-- 8. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================

/*
-- Query para verificar dados do último mês com informações completas
SELECT TOP 10
    ano,
    mes,
    nome_mes,
    cod_assessor,
    nome_assessor,
    nome_estrutura,
    captacao_liquida_total
FROM [gold_performance].[view_captacao_liquida_assessor]
WHERE ano = YEAR(GETDATE()) 
  AND mes = MONTH(GETDATE())
ORDER BY captacao_liquida_total DESC;

-- Query para verificar integridade - assessores sem captação mas com resgate
SELECT 
    cod_assessor,
    COUNT(*) as meses_negativos,
    SUM(captacao_liquida_total) as captacao_liquida_acumulada
FROM [gold_performance].[view_captacao_liquida_assessor]
WHERE captacao_liquida_total < 0
GROUP BY cod_assessor
HAVING COUNT(*) > 3
ORDER BY meses_negativos DESC;

-- Query para validar totais mensais
SELECT 
    ano,
    mes,
    COUNT(DISTINCT cod_assessor) as qtd_assessores,
    SUM(captacao_bruta_total) as captacao_bruta_mes,
    SUM(resgate_bruto_total) as resgate_bruto_mes,
    SUM(captacao_liquida_total) as captacao_liquida_mes
FROM [gold_performance].[view_captacao_liquida_assessor]
GROUP BY ano, mes
ORDER BY ano DESC, mes DESC;
*/

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor              | Descrição
--------|------------|--------------------|-----------------------------------------
1.0.0   | 2025-01-06 | Bruno Chiaramonti  | Criação inicial da view
1.1.0   | 2025-01-06 | Bruno Chiaramonti  | Adição de dimensões: calendário, pessoas e estruturas
1.2.0   | 2025-01-06 | Bruno Chiaramonti  | Adição de métricas de clientes e análise comportamental
1.3.0   | 2025-01-06 | Bruno Chiaramonti  | Ajuste para schema gold_performance
*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- A view considera apenas o último dia disponível de cada mês para evitar duplicação
- Valores de resgate são armazenados como negativos nas tabelas origem
- COALESCE é usado para tratar assessores sem resgates no período
- Performance otimizada para consultas mensais agregadas

Troubleshooting comum:
1. Dados faltantes: Verificar se as procedures de carga das tabelas silver foram executadas
2. Valores incorretos: Confirmar se os sinais (positivo/negativo) estão corretos nas tabelas origem
3. Performance lenta: Verificar índices nas tabelas silver e estatísticas atualizadas

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/