-- ==============================================================================
-- QRY-CAP-004-create_gold_view_captacao_liquida_cliente
-- ==============================================================================
-- Tipo: View
-- Versão: 1.0.0
-- Última atualização: 2025-01-06
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [captação, cliente, mensal, análise]
-- Status: desenvolvimento
-- Banco de Dados: SQL Server
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: View que consolida dados de captação líquida por cliente, agregando
           valores de captação bruta e resgates para calcular a captação líquida
           mensal de cada cliente, incluindo métricas de comportamento e tendências.

Casos de uso:
- Análise de comportamento individual de clientes
- Identificação de clientes em risco de churn
- Segmentação de clientes por padrão de investimento
- Relatórios de relacionamento para assessores
- Base para modelos preditivos de resgate

Frequência de execução: View materializada - atualização diária
Tempo médio de execução: < 10 segundos
Volume esperado de linhas: ~50.000-100.000 registros por mês
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Esta é uma view sem parâmetros diretos. Filtros devem ser aplicados na consulta:

Exemplos de uso:
-- Histórico de um cliente específico
SELECT * FROM [gold].[view_captacao_liquida_cliente] 
WHERE conta_xp_cliente = 12345 
ORDER BY data_ref DESC;

-- Clientes com maior captação líquida no mês
SELECT TOP 100 * FROM [gold].[view_captacao_liquida_cliente]
WHERE ano = 2024 AND mes = 12
ORDER BY captacao_liquida_total DESC;

-- Clientes em risco (resgates consecutivos)
SELECT * FROM [gold].[view_captacao_liquida_cliente]
WHERE captacao_liquida_total < 0
  AND ano = 2024
ORDER BY captacao_liquida_total;
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Colunas retornadas:

| Coluna                          | Tipo           | Descrição                                    | Exemplo          |
|---------------------------------|----------------|----------------------------------------------|------------------|
| data_ref                        | DATE           | Data de referência (último dia do mês)       | 2024-12-31       |
| ano                            | INT            | Ano de referência                            | 2024             |
| mes                            | INT            | Mês de referência                            | 12               |
| nome_mes                       | VARCHAR(20)    | Nome do mês por extenso                      | 'Dezembro'       |
| trimestre                      | CHAR(2)        | Trimestre do ano                             | 'Q4'             |
| conta_xp_cliente               | INT            | Código da conta do cliente                   | 12345            |
| nome_cliente                   | VARCHAR(200)   | Nome do cliente                              | 'João Silva'     |
| tipo_cliente                   | VARCHAR(12)    | Tipo de cliente (PF/PJ)                      | 'PF'             |
| grupo_cliente                  | VARCHAR(50)    | Grupo do cliente                             | 'Grupo ABC'      |
| segmento_cliente               | VARCHAR(50)    | Segmentação do cliente                       | 'Private'        |
| status_cliente                 | VARCHAR(50)    | Status do cliente (ativo/inativo)            | 'Ativo'          |
| faixa_etaria                   | VARCHAR(50)    | Faixa etária do cliente                      | '40-50 anos'     |
| codigo_cliente_crm             | VARCHAR(20)    | Código CRM do cliente                        | 'CLI123'         |
| cod_assessor                   | VARCHAR(50)    | Código do assessor responsável               | 'AAI123'         |
| nome_assessor                  | VARCHAR(200)   | Nome do assessor                             | 'Maria Santos'   |
| assessor_nivel                 | VARCHAR(50)    | Nível do assessor                            | 'Senior'         |
| assessor_status                | VARCHAR(50)    | Status do assessor (ativo/inativo)           | 'Ativo'          |
| codigo_assessor_crm            | VARCHAR(20)    | Código CRM do assessor                       | 'CRM123'         |
| nome_estrutura                 | VARCHAR(100)   | Estrutura do assessor                        | 'Equipe SP'      |
| captacao_bruta_xp              | DECIMAL(18,2)  | Captação bruta via XP                        | 50000.00         |
| captacao_bruta_transferencia   | DECIMAL(18,2)  | Captação bruta via transferência             | 0.00             |
| captacao_bruta_total           | DECIMAL(18,2)  | Total de captação bruta                      | 50000.00         |
| resgate_bruto_xp               | DECIMAL(18,2)  | Resgates via XP                              | -10000.00        |
| resgate_bruto_transferencia    | DECIMAL(18,2)  | Resgates via transferência                   | 0.00             |
| resgate_bruto_total            | DECIMAL(18,2)  | Total de resgates                            | -10000.00        |
| captacao_liquida_xp            | DECIMAL(18,2)  | Captação líquida XP                          | 40000.00         |
| captacao_liquida_transferencia | DECIMAL(18,2)  | Captação líquida transferência               | 0.00             |
| captacao_liquida_total         | DECIMAL(18,2)  | Captação líquida total                       | 40000.00         |
| qtd_operacoes_aporte           | INT            | Número de operações de aporte no mês         | 3                |
| qtd_operacoes_resgate          | INT            | Número de operações de resgate no mês        | 1                |
| ticket_medio_aporte            | DECIMAL(18,2)  | Valor médio por operação de aporte           | 16666.67         |
| ticket_medio_resgate           | DECIMAL(18,2)  | Valor médio por operação de resgate          | -10000.00        |
| meses_como_cliente             | INT            | Tempo de relacionamento em meses             | 24               |
| primeira_captacao              | DATE           | Data da primeira captação                    | 2023-01-15       |
| ultima_captacao                | DATE           | Data da última captação                      | 2024-12-20       |
| ultimo_resgate                 | DATE           | Data do último resgate                       | 2024-11-15       |

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
- [silver].[dim_pessoas]: Dimensão pessoas com dados cadastrais
- [silver].[dim_estruturas]: Dimensão estruturas organizacionais
- [silver].[fact_estrutura_pessoas]: Fato com histórico de estrutura por pessoa
- [silver].[dim_clientes]: Dimensão de clientes com dados cadastrais e segmentação

Funções/Procedures chamadas:
- Nenhuma

Pré-requisitos:
- Índices em fact_captacao_bruta: (data_ref, conta_xp_cliente)
- Índices em fact_resgates: (data_ref, conta_xp_cliente)
- Dados atualizados nas tabelas silver
- Permissões de leitura no schema silver
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- View otimizada para agregações mensais por cliente
-- Considera apenas o último dia disponível de cada mês para evitar duplicações

-- ==============================================================================
-- 6. IMPLEMENTAÇÃO DA VIEW
-- ==============================================================================

-- Remover view existente se necessário
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[gold].[view_captacao_liquida_cliente]'))
    DROP VIEW [gold].[view_captacao_liquida_cliente]
GO

CREATE VIEW [gold].[view_captacao_liquida_cliente] AS
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
-- CTE: dados_cliente
-- Descrição: Busca dados cadastrais e de segmentação dos clientes
-- -----------------------------------------------------------------------------
-- CTE dados_cliente removida - usar diretamente dim_clientes no JOIN principal

-- -----------------------------------------------------------------------------
-- CTE: historico_cliente
-- Descrição: Calcula métricas históricas do cliente
-- -----------------------------------------------------------------------------
historico_cliente AS (
    SELECT 
        conta_xp_cliente,
        MIN(data_ref) AS primeira_captacao,
        MAX(CASE WHEN captacao_bruta_total > 0 THEN data_ref END) AS ultima_captacao,
        MAX(CASE WHEN resgate_bruto_total < 0 THEN data_ref END) AS ultimo_resgate
    FROM (
        SELECT conta_xp_cliente, data_ref, captacao_bruta_total, 0 AS resgate_bruto_total
        FROM [silver].[fact_captacao_bruta]
        UNION ALL
        SELECT conta_xp_cliente, data_ref, 0 AS captacao_bruta_total, resgate_bruto_total
        FROM [silver].[fact_resgates]
    ) operacoes
    GROUP BY conta_xp_cliente
),

-- -----------------------------------------------------------------------------
-- CTE: metricas_captacao_cliente
-- Descrição: Agrega métricas de captação por cliente para todo o mês
-- -----------------------------------------------------------------------------
metricas_captacao_cliente AS (
    SELECT 
        udm.ultimo_dia_disponivel AS data_ref,
        fcb.conta_xp_cliente,
        fcb.cod_assessor,
        COUNT(*) AS qtd_operacoes_aporte,
        SUM(fcb.captacao_bruta_xp) AS captacao_bruta_xp_total,
        SUM(fcb.captacao_bruta_transferencia) AS captacao_bruta_transferencia_total,
        SUM(fcb.captacao_bruta_total) AS captacao_bruta_total,
        AVG(fcb.captacao_bruta_total) AS ticket_medio_aporte
    FROM 
        [silver].[fact_captacao_bruta] fcb
        INNER JOIN ultimo_dia_mes udm
            ON YEAR(fcb.data_ref) = udm.ano
            AND MONTH(fcb.data_ref) = udm.mes
    GROUP BY 
        udm.ultimo_dia_disponivel,
        fcb.conta_xp_cliente,
        fcb.cod_assessor
),

-- -----------------------------------------------------------------------------
-- CTE: metricas_resgate_cliente
-- Descrição: Agrega métricas de resgate por cliente para todo o mês
-- -----------------------------------------------------------------------------
metricas_resgate_cliente AS (
    SELECT 
        udm.ultimo_dia_disponivel AS data_ref,
        fr.conta_xp_cliente,
        fr.cod_assessor,
        COUNT(*) AS qtd_operacoes_resgate,
        SUM(fr.resgate_bruto_xp) AS resgate_bruto_xp_total,
        SUM(fr.resgate_bruto_transferencia) AS resgate_bruto_transferencia_total,
        SUM(fr.resgate_bruto_total) AS resgate_bruto_total,
        AVG(fr.resgate_bruto_total) AS ticket_medio_resgate
    FROM 
        [silver].[fact_resgates] fr
        INNER JOIN ultimo_dia_mes udm
            ON YEAR(fr.data_ref) = udm.ano
            AND MONTH(fr.data_ref) = udm.mes
    GROUP BY 
        udm.ultimo_dia_disponivel,
        fr.conta_xp_cliente,
        fr.cod_assessor
),

-- -----------------------------------------------------------------------------
-- CTE: estrutura_assessor_periodo
-- Descrição: Busca a estrutura vigente do assessor em cada período
-- -----------------------------------------------------------------------------
estrutura_assessor_periodo AS (
    SELECT DISTINCT
        p.cod_aai,
        p.nome_pessoa AS nome_assessor,
        p.assessor_nivel,
        p.crm_id AS codigo_assessor_crm,
        CASE 
            WHEN p.data_fim_vigencia IS NULL THEN 'Ativo'
            ELSE 'Inativo'
        END AS assessor_status,
        e.nome_estrutura,
        fep.data_entrada,
        COALESCE(fep.data_saida, '9999-12-31') AS data_saida,
        udm.ultimo_dia_disponivel AS data_ref
    FROM 
        [silver].[dim_pessoas] p
        CROSS JOIN ultimo_dia_mes udm
        LEFT JOIN [silver].[fact_estrutura_pessoas] fep 
            ON p.crm_id = fep.crm_id
            AND udm.ultimo_dia_disponivel >= fep.data_entrada
            AND udm.ultimo_dia_disponivel <= COALESCE(fep.data_saida, '9999-12-31')
        LEFT JOIN [silver].[dim_estruturas] e 
            ON fep.id_estrutura = e.id_estrutura
    WHERE 
        p.cod_aai IS NOT NULL
)

-- ==============================================================================
-- 7. QUERY PRINCIPAL
-- ==============================================================================
SELECT 
    -- Dimensões temporais
    COALESCE(mcc.data_ref, mrc.data_ref) AS data_ref,
    cal.ano,
    cal.mes,
    cal.nome_mes,
    cal.trimestre,
    
    -- Dimensão cliente
    COALESCE(mcc.conta_xp_cliente, mrc.conta_xp_cliente) AS conta_xp_cliente,
    COALESCE(dc.nome_cliente, 'Cliente não identificado') AS nome_cliente,
    CASE 
        WHEN dc.cpf IS NOT NULL THEN 'PF'
        WHEN dc.cnpj IS NOT NULL THEN 'PJ'
        ELSE 'Não definido'
    END AS tipo_cliente,
    dc.grupo_cliente,
    NULL AS segmento_cliente,  -- Campo não disponível na dim_clientes
    NULL AS status_cliente,     -- Campo não disponível na dim_clientes
    NULL AS faixa_etaria,       -- Campo não disponível na dim_clientes
    dc.codigo_cliente_crm,
    
    -- Dimensão assessor
    COALESCE(mcc.cod_assessor, mrc.cod_assessor) AS cod_assessor,
    COALESCE(ea.nome_assessor, 'Assessor não identificado') AS nome_assessor,
    ea.assessor_nivel,
    COALESCE(ea.assessor_status, 'Não definido') AS assessor_status,
    ea.codigo_assessor_crm,
    COALESCE(ea.nome_estrutura, 'Sem estrutura') AS nome_estrutura,
    
    -- Métricas de Captação Bruta
    COALESCE(mcc.captacao_bruta_xp_total, 0) AS captacao_bruta_xp,
    COALESCE(mcc.captacao_bruta_transferencia_total, 0) AS captacao_bruta_transferencia,
    COALESCE(mcc.captacao_bruta_total, 0) AS captacao_bruta_total,
    
    -- Métricas de Resgate Bruto
    COALESCE(mrc.resgate_bruto_xp_total, 0) AS resgate_bruto_xp,
    COALESCE(mrc.resgate_bruto_transferencia_total, 0) AS resgate_bruto_transferencia,
    COALESCE(mrc.resgate_bruto_total, 0) AS resgate_bruto_total,
    
    -- Métricas de Captação Líquida (Captação + Resgate, pois resgates já são negativos)
    COALESCE(mcc.captacao_bruta_xp_total, 0) + COALESCE(mrc.resgate_bruto_xp_total, 0) AS captacao_liquida_xp,
    COALESCE(mcc.captacao_bruta_transferencia_total, 0) + COALESCE(mrc.resgate_bruto_transferencia_total, 0) AS captacao_liquida_transferencia,
    COALESCE(mcc.captacao_bruta_total, 0) + COALESCE(mrc.resgate_bruto_total, 0) AS captacao_liquida_total,
    
    -- Métricas de Operações
    COALESCE(mcc.qtd_operacoes_aporte, 0) AS qtd_operacoes_aporte,
    COALESCE(mrc.qtd_operacoes_resgate, 0) AS qtd_operacoes_resgate,
    COALESCE(mcc.ticket_medio_aporte, 0) AS ticket_medio_aporte,
    COALESCE(mrc.ticket_medio_resgate, 0) AS ticket_medio_resgate,
    
    -- Métricas de Relacionamento
    DATEDIFF(MONTH, dc.data_cadastro, COALESCE(mcc.data_ref, mrc.data_ref)) AS meses_como_cliente,
    hc.primeira_captacao,
    hc.ultima_captacao,
    hc.ultimo_resgate
    
FROM 
    metricas_captacao_cliente mcc
    FULL OUTER JOIN metricas_resgate_cliente mrc
        ON mcc.data_ref = mrc.data_ref
        AND mcc.conta_xp_cliente = mrc.conta_xp_cliente
        AND mcc.cod_assessor = mrc.cod_assessor
    LEFT JOIN [silver].[dim_clientes] dc
        ON COALESCE(mcc.conta_xp_cliente, mrc.conta_xp_cliente) = dc.cod_xp
    LEFT JOIN historico_cliente hc
        ON COALESCE(mcc.conta_xp_cliente, mrc.conta_xp_cliente) = hc.conta_xp_cliente
    LEFT JOIN [silver].[dim_calendario] cal
        ON COALESCE(mcc.data_ref, mrc.data_ref) = cal.data_ref
    LEFT JOIN estrutura_assessor_periodo ea
        ON COALESCE(mcc.cod_assessor, mrc.cod_assessor) = ea.cod_aai
        AND COALESCE(mcc.data_ref, mrc.data_ref) = ea.data_ref
GO

-- ==============================================================================
-- 8. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================

/*
-- Query para verificar clientes com maior captação líquida no último mês
SELECT TOP 20
    ano,
    mes,
    conta_xp_cliente,
    nome_cliente,
    nome_assessor,
    captacao_liquida_total,
    meses_como_cliente
FROM [gold].[view_captacao_liquida_cliente]
WHERE ano = YEAR(GETDATE()) 
  AND mes = MONTH(GETDATE()) - 1
ORDER BY captacao_liquida_total DESC;

-- Query para identificar clientes em risco (3 meses consecutivos de resgate líquido)
WITH resgates_consecutivos AS (
    SELECT 
        conta_xp_cliente,
        nome_cliente,
        captacao_liquida_total,
        LAG(captacao_liquida_total, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY data_ref) AS mes_anterior_1,
        LAG(captacao_liquida_total, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY data_ref) AS mes_anterior_2
    FROM [gold].[view_captacao_liquida_cliente]
)
SELECT DISTINCT
    conta_xp_cliente,
    nome_cliente
FROM resgates_consecutivos
WHERE captacao_liquida_total < 0 
  AND mes_anterior_1 < 0 
  AND mes_anterior_2 < 0;

-- Query para análise por tipo de cliente (PF/PJ)
SELECT 
    tipo_cliente,
    COUNT(DISTINCT conta_xp_cliente) as qtd_clientes,
    AVG(ticket_medio_aporte) as ticket_medio_aporte_tipo,
    AVG(ticket_medio_resgate) as ticket_medio_resgate_tipo,
    SUM(captacao_liquida_total) as captacao_liquida_tipo
FROM [gold].[view_captacao_liquida_cliente]
WHERE ano = 2024
GROUP BY tipo_cliente
ORDER BY captacao_liquida_tipo DESC;

-- Query para análise por assessor
SELECT 
    cod_assessor,
    nome_assessor,
    COUNT(DISTINCT conta_xp_cliente) as qtd_clientes,
    AVG(captacao_liquida_total) as captacao_liquida_media,
    SUM(captacao_liquida_total) as captacao_liquida_total
FROM [gold].[view_captacao_liquida_cliente]
WHERE ano = 2024
GROUP BY cod_assessor, nome_assessor
ORDER BY captacao_liquida_total DESC;
*/

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor              | Descrição
--------|------------|--------------------|-----------------------------------------
1.0.0   | 2025-01-06 | Bruno Chiaramonti  | Criação inicial da view
1.1.0   | 2025-01-16 | Bruno Chiaramonti  | Migração para schema gold
*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- A view considera apenas o último dia disponível de cada mês para evitar duplicação
- Valores de resgate são armazenados como negativos nas tabelas origem
- COALESCE é usado para tratar clientes sem captação ou resgate no período
- Performance otimizada para consultas mensais agregadas por cliente

Troubleshooting comum:
1. Dados faltantes: Verificar se as procedures de carga das tabelas silver foram executadas
2. Clientes sem nome: Verificar integridade da silver.dim_clientes
   - grupo_cliente pode estar NULL para clientes sem grupo econômico
   - faixa_etaria já vem calculada da dimensão
3. Performance lenta: Verificar índices nas tabelas silver e estatísticas atualizadas

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/