-- ==============================================================================
-- QRY-PAT-001-create_gold_performance_view_patrimonio_cliente
-- ==============================================================================
-- Tipo: View
-- Versão: 1.0.0
-- Última atualização: 2025-01-13
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [patrimonio, cliente, mensal, análise, open-investment]
-- Status: desenvolvimento
-- Banco de Dados: SQL Server
-- Schema: gold_performance
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: View que consolida dados de patrimônio por cliente, integrando informações
           de patrimônio na XP, patrimônio declarado e investimentos em outras
           instituições (Open Investment), permitindo análise de share of wallet
           e evolução patrimonial.

Casos de uso:
- Análise de share of wallet por cliente
- Identificação de oportunidades de captação
- Acompanhamento da evolução patrimonial
- Segmentação de clientes por faixa de patrimônio
- Relatórios de patrimônio para assessores
- Base para modelos de propensão a investir

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
-- Patrimônio atual de um cliente específico
SELECT * FROM [gold_performance].[view_patrimonio_cliente] 
WHERE conta_xp_cliente = 12345 
  AND data_ref = (SELECT MAX(data_ref) FROM [gold_performance].[view_patrimonio_cliente])
ORDER BY data_ref DESC;

-- Clientes com maior patrimônio na XP
SELECT TOP 100 * FROM [gold_performance].[view_patrimonio_cliente]
WHERE ano = 2024 AND mes = 12
ORDER BY patrimonio_xp DESC;

-- Clientes com baixo share of wallet (oportunidade)
SELECT * FROM [gold_performance].[view_patrimonio_cliente]
WHERE share_of_wallet < 30
  AND patrimonio_declarado > 1000000
  AND ano = 2024
ORDER BY patrimonio_declarado DESC;
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Colunas retornadas:

| Coluna                          | Tipo           | Descrição                                    | Exemplo          |
|---------------------------------|----------------|----------------------------------------------|------------------|
| data_ref                        | DATE           | Data de referência dos dados                 | 2024-12-31       |
| ano                            | INT            | Ano de referência                            | 2024             |
| mes                            | INT            | Mês de referência                            | 12               |
| nome_mes                       | VARCHAR(20)    | Nome do mês por extenso                      | 'Dezembro'       |
| trimestre                      | CHAR(2)        | Trimestre do ano                             | 'Q4'             |
| conta_xp_cliente               | INT            | Código da conta do cliente                   | 12345            |
| nome_cliente                   | VARCHAR(200)   | Nome do cliente                              | 'João Silva'     |
| tipo_cliente                   | VARCHAR(10)    | Tipo de cliente (PF/PJ)                      | 'PF'             |
| grupo_cliente                  | VARCHAR(50)    | Grupo econômico do cliente                   | 'Grupo ABC'      |
| status_cliente                 | VARCHAR(50)    | Status do cliente (ativo/inativo)            | 'Ativo'          |
| codigo_cliente_crm             | VARCHAR(20)    | Código CRM do cliente                        | 'CLI123'         |
| cod_assessor                   | VARCHAR(50)    | Código do assessor responsável               | 'AAI123'         |
| nome_assessor                  | VARCHAR(200)   | Nome do assessor                             | 'Maria Santos'   |
| assessor_nivel                 | VARCHAR(50)    | Nível do assessor                            | 'Senior'         |
| assessor_status                | VARCHAR(50)    | Status do assessor (ativo/inativo)           | 'Ativo'          |
| codigo_assessor_crm            | VARCHAR(20)    | Código CRM do assessor                       | 'CRM123'         |
| nome_estrutura                 | VARCHAR(100)   | Estrutura do assessor                        | 'Equipe SP'      |
| patrimonio_xp                  | DECIMAL(18,2)  | Patrimônio do cliente na XP                  | 500000.00        |
| patrimonio_declarado           | DECIMAL(18,2)  | Patrimônio total declarado pelo cliente      | 2000000.00       |
| patrimonio_open_investment     | DECIMAL(18,2)  | Patrimônio em outras instituições            | 1500000.00       |
| share_of_wallet                | DECIMAL(18,2)  | % do patrimônio investido na XP              | 25.00            |
| patrimonio_fora_xp             | DECIMAL(18,2)  | Patrimônio fora da XP (calculado)            | 1500000.00       |
| potencial_captacao             | DECIMAL(18,2)  | Potencial de captação (fora XP)              | 1500000.00       |
| faixa_patrimonio_xp            | VARCHAR(50)    | Classificação por patrimônio na XP           | '100K-500K'      |
| faixa_patrimonio_declarado     | VARCHAR(50)    | Classificação por patrimônio total           | '1M-5M'          |
| classificacao_share_wallet     | VARCHAR(50)    | Classificação do share of wallet             | 'Baixo'          |
| variacao_patrimonio_xp_mes     | DECIMAL(18,2)  | Variação % do patrimônio XP vs mês anterior  | 5.50             |
| variacao_patrimonio_xp_3m      | DECIMAL(18,2)  | Variação % do patrimônio XP vs 3 meses       | 15.25            |
| variacao_patrimonio_xp_12m     | DECIMAL(18,2)  | Variação % do patrimônio XP vs 12 meses      | 35.80            |
| meses_como_cliente             | INT            | Tempo de relacionamento em meses             | 24               |
| data_cadastro                  | DATE           | Data de cadastro do cliente                  | 2023-01-15       |
| primeira_data_patrimonio       | DATE           | Primeira data com patrimônio registrado      | 2023-01-20       |
| ultima_atualizacao_patrimonio  | DATE           | Última atualização do patrimônio             | 2024-12-31       |

Ordenação padrão: Nenhuma (aplicar ORDER BY na consulta)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- [silver].[fact_patrimonio]: Tabela fato com dados de patrimônio dos clientes
- [silver].[fact_cliente_perfil_historico]: Tabela fato com histórico mensal do perfil dos clientes (incluindo cod_assessor)
- [silver].[dim_calendario]: Dimensão calendário com informações temporais
- [silver].[dim_clientes]: Dimensão de clientes com dados cadastrais básicos
- [silver].[dim_pessoas]: Dimensão pessoas com dados cadastrais dos assessores
- [silver].[dim_estruturas]: Dimensão estruturas organizacionais
- [silver].[fact_estrutura_pessoas]: Fato com histórico de estrutura por pessoa

Funções/Procedures chamadas:
- Nenhuma

Pré-requisitos:
- Índices em fact_patrimonio: (data_ref, conta_xp_cliente)
- Dados atualizados nas tabelas silver
- Permissões de leitura no schema silver
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- View otimizada para análises de patrimônio e share of wallet
-- Inclui cálculos de variação temporal e classificações

-- ==============================================================================
-- 6. IMPLEMENTAÇÃO DA VIEW
-- ==============================================================================

-- Criar schema se não existir
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'gold_performance')
    EXEC('CREATE SCHEMA [gold_performance]')
GO

-- Remover view existente se necessário
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[gold_performance].[view_patrimonio_cliente]'))
    DROP VIEW [gold_performance].[view_patrimonio_cliente]
GO

CREATE VIEW [gold_performance].[view_patrimonio_cliente] AS
WITH 
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
        COALESCE(fep.data_saida, '9999-12-31') AS data_saida
    FROM 
        [silver].[dim_pessoas] p
        LEFT JOIN [silver].[fact_estrutura_pessoas] fep 
            ON p.crm_id = fep.crm_id
        LEFT JOIN [silver].[dim_estruturas] e 
            ON fep.id_estrutura = e.id_estrutura
    WHERE 
        p.cod_aai IS NOT NULL
),

-- -----------------------------------------------------------------------------
-- CTE: historico_patrimonio
-- Descrição: Calcula métricas históricas de patrimônio por cliente
-- -----------------------------------------------------------------------------
historico_patrimonio AS (
    SELECT 
        fp.conta_xp_cliente,
        fp.data_ref,
        fp.patrimonio_xp,
        -- Variações mensais
        LAG(fp.patrimonio_xp, 1) OVER (PARTITION BY fp.conta_xp_cliente ORDER BY fp.data_ref) AS patrimonio_xp_mes_anterior,
        LAG(fp.patrimonio_xp, 3) OVER (PARTITION BY fp.conta_xp_cliente ORDER BY fp.data_ref) AS patrimonio_xp_3m_anterior,
        LAG(fp.patrimonio_xp, 12) OVER (PARTITION BY fp.conta_xp_cliente ORDER BY fp.data_ref) AS patrimonio_xp_12m_anterior,
        -- Primeira e última data com patrimônio
        MIN(fp.data_ref) OVER (PARTITION BY fp.conta_xp_cliente) AS primeira_data_patrimonio,
        MAX(fp.data_ref) OVER (PARTITION BY fp.conta_xp_cliente) AS ultima_atualizacao_patrimonio
    FROM 
        [silver].[fact_patrimonio] fp
    WHERE 
        fp.patrimonio_xp IS NOT NULL OR fp.patrimonio_declarado IS NOT NULL
),

-- -----------------------------------------------------------------------------
-- CTE: classificacao_patrimonio
-- Descrição: Define faixas de classificação de patrimônio
-- -----------------------------------------------------------------------------
classificacao_patrimonio AS (
    SELECT 
        fp.conta_xp_cliente,
        fp.data_ref,
        -- Classificação por patrimônio na XP
        CASE 
            WHEN fp.patrimonio_xp IS NULL OR fp.patrimonio_xp = 0 THEN 'Sem patrimônio'
            WHEN fp.patrimonio_xp < 10000 THEN '< 10K'
            WHEN fp.patrimonio_xp < 50000 THEN '10K-50K'
            WHEN fp.patrimonio_xp < 100000 THEN '50K-100K'
            WHEN fp.patrimonio_xp < 500000 THEN '100K-500K'
            WHEN fp.patrimonio_xp < 1000000 THEN '500K-1M'
            WHEN fp.patrimonio_xp < 5000000 THEN '1M-5M'
            WHEN fp.patrimonio_xp < 10000000 THEN '5M-10M'
            ELSE '> 10M'
        END AS faixa_patrimonio_xp,
        -- Classificação por patrimônio declarado
        CASE 
            WHEN fp.patrimonio_declarado IS NULL OR fp.patrimonio_declarado = 0 THEN 'Não declarado'
            WHEN fp.patrimonio_declarado < 50000 THEN '< 50K'
            WHEN fp.patrimonio_declarado < 100000 THEN '50K-100K'
            WHEN fp.patrimonio_declarado < 500000 THEN '100K-500K'
            WHEN fp.patrimonio_declarado < 1000000 THEN '500K-1M'
            WHEN fp.patrimonio_declarado < 5000000 THEN '1M-5M'
            WHEN fp.patrimonio_declarado < 10000000 THEN '5M-10M'
            WHEN fp.patrimonio_declarado < 50000000 THEN '10M-50M'
            ELSE '> 50M'
        END AS faixa_patrimonio_declarado,
        -- Classificação por share of wallet
        CASE 
            WHEN fp.share_of_wallet IS NULL THEN 'Não calculado'
            WHEN fp.share_of_wallet = 0 THEN 'Zero'
            WHEN fp.share_of_wallet < 10 THEN 'Muito baixo (<10%)'
            WHEN fp.share_of_wallet < 25 THEN 'Baixo (10-25%)'
            WHEN fp.share_of_wallet < 50 THEN 'Médio (25-50%)'
            WHEN fp.share_of_wallet < 75 THEN 'Alto (50-75%)'
            WHEN fp.share_of_wallet < 90 THEN 'Muito alto (75-90%)'
            ELSE 'Exclusivo (>90%)'
        END AS classificacao_share_wallet
    FROM 
        [silver].[fact_patrimonio] fp
)

-- ==============================================================================
-- 7. QUERY PRINCIPAL
-- ==============================================================================
SELECT 
    -- Dimensões temporais
    fp.data_ref,
    cal.ano,
    cal.mes,
    cal.nome_mes,
    cal.trimestre,
    
    -- Dimensão cliente
    fp.conta_xp_cliente,
    COALESCE(dc.nome_cliente, 'Cliente não identificado') AS nome_cliente,
    CASE 
        WHEN dc.cpf IS NOT NULL THEN 'PF'
        WHEN dc.cnpj IS NOT NULL THEN 'PJ'
        ELSE 'Não definido'
    END AS tipo_cliente,
    dc.grupo_cliente,
    COALESCE(fcph.status_cliente, 'Não definido') AS status_cliente,
    dc.codigo_cliente_crm,
    
    -- Dimensão assessor (vinda de fact_cliente_perfil_historico)
    fcph.cod_assessor,
    COALESCE(ea.nome_assessor, 'Assessor não identificado') AS nome_assessor,
    ea.assessor_nivel,
    COALESCE(ea.assessor_status, 'Não definido') AS assessor_status,
    ea.codigo_assessor_crm,
    COALESCE(ea.nome_estrutura, 'Sem estrutura') AS nome_estrutura,
    
    -- Métricas de Patrimônio
    COALESCE(fp.patrimonio_xp, 0) AS patrimonio_xp,
    COALESCE(fp.patrimonio_declarado, 0) AS patrimonio_declarado,
    COALESCE(fp.patrimonio_open_investment, 0) AS patrimonio_open_investment,
    COALESCE(fp.share_of_wallet, 0) AS share_of_wallet,
    
    -- Métricas calculadas
    CASE 
        WHEN fp.patrimonio_declarado > 0 THEN 
            COALESCE(fp.patrimonio_declarado - fp.patrimonio_xp, 0)
        ELSE 0
    END AS patrimonio_fora_xp,
    
    CASE 
        WHEN fp.patrimonio_declarado > fp.patrimonio_xp THEN 
            fp.patrimonio_declarado - fp.patrimonio_xp
        ELSE 0
    END AS potencial_captacao,
    
    -- Classificações
    cp.faixa_patrimonio_xp,
    cp.faixa_patrimonio_declarado,
    cp.classificacao_share_wallet,
    
    -- Variações temporais
    CASE 
        WHEN hp.patrimonio_xp_mes_anterior > 0 THEN 
            ROUND(100.0 * (fp.patrimonio_xp - hp.patrimonio_xp_mes_anterior) / hp.patrimonio_xp_mes_anterior, 2)
        ELSE NULL
    END AS variacao_patrimonio_xp_mes,
    
    CASE 
        WHEN hp.patrimonio_xp_3m_anterior > 0 THEN 
            ROUND(100.0 * (fp.patrimonio_xp - hp.patrimonio_xp_3m_anterior) / hp.patrimonio_xp_3m_anterior, 2)
        ELSE NULL
    END AS variacao_patrimonio_xp_3m,
    
    CASE 
        WHEN hp.patrimonio_xp_12m_anterior > 0 THEN 
            ROUND(100.0 * (fp.patrimonio_xp - hp.patrimonio_xp_12m_anterior) / hp.patrimonio_xp_12m_anterior, 2)
        ELSE NULL
    END AS variacao_patrimonio_xp_12m,
    
    -- Métricas de Relacionamento
    DATEDIFF(MONTH, dc.data_cadastro, fp.data_ref) AS meses_como_cliente,
    dc.data_cadastro,
    hp.primeira_data_patrimonio,
    hp.ultima_atualizacao_patrimonio
    
FROM 
    [silver].[fact_patrimonio] fp
    LEFT JOIN [silver].[dim_clientes] dc
        ON fp.conta_xp_cliente = dc.cod_xp
    LEFT JOIN [silver].[fact_cliente_perfil_historico] fcph
        ON fp.conta_xp_cliente = fcph.conta_xp_cliente
        AND fp.data_ref = fcph.data_ref
    LEFT JOIN [silver].[dim_calendario] cal
        ON fp.data_ref = cal.data_ref
    LEFT JOIN estrutura_assessor_periodo ea
        ON fcph.cod_assessor = ea.cod_aai
        AND fp.data_ref >= ea.data_entrada
        AND fp.data_ref <= ea.data_saida
    LEFT JOIN historico_patrimonio hp
        ON fp.conta_xp_cliente = hp.conta_xp_cliente
        AND fp.data_ref = hp.data_ref
    LEFT JOIN classificacao_patrimonio cp
        ON fp.conta_xp_cliente = cp.conta_xp_cliente
        AND fp.data_ref = cp.data_ref
GO

-- ==============================================================================
-- 8. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================

/*
-- Query para verificar clientes com maior patrimônio no último mês disponível
SELECT TOP 20
    ano,
    mes,
    conta_xp_cliente,
    nome_cliente,
    nome_assessor,
    patrimonio_xp,
    patrimonio_declarado,
    share_of_wallet,
    classificacao_share_wallet
FROM [gold_performance].[view_patrimonio_cliente]
WHERE data_ref = (SELECT MAX(data_ref) FROM [gold_performance].[view_patrimonio_cliente])
ORDER BY patrimonio_xp DESC;

-- Query para identificar oportunidades de captação (baixo share of wallet)
SELECT TOP 50
    conta_xp_cliente,
    nome_cliente,
    nome_assessor,
    patrimonio_xp,
    patrimonio_declarado,
    potencial_captacao,
    share_of_wallet,
    classificacao_share_wallet
FROM [gold_performance].[view_patrimonio_cliente]
WHERE ano = YEAR(GETDATE()) 
  AND mes = MONTH(GETDATE()) - 1
  AND share_of_wallet < 30
  AND potencial_captacao > 1000000
ORDER BY potencial_captacao DESC;

-- Query para análise de evolução patrimonial
SELECT 
    conta_xp_cliente,
    nome_cliente,
    MIN(patrimonio_xp) AS patrimonio_inicial,
    MAX(patrimonio_xp) AS patrimonio_atual,
    AVG(variacao_patrimonio_xp_mes) AS variacao_media_mensal,
    COUNT(DISTINCT data_ref) AS meses_com_dados
FROM [gold_performance].[view_patrimonio_cliente]
WHERE ano >= YEAR(GETDATE()) - 1
GROUP BY conta_xp_cliente, nome_cliente
HAVING COUNT(DISTINCT data_ref) >= 6
ORDER BY MAX(patrimonio_xp) DESC;

-- Query para distribuição por faixa de patrimônio
SELECT 
    faixa_patrimonio_xp,
    COUNT(DISTINCT conta_xp_cliente) AS qtd_clientes,
    SUM(patrimonio_xp) AS patrimonio_total_xp,
    AVG(patrimonio_xp) AS patrimonio_medio_xp,
    AVG(share_of_wallet) AS share_wallet_medio
FROM [gold_performance].[view_patrimonio_cliente]
WHERE data_ref = (SELECT MAX(data_ref) FROM [gold_performance].[view_patrimonio_cliente])
GROUP BY faixa_patrimonio_xp
ORDER BY 
    CASE faixa_patrimonio_xp
        WHEN 'Sem patrimônio' THEN 1
        WHEN '< 10K' THEN 2
        WHEN '10K-50K' THEN 3
        WHEN '50K-100K' THEN 4
        WHEN '100K-500K' THEN 5
        WHEN '500K-1M' THEN 6
        WHEN '1M-5M' THEN 7
        WHEN '5M-10M' THEN 8
        WHEN '> 10M' THEN 9
    END;
*/

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor              | Descrição
--------|------------|--------------------|-----------------------------------------
1.0.0   | 2025-01-13 | Bruno Chiaramonti  | Criação inicial da view
1.0.1   | 2025-01-13 | Bruno Chiaramonti  | Ajuste para buscar cod_assessor da fact_cliente_perfil_historico ao invés de dim_clientes
1.0.2   | 2025-01-13 | Bruno Chiaramonti  | Revisão das junções com tabelas silver conforme solicitado
1.0.3   | 2025-01-13 | Bruno Chiaramonti  | Correção: status_cliente vem de fact_cliente_perfil_historico, não de dim_clientes

*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Share of wallet é calculado como (patrimonio_xp / patrimonio_declarado * 100)
- Patrimônio Open Investment representa investimentos em outras instituições
- COALESCE é usado para tratar valores NULL em cálculos
- Variações temporais só são calculadas quando há dados históricos disponíveis
- A view permite análise de oportunidades através do potencial de captação
- O cod_assessor vem de fact_cliente_perfil_historico pois dim_clientes contém apenas dados cadastrais básicos

Troubleshooting comum:
1. Share of wallet NULL: Ocorre quando patrimônio declarado é NULL ou zero
2. Variações temporais NULL: Normal para clientes novos sem histórico
3. Dados de Open Investment: Podem ter defasagem devido à natureza da integração
4. Performance lenta: Verificar índices em fact_patrimonio e estatísticas atualizadas

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/