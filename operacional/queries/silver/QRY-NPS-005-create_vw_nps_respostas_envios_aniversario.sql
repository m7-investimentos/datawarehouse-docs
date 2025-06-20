-- ==============================================================================
-- QRY-NPS-005-CREATE_VW_NPS_RESPOSTAS_ENVIOS_ANIVERSARIO
-- ==============================================================================
-- Tipo: View
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [view, silver, nps, pesquisa, consolidacao]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: View que consolida dados de envio e respostas de pesquisas NPS de
           aniversário de relacionamento. Une informações das tabelas bronze
           xp_nps_respostas e xp_nps_envios, aplicando lógica de versionamento
           para garantir apenas os registros mais recentes de cada survey_id.

Casos de uso:
- Fonte para carga da tabela fact_nps_respostas_envios_aniversario
- Análise em tempo real de pesquisas NPS sem materialização
- Cálculo automático de classificações (Promotor/Neutro/Detrator)
- Consolidação de envios sem resposta e respostas completas
- Base para relatórios e dashboards de NPS

Frequência de execução: Sob demanda (view não materializada)
Tempo médio de execução: 2-5 segundos
Volume esperado de linhas: ~50.000 registros totais
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros: Nenhum (view padrão)

Filtros aplicáveis na consulta:
- WHERE survey_id = 'XXX' -- Para buscar pesquisa específica
- WHERE cod_assessor = 'XXX' -- Para filtrar por assessor
- WHERE data_resposta BETWEEN @inicio AND @fim -- Por período
- WHERE survey_status = 'completed' -- Apenas respondidas
- WHERE classificacao_nps_assessor = 'Promotor' -- Por classificação
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Colunas retornadas:

| Coluna                                  | Tipo         | Descrição                                              | Exemplo           |
|-----------------------------------------|--------------|--------------------------------------------------------|-------------------|
| survey_id                               | VARCHAR(20)  | ID único da pesquisa (de respostas ou envios)        | 'SV_123456'       |
| customer_id                             | VARCHAR(20)  | Código da conta XP do cliente                         | '1234567'         |
| cod_assessor                            | VARCHAR(20)  | Código do assessor responsável                        | 'ASS001'          |
| data_entrega                            | DATE         | Data de envio da pesquisa                             | '2024-03-15'      |
| data_resposta                           | DATE         | Data da resposta (de ambas as tabelas)                | '2024-03-17'      |
| delivered_on_date                       | DATETIME     | Timestamp de entrega (bronze.respostas)               | '2024-03-15 10:00'|
| invitation_opened_date                  | DATETIME     | Timestamp de abertura do convite                      | '2024-03-16 14:30'|
| survey_start_date                       | DATETIME     | Timestamp de início do preenchimento                  | '2024-03-17 09:15'|
| survey_status                           | VARCHAR(50)  | Status da pesquisa                                    | 'completed'       |
| invitation_opened                       | CHAR(3)      | Se o convite foi aberto                              | 'sim'             |
| xp_aniversario_nps_assessor            | DECIMAL(3,1) | Nota NPS do assessor (0-10)                          | 9.0               |
| xp_aniversario_nps_xp                   | DECIMAL(3,1) | Nota NPS da XP (0-10)                                | 8.5               |
| xp_aniversario_recomendaria_assessor   | CHAR(3)      | Se recomendaria o assessor                           | 'sim'             |
| classificacao_nps_assessor             | VARCHAR(10)  | Classificação calculada do assessor                  | 'Promotor'        |
| classificacao_nps_xp                    | VARCHAR(10)  | Classificação calculada da XP                        | 'Neutro'          |
| xp_aniversario_comentario_assessor      | NVARCHAR(MAX)| Comentário sobre o assessor                          | 'Excelente...'    |
| xp_aniversario_comentario_xp            | NVARCHAR(MAX)| Comentário sobre a XP                                | 'Plataforma...'   |
| xp_razao_nps                            | NVARCHAR(100)| Razão principal da nota                              | 'Atendimento'     |
| xp_aniversario_razao_nps_assessor       | NVARCHAR(100)| Razão específica do assessor                         | 'Conhecimento'    |
| xp_aniversario_razao_nps_xp             | NVARCHAR(100)| Razão específica da XP                               | 'Rentabilidade'   |
| topics_tagged_original                  | NVARCHAR(500)| Tópicos identificados                                | 'suporte,carteira'|

Ordenação padrão: Não aplicada (usar ORDER BY na consulta)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas utilizadas:
- bronze.xp_nps_respostas: Respostas das pesquisas NPS
  * Contém notas, comentários e classificações
  * Versionada por data_carga
  * Chave: survey_id
  
- bronze.xp_nps_envios: Dados de envio das pesquisas
  * Contém datas de envio e status
  * Versionada por data_carga
  * Chave: survey_id

Lógica de join:
- FULL OUTER JOIN para capturar:
  * Envios sem resposta (customer não respondeu)
  * Respostas sem envio correspondente (possível erro de dados)
- COALESCE para campos presentes em ambas tabelas
- ROW_NUMBER() para pegar versão mais recente de cada survey_id

Pré-requisitos:
- Tabelas bronze devem existir e estar populadas
- Índices em survey_id e data_carga para performance
- Permissões SELECT nas tabelas bronze
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- View padrão, otimizações dependem dos índices nas tabelas base
-- Recomenda-se índices em:
-- bronze.xp_nps_respostas (survey_id, data_carga DESC)
-- bronze.xp_nps_envios (survey_id, data_carga DESC)

-- ==============================================================================
-- 6. SCRIPT DA VIEW
-- ==============================================================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE     VIEW [silver].[vw_nps_respostas_envios_aniversario] AS

WITH 
-- -----------------------------------------------------------------------------
-- CTE: RespostasLatest
-- Descrição: Seleciona a versão mais recente de cada survey_id nas respostas
-- -----------------------------------------------------------------------------
RespostasLatest AS (
    SELECT 
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY r.survey_id 
            ORDER BY r.data_carga DESC
        ) as rn_resposta
    FROM bronze.xp_nps_respostas r
),

-- -----------------------------------------------------------------------------
-- CTE: EnviosLatest
-- Descrição: Seleciona a versão mais recente de cada survey_id nos envios
-- -----------------------------------------------------------------------------
EnviosLatest AS (
    SELECT 
        e.*,
        ROW_NUMBER() OVER (
            PARTITION BY e.survey_id 
            ORDER BY e.data_carga DESC
        ) as rn_envio
    FROM bronze.xp_nps_envios e
)

-- ==============================================================================
-- 7. QUERY PRINCIPAL
-- ==============================================================================
SELECT 
    -- Identificadores principais (COALESCE para FULL OUTER JOIN)
    COALESCE(r.survey_id, e.survey_id) as survey_id,
    COALESCE(r.customer_id, e.customer_id) as customer_id,
    COALESCE(r.cod_assessor, e.cod_assessor) as cod_assessor,
    
    -- Datas do processo
    e.data_entrega,
    COALESCE(r.data_resposta, e.data_resposta) as data_resposta,
    r.delivered_on_date,
    e.invitation_opened_date,
    e.survey_start_date,
    
    -- Status do envio
    e.survey_status,
    e.invitation_opened,
    
    -- Notas NPS (apenas de respostas)
    r.xp_aniversario_nps_assessor,
    r.xp_aniversario_nps_xp,
    
    -- Indicador de recomendação
    r.xp_aniversario_recomendaria_assessor,
    
    -- Classificação NPS calculada para assessor
    CASE 
        WHEN r.xp_aniversario_nps_assessor >= 9 THEN 'Promotor'
        WHEN r.xp_aniversario_nps_assessor >= 7 THEN 'Neutro'
        WHEN r.xp_aniversario_nps_assessor >= 0 THEN 'Detrator'
        ELSE NULL
    END AS classificacao_nps_assessor,
    
    -- Classificação NPS calculada para XP
    CASE 
        WHEN r.xp_aniversario_nps_xp >= 9 THEN 'Promotor'
        WHEN r.xp_aniversario_nps_xp >= 7 THEN 'Neutro'
        WHEN r.xp_aniversario_nps_xp >= 0 THEN 'Detrator'
        ELSE NULL
    END AS classificacao_nps_xp,
    
    -- Comentários em texto livre
    r.xp_aniversario_comentario_assessor,
    r.xp_aniversario_comentario_xp,
    
    -- Razões das notas
    r.xp_razao_nps,
    r.xp_aniversario_razao_nps_assessor,
    r.xp_aniversario_razao_nps_xp,
    
    -- Tópicos identificados
    r.topics_tagged_original

FROM RespostasLatest r
FULL OUTER JOIN EnviosLatest e
    ON r.survey_id = e.survey_id
    AND r.rn_resposta = 1  -- Apenas a versão mais recente da resposta
    AND e.rn_envio = 1     -- Apenas a versão mais recente do envio

WHERE 
    -- Garante que pegamos apenas os registros mais recentes
    (r.rn_resposta = 1 OR r.rn_resposta IS NULL)
    AND (e.rn_envio = 1 OR e.rn_envio IS NULL);

GO

-- ==============================================================================
-- 8. DOCUMENTAÇÃO DA VIEW
-- ==============================================================================

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'View que consolida dados de envio e respostas de pesquisas NPS, aplicando versionamento para garantir apenas registros mais recentes. Calcula classificações NPS automaticamente.' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'VIEW',@level1name=N'vw_nps_respostas_envios_aniversario'
GO

-- ==============================================================================
-- 9. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================

/*
-- Query para verificar volume de dados
SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT survey_id) as total_pesquisas,
    COUNT(CASE WHEN survey_status = 'completed' THEN 1 END) as pesquisas_respondidas,
    COUNT(CASE WHEN survey_status != 'completed' OR survey_status IS NULL THEN 1 END) as pesquisas_pendentes
FROM silver.vw_nps_respostas_envios_aniversario;

-- Query para verificar integridade do FULL OUTER JOIN
SELECT 
    CASE 
        WHEN xp_aniversario_nps_assessor IS NOT NULL AND survey_status IS NULL THEN 'Resposta sem envio'
        WHEN xp_aniversario_nps_assessor IS NULL AND survey_status IS NOT NULL THEN 'Envio sem resposta'
        WHEN xp_aniversario_nps_assessor IS NOT NULL AND survey_status IS NOT NULL THEN 'Completo'
        ELSE 'Indefinido'
    END as tipo_registro,
    COUNT(*) as quantidade
FROM silver.vw_nps_respostas_envios_aniversario
GROUP BY 
    CASE 
        WHEN xp_aniversario_nps_assessor IS NOT NULL AND survey_status IS NULL THEN 'Resposta sem envio'
        WHEN xp_aniversario_nps_assessor IS NULL AND survey_status IS NOT NULL THEN 'Envio sem resposta'
        WHEN xp_aniversario_nps_assessor IS NOT NULL AND survey_status IS NOT NULL THEN 'Completo'
        ELSE 'Indefinido'
    END;

-- Query para análise de duplicatas (não deve retornar registros)
SELECT survey_id, COUNT(*) as qtd
FROM silver.vw_nps_respostas_envios_aniversario
GROUP BY survey_id
HAVING COUNT(*) > 1;
*/

-- ==============================================================================
-- 10. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-20 | [Nome]         | Criação inicial da view

*/

-- ==============================================================================
-- 11. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- View usa FULL OUTER JOIN para capturar todos cenários
- ROW_NUMBER() garante apenas versão mais recente por survey_id
- COALESCE necessário devido ao FULL OUTER JOIN
- Classificações NPS seguem padrão internacional
- View não é materializada, performance depende das tabelas base

Cenários capturados:
1. Envio + Resposta: Fluxo completo normal
2. Envio sem Resposta: Cliente não respondeu
3. Resposta sem Envio: Possível erro de dados ou carga manual

Classificação NPS:
- Promotor: Notas 9-10 (altamente satisfeitos)
- Neutro: Notas 7-8 (satisfeitos mas não entusiasmados)
- Detrator: Notas 0-6 (insatisfeitos)

Campos importantes:
- survey_id: Chave única da pesquisa
- rn_resposta/rn_envio: Controle de versionamento (sempre = 1)
- data_carga: Campo usado para versionamento (não exposto na view)

Performance:
- Criar índices nas tabelas bronze por (survey_id, data_carga DESC)
- Considerar view materializada se volume > 100k registros
- Monitorar plano de execução para otimizações

Integração:
- Fonte para: prc_bronze_to_silver_fact_nps_respostas_envios_aniversario
- Atualização: Automática conforme bronze é atualizada
- Dependências: bronze.xp_nps_respostas, bronze.xp_nps_envios

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
