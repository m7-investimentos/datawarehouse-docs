-- ==============================================================================
-- QRY-CAP-003-PRC_BRONZE_TO_SILVER_FACT_CAPTACAO_BRUTA
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [etl, bronze_to_silver, procedure, captacao, captacao_bruta]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por realizar a carga de dados da camada bronze 
           para a silver, processando registros de captação bruta de clientes.
           Consolida dados de captações da XP e transferências de entrada,
           classificando-as como 'nova conta' ou 'transferência de escritório'.

Casos de uso:
- Carga diária de dados de captação bruta
- Atualização completa da tabela fact_captacao_bruta
- Parte do processo ETL bronze -> silver
- Consolidação de captações e transferências

Frequência de execução: Diária
Tempo médio de execução: 5-10 minutos
Volume esperado de linhas: ~5.000 registros/dia
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros: Nenhum (full load)

Obs: A procedure realiza carga completa (TRUNCATE/INSERT), 
     não possui parâmetros de data para carga incremental.
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela de destino: [silver].[fact_captacao_bruta]

Colunas carregadas:
- data_ref: Data de referência do registro
- conta_xp_cliente: Código da conta do cliente no sistema XP
- cod_assessor: Código do assessor no sistema da XP
- origem_captacao: Origem da captação (TED, PREV, OTA, Transferência)
- captacao_bruta_xp: Valor da captação parcial ótica XP
- tipo_transferencia: Tipo de transferência (nova conta/transferência escritório/N/A)
- captacao_bruta_transferencia: Valor da transferência de entrada
- captacao_bruta_total: Soma de captacao_bruta_xp + captacao_bruta_transferencia
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas de origem (INPUT):
- [M7Medallion].[bronze].[xp_captacao]: Dados brutos de captações da XP
  * Campos utilizados: data_ref, cod_xp, cod_aai, tipo_de_captacao, valor_captacao, sinal_captacao
  * Filtro aplicado: WHERE sinal_captacao = 1 (apenas entradas)
  * Volume esperado: ~3.000 registros/dia
  * Uso: CTE CaptacoesEntrada
  
- [M7Medallion].[bronze].[xp_transferencia_clientes]: Dados de transferências entre assessores
  * Campos utilizados: data_transferencia, cod_xp, cod_aai_origem, cod_aai_destino, status
  * Filtro aplicado: WHERE status = 'CONCLUIDO'
  * Volume esperado: ~500 registros/dia
  * Uso: CTE TransferenciasConcluidas
  
- [M7Medallion].[bronze].[xp_positivador]: Dados de patrimônio e cadastro de clientes
  * Campos utilizados: cod_xp, data_ref, data_cadastro, net_em_M
  * Uso múltiplo:
    - CTE PrimeiraDataCliente: MIN(data_ref) e MIN(data_cadastro) por cliente
    - CTE PrimeiroNetClienteNovaConta: Primeiro valor de patrimônio
    - OUTER APPLY: Busca patrimônio após transferência
  * Volume: Tabela histórica grande (milhões de registros)

Tabela de destino (OUTPUT):
- [M7Medallion].[silver].[fact_captacao_bruta]: Tabela fato de captação bruta
  * Operação: TRUNCATE seguido de INSERT
  * Modo: Full load (carga completa)
  * Volume final: ~5.000 registros/dia

Pré-requisitos:
- Tabelas bronze devem estar atualizadas com dados do dia
- Database M7Medallion deve ser acessível (nome completo usado)
- Usuário deve ter permissões: TRUNCATE, INSERT, SELECT
- Índices recomendados:
  * bronze.xp_positivador: índice em (cod_xp, data_ref)
  * bronze.xp_transferencia_clientes: índice em (status, data_transferencia)
*/

-- ==============================================================================
-- 5. SCRIPT DA PROCEDURE
-- ==============================================================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [silver].[prc_bronze_to_silver_fact_captacao_bruta]
AS
BEGIN
    SET NOCOUNT ON;

    -- ==============================================================================
    -- 5.1. TRUNCATE DA TABELA DE DESTINO
    -- ==============================================================================
    -- Limpa a tabela silver
    TRUNCATE TABLE [M7Medallion].[silver].[fact_captacao_bruta];

    -- ==============================================================================
    -- 5.2. PROCESSAMENTO DOS DADOS COM CTEs
    -- ==============================================================================
    WITH 
    -- CTE para captações (apenas entradas)
    CaptacoesEntrada AS (
        SELECT 
            c.data_ref,
            c.cod_xp AS conta_xp_cliente,
            c.cod_aai AS cod_assessor,
            c.tipo_de_captacao AS origem_captacao,
            c.valor_captacao AS captacao_bruta_xp,
            'N/A' AS tipo_transferencia,
            0 AS captacao_bruta_transferencia
        FROM [M7Medallion].[bronze].[xp_captacao] c
        WHERE c.sinal_captacao = 1 -- Apenas captações de entrada
    ),
    
    -- CTE para transferências concluídas
    TransferenciasConcluidas AS (
        SELECT *
        FROM [M7Medallion].[bronze].[xp_transferencia_clientes]
        WHERE [status] = 'CONCLUIDO'
    ),
    
    -- CTE para primeira data de cada cliente no positivador (para nova conta)
    PrimeiraDataCliente AS (
        SELECT 
            cod_xp,
            MIN(data_ref) as primeira_data,
            MIN(data_cadastro) as data_cadastro
        FROM [M7Medallion].[bronze].[xp_positivador]
        GROUP BY cod_xp
    ),
    
    -- CTE para primeira data e valor net_em_M de cada cliente (para nova conta)
    PrimeiroNetClienteNovaConta AS (
        SELECT 
            p.cod_xp,
            p.net_em_M
        FROM [M7Medallion].[bronze].[xp_positivador] p
        INNER JOIN PrimeiraDataCliente pdc 
            ON p.cod_xp = pdc.cod_xp 
            AND p.data_ref = pdc.primeira_data
    ),
    
    -- CTE para classificar transferências e calcular valores
    TransferenciasProcessadas AS (
        SELECT 
            t.data_transferencia AS data_ref,
            t.cod_xp AS conta_xp_cliente,
            t.cod_aai_destino AS cod_assessor,
            'Transferência' AS origem_captacao,
            0 AS captacao_bruta_xp,
            CASE
                -- Nova conta: cliente transferido até 30 dias após cadastro
                WHEN ABS(DATEDIFF(DAY, pdc.data_cadastro, t.data_transferencia)) <= 30 
                    THEN 'nova conta'
                -- Transferência de escritório: cliente sem assessor origem e com assessor destino
                WHEN ABS(DATEDIFF(DAY, pdc.data_cadastro, t.data_transferencia)) > 30 
                     AND (t.cod_aai_origem IS NULL OR t.cod_aai_origem = '')
                     AND t.cod_aai_destino IS NOT NULL
                    THEN 'transferencia de escritorio'
                ELSE 'outros'
            END AS tipo_transferencia,
            CASE
                -- Para nova conta: usar primeira data histórica
                WHEN ABS(DATEDIFF(DAY, pdc.data_cadastro, t.data_transferencia)) <= 30 
                    THEN ISNULL(pnc_nova.net_em_M, 0)
                -- Para transferência de escritório: usar primeira data após transferência
                WHEN ABS(DATEDIFF(DAY, pdc.data_cadastro, t.data_transferencia)) > 30 
                    THEN ISNULL(pos_escritorio.net_em_M, 0)
                ELSE 0
            END AS captacao_bruta_transferencia
        FROM TransferenciasConcluidas t
        LEFT JOIN PrimeiraDataCliente pdc ON t.cod_xp = pdc.cod_xp
        LEFT JOIN PrimeiroNetClienteNovaConta pnc_nova ON t.cod_xp = pnc_nova.cod_xp
        OUTER APPLY (
            -- Busca o primeiro valor de patrimônio após a transferência
            SELECT TOP 1 net_em_M
            FROM [M7Medallion].[bronze].[xp_positivador] p
            WHERE p.cod_xp = t.cod_xp
              AND p.data_ref >= t.data_transferencia
            ORDER BY p.data_ref ASC
        ) pos_escritorio
        WHERE 
            pdc.data_cadastro IS NOT NULL
    ),
    
    -- CTE para consolidar todos os dados
    DadosConsolidados AS (
        -- Captações
        SELECT 
            data_ref,
            conta_xp_cliente,
            cod_assessor,
            origem_captacao,
            captacao_bruta_xp,
            tipo_transferencia,
            captacao_bruta_transferencia
        FROM CaptacoesEntrada
        
        UNION ALL
        
        -- Transferências válidas (apenas nova conta e transferencia de escritorio)
        SELECT 
            data_ref,
            conta_xp_cliente,
            cod_assessor,
            origem_captacao,
            captacao_bruta_xp,
            tipo_transferencia,
            captacao_bruta_transferencia
        FROM TransferenciasProcessadas
        WHERE tipo_transferencia IN ('nova conta', 'transferencia de escritorio')
    )
    
    -- ==============================================================================
    -- 5.3. INSERT FINAL NA TABELA SILVER
    -- ==============================================================================
    -- Insert final na tabela silver
    INSERT INTO [M7Medallion].[silver].[fact_captacao_bruta]
    (
        data_ref,
        conta_xp_cliente,
        cod_assessor,
        origem_captacao,
        captacao_bruta_xp,
        tipo_transferencia,
        captacao_bruta_transferencia,
        captacao_bruta_total
    )
    SELECT 
        data_ref,
        conta_xp_cliente,
        cod_assessor,
        origem_captacao,
        captacao_bruta_xp,
        tipo_transferencia,
        captacao_bruta_transferencia,
        (captacao_bruta_xp + captacao_bruta_transferencia) AS captacao_bruta_total
    FROM DadosConsolidados
    WHERE 
        -- Garantir que temos dados válidos
        conta_xp_cliente IS NOT NULL
        AND cod_assessor IS NOT NULL
        AND (captacao_bruta_xp > 0 OR captacao_bruta_transferencia > 0)
    ORDER BY data_ref, conta_xp_cliente;

END;
GO

-- ==============================================================================
-- 6. TRATAMENTO DE ERROS
-- ==============================================================================
/*
Notas sobre tratamento de erros:
- A procedure não possui tratamento explícito de erros (TRY/CATCH)
- Em caso de erro, a transação será revertida automaticamente
- Logs de erro devem ser verificados no SQL Server Agent

Recomendação: Implementar bloco TRY/CATCH para melhor controle de erros
*/

-- ==============================================================================
-- 7. SCRIPTS DE VALIDAÇÃO
-- ==============================================================================
/*
-- Verificar quantidade de registros carregados
SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT conta_xp_cliente) as clientes_unicos,
    COUNT(DISTINCT cod_assessor) as assessores_unicos,
    MIN(data_ref) as data_inicial,
    MAX(data_ref) as data_final,
    SUM(captacao_bruta_total) as volume_total
FROM [silver].[fact_captacao_bruta];

-- Verificar distribuição por origem de captação
SELECT 
    origem_captacao,
    COUNT(*) as quantidade,
    SUM(captacao_bruta_total) as volume_total
FROM [silver].[fact_captacao_bruta]
GROUP BY origem_captacao
ORDER BY volume_total DESC;

-- Verificar distribuição por tipo de transferência
SELECT 
    tipo_transferencia,
    COUNT(*) as quantidade,
    SUM(captacao_bruta_transferencia) as volume_transferencia
FROM [silver].[fact_captacao_bruta]
WHERE origem_captacao = 'Transferência'
GROUP BY tipo_transferencia;

-- Verificar registros com possíveis problemas
SELECT COUNT(*) as registros_suspeitos
FROM [silver].[fact_captacao_bruta]
WHERE captacao_bruta_total != (captacao_bruta_xp + captacao_bruta_transferencia);
*/

-- ==============================================================================
-- 8. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-20 | [Nome]         | Criação inicial da procedure

*/

-- ==============================================================================
-- 9. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Procedure realiza TRUNCATE/INSERT (full load), não incremental
- Apenas captações com sinal_captacao = 1 são consideradas (entradas)
- Transferências são classificadas como:
  * 'nova conta': até 30 dias após cadastro
  * 'transferencia de escritorio': sem assessor origem
  * 'outros': não são carregadas
- OUTER APPLY é usado para buscar o primeiro patrimônio após transferência
- Database M7Medallion é usado com nome completo para evitar conflitos

Regras de negócio:
1. Captações: apenas entradas (sinal_captacao = 1)
2. Transferências: apenas status 'CONCLUIDO'
3. Nova conta: cliente transferido até 30 dias após cadastro
4. Transferência escritório: cliente sem assessor origem
5. Apenas registros com valores > 0 são carregados

Possíveis melhorias:
1. Implementar carga incremental baseada em data_ref
2. Adicionar tratamento de erros com TRY/CATCH
3. Incluir log de execução em tabela de controle
4. Otimizar OUTER APPLY com índices apropriados
5. Adicionar parâmetro para reprocessar período específico

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
