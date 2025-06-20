-- ==============================================================================
-- QRY-RES-002-prc_bronze_to_silver_fact_resgates
-- ==============================================================================
-- Tipo: PROCEDURE
-- Versão: 1.0.0
-- Última atualização: 2024-11-28
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [etl, bronze-to-silver, resgates, transferências]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por processar e carregar dados de resgates e 
transferências de saída da camada bronze para a camada silver. Consolida dados
de duas fontes principais: resgates diretos (xp_captacao) e transferências de
saída (xp_transferencia_clientes), calculando o valor patrimonial no momento
da transferência.

Casos de uso:
- Carga diária/periódica de dados de resgates para o DW
- Consolidação de múltiplas fontes de saída de recursos
- Cálculo automático de valores de transferência baseado em patrimônio
- Normalização de dados para análises unificadas

Frequência de execução: Diária (preferencialmente após 6h da manhã)
Tempo médio de execução: 2-5 minutos
Volume esperado de linhas: ~500-1000 registros/dia
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Nenhum parâmetro de entrada - Procedure processa todos os dados disponíveis
na camada bronze realizando carga completa (TRUNCATE + INSERT)
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela de destino: [silver].[fact_resgates]

Colunas populadas:
| Coluna                      | Origem                                     | Transformação                      |
|-----------------------------|--------------------------------------------|---------------------------------|
| data_ref                    | xp_captacao.data_ref / transferencia.data | Direta                          |
| conta_xp_cliente            | xp_captacao.cod_xp / transferencia.cod_xp | Direta                          |
| cod_assessor                | xp_captacao.cod_aai / cod_aai_origem      | Direta                          |
| origem_resgate              | tipo_de_captacao / 'Transferência'        | Categorização                   |
| resgate_bruto_xp            | valor_captacao (sinal = -1)                | Mantém negativo                 |
| tipo_transferencia          | 'N/A' / 'saída'                            | Categorização                   |
| resgate_bruto_transferencia | 0 / último net_em_M antes transferência   | Cálculo complexo                |
| resgate_bruto_total         | Calculado                                  | Soma dos componentes            |
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- bronze.xp_captacao: Fonte de dados de resgates diretos (sinal_captacao = -1)
- bronze.xp_transferencia_clientes: Fonte de transferências de saída
- bronze.xp_positivador: Fonte de patrimônio para cálculo de transferências
- silver.fact_resgates: Tabela de destino (TRUNCATE antes da carga)

Funções/Procedures chamadas:
- Nenhuma

Pré-requisitos:
- Tabelas bronze devem estar atualizadas
- Tabela silver.fact_resgates deve existir
- Permissões de TRUNCATE e INSERT na tabela de destino
- Índices recomendados em bronze.xp_positivador (cod_xp, data_ref)
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ==============================================================================
-- 6. CRIAÇÃO DA PROCEDURE
-- ==============================================================================
CREATE   PROCEDURE [silver].[prc_bronze_to_silver_fact_resgates]
AS
BEGIN
    SET NOCOUNT ON;

    -- ==============================================================================
    -- 7. LÓGICA DE PROCESSAMENTO
    -- ==============================================================================
    
    -- Limpa a tabela silver (carga completa)
    TRUNCATE TABLE [M7Medallion].[silver].[fact_resgates];

    -- ==============================================================================
    -- 8. CTEs (COMMON TABLE EXPRESSIONS)
    -- ==============================================================================
    WITH 
    -- CTE para resgates (apenas saídas)
    ResgatesSaida AS (
        SELECT 
            c.data_ref,
            c.cod_xp AS conta_xp_cliente,
            c.cod_aai AS cod_assessor,
            c.tipo_de_captacao AS origem_resgate,
            c.valor_captacao AS resgate_bruto, -- Mantém valor negativo original
            'N/A' AS tipo_transferencia,
            0 AS resgate_bruto_transferencia
        FROM [M7Medallion].[bronze].[xp_captacao] c
        WHERE c.sinal_captacao = -1 -- Apenas resgates/saídas
    ),
    
    -- CTE para transferências de saída (origem existe, destino não existe)
    TransferenciasSaida AS (
        SELECT *
        FROM [M7Medallion].[bronze].[xp_transferencia_clientes]
        WHERE [status] = 'CONCLUIDO'
          AND cod_aai_origem IS NOT NULL 
          AND (cod_aai_destino IS NULL OR cod_aai_destino = '')
    ),
    
    -- CTE para processar transferências de saída com valores
    TransferenciasSaidaProcessadas AS (
        SELECT 
            t.data_transferencia AS data_ref,
            t.cod_xp AS conta_xp_cliente,
            t.cod_aai_origem AS cod_assessor,
            'Transferência' AS origem_resgate,
            0 AS resgate_bruto,
            'saída' AS tipo_transferencia,
            -ISNULL(pos_saida.net_em_M, 0) AS resgate_bruto_transferencia -- Valor negativo
        FROM TransferenciasSaida t
        OUTER APPLY (
            -- Para transferências de saída: último valor do cliente antes da data de transferência
            -- Pega o último net_em_M da temporada anterior à saída
            SELECT TOP 1 net_em_M
            FROM [M7Medallion].[bronze].[xp_positivador] p
            WHERE p.cod_xp = t.cod_xp
              AND p.data_ref <= t.data_transferencia
            ORDER BY p.data_ref DESC
        ) pos_saida
    ),
    
    -- CTE para unir resgates e transferências de saída
    DadosConsolidados AS (
        -- Resgates
        SELECT 
            data_ref,
            conta_xp_cliente,
            cod_assessor,
            origem_resgate,
            resgate_bruto,
            tipo_transferencia,
            resgate_bruto_transferencia
        FROM ResgatesSaida
        
        UNION ALL
        
        -- Transferências de saída
        SELECT 
            data_ref,
            conta_xp_cliente,
            cod_assessor,
            origem_resgate,
            resgate_bruto,
            tipo_transferencia,
            resgate_bruto_transferencia
        FROM TransferenciasSaidaProcessadas
    )
    
    -- ==============================================================================
    -- 9. QUERY PRINCIPAL (INSERT)
    -- ==============================================================================
    INSERT INTO [M7Medallion].[silver].[fact_resgates]
    (
        data_ref,
        conta_xp_cliente,
        cod_assessor,
        origem_resgate,
        resgate_bruto_xp,
        tipo_transferencia,
        resgate_bruto_transferencia,
        resgate_bruto_total
    )
    SELECT 
        data_ref,
        conta_xp_cliente,
        cod_assessor,
        origem_resgate,
        resgate_bruto,
        tipo_transferencia,
        resgate_bruto_transferencia,
        (resgate_bruto + resgate_bruto_transferencia) AS resgate_bruto_total
    FROM DadosConsolidados
    WHERE 
        -- Garantir que temos dados válidos
        conta_xp_cliente IS NOT NULL
        AND cod_assessor IS NOT NULL
        AND (resgate_bruto < 0 OR resgate_bruto_transferencia < 0)
    ORDER BY data_ref, conta_xp_cliente;

END;
GO

-- ==============================================================================
-- 10. TRATAMENTO DE ERROS
-- ==============================================================================
/*
A procedure não implementa tratamento explícito de erros (TRY/CATCH).
Erros são propagados para o processo chamador.

Erros comuns:
- Violação de constraints: Verificar dados NULL em campos obrigatórios
- Timeout: Otimizar índices em bronze.xp_positivador
- Estouro de precisão: Validar valores extremos em decimal(18,2)
*/

-- ==============================================================================
-- 11. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2024-11-28 | [Nome]         | Criação inicial da procedure

*/

-- ==============================================================================
-- 12. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Procedure realiza carga COMPLETA (truncate/load) - não incremental
- Transferências de saída são identificadas por cod_aai_destino NULL ou vazio
- OUTER APPLY é usado para buscar o último patrimônio antes da transferência
- Todos os valores são mantidos/convertidos para NEGATIVO
- Filtro final garante apenas registros com valores negativos

Performance:
- Considerar particionar bronze.xp_positivador por data_ref
- Criar índice em bronze.xp_transferencia_clientes (status, cod_aai_origem)
- Monitorar tempo de execução do OUTER APPLY

Validações recomendadas:
1. Verificar se todas as transferências têm patrimônio correspondente
2. Validar sinais negativos após carga
3. Comparar totais com relatórios oficiais

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
