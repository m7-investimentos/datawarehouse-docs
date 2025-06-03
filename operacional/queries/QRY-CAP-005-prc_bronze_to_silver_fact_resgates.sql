USE [M7Medallion];
GO

CREATE OR ALTER PROCEDURE [dbo].[prc_bronze_to_silver_fact_resgates]
AS
BEGIN
    SET NOCOUNT ON;

    -- Limpa a tabela silver
    TRUNCATE TABLE [M7Medallion].[silver].[silver_fact_resgates];

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
        FROM [M7Medallion].[bronze].[bronze_xp_captacao] c
        WHERE c.sinal_captacao = -1 -- Apenas resgates/saídas
    ),
    
    -- CTE para transferências de saída (origem existe, destino não existe)
    TransferenciasSaida AS (
        SELECT *
        FROM [M7Medallion].[bronze].[bronze_xp_transferencia_clientes]
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
            FROM [M7Medallion].[bronze].[bronze_xp_positivador] p
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
    
    -- Insert final na tabela silver
    INSERT INTO [M7Medallion].[silver].[silver_fact_resgates]
    (
        data_ref,
        conta_xp_cliente,
        cod_assessor,
        origem_resgate,
        resgate_bruto,
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



