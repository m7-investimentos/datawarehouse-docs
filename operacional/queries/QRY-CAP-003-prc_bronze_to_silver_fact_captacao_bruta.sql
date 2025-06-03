USE [M7Medallion];
GO

CREATE OR ALTER PROCEDURE [dbo].[prc_bronze_to_silver_fact_captacao_bruta]
AS
BEGIN
    SET NOCOUNT ON;

    -- Limpa a tabela silver
    TRUNCATE TABLE [M7Medallion].[silver].[silver_fact_captacao_bruta];

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
        FROM [M7Medallion].[bronze].[bronze_xp_captacao] c
        WHERE c.sinal_captacao = 1 -- Apenas captações de entrada
    ),
    
    -- CTE para transferências concluídas
    TransferenciasConcluidas AS (
        SELECT *
        FROM [M7Medallion].[bronze].[bronze_xp_transferencia_clientes]
        WHERE [status] = 'CONCLUIDO'
    ),
    
    -- CTE para primeira data de cada cliente no positivador (para nova conta)
    PrimeiraDataCliente AS (
        SELECT 
            cod_xp,
            MIN(data_ref) as primeira_data,
            MIN(data_cadastro) as data_cadastro
        FROM [M7Medallion].[bronze].[bronze_xp_positivador]
        GROUP BY cod_xp
    ),
    
    -- CTE para primeira data e valor net_em_M de cada cliente (para nova conta)
    PrimeiroNetClienteNovaConta AS (
        SELECT 
            p.cod_xp,
            p.net_em_M
        FROM [M7Medallion].[bronze].[bronze_xp_positivador] p
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
                WHEN ABS(DATEDIFF(DAY, pdc.data_cadastro, t.data_transferencia)) <= 30 
                    THEN 'nova conta'
                WHEN ABS(DATEDIFF(DAY, pdc.data_cadastro, t.data_transferencia)) > 30 
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
            -- Para transferências de escritório: primeiro valor após a data de transferência menos 5 dias
            -- Isso evita pegar o primeiro valor histórico, focando na nova entrada após retorno
            SELECT TOP 1 net_em_M
            FROM [M7Medallion].[bronze].[bronze_xp_positivador] p
            WHERE p.cod_xp = t.cod_xp
              AND p.data_ref > DATEADD(DAY, -5, t.data_transferencia)
            ORDER BY p.data_ref ASC
        ) pos_escritorio
        WHERE 
            pdc.data_cadastro IS NOT NULL
    ),
    
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
    
    -- Insert final na tabela silver
    INSERT INTO [M7Medallion].[silver].[silver_fact_captacao_bruta]
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