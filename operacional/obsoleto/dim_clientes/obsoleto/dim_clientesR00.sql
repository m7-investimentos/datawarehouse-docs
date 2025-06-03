-- CLIENTES OK

CREATE TABLE [M7InvestimentosOLAP].[dbo].[dim_clientes] (
    cod_xp                   int            NOT NULL,
    nome                     VARCHAR(250)   NOT NULL DEFAULT '',
    status_cliente           BIT            NULL,
    profissao                VARCHAR(100)   NULL,
    sexo                     CHAR(1)        NULL,
    tipo_conta               VARCHAR(2)     NULL,
    PL_atual                 DECIMAL(18,4)  NULL,
    PL_max                   DECIMAL(18,4)  NULL,
    PL_media                 DECIMAL(18,4)  NULL,
    primeiro_assessor_m7     VARCHAR(20)    NULL

    CONSTRAINT PK_dim_clientes PRIMARY KEY (cod_xp)
);
GO

-- Alterações de coluna
ALTER TABLE [M7InvestimentosOLAP].[dbo].[dim_clientes]
ALTER COLUMN profissao VARCHAR(250) NULL;

-- Adicionar nova coluna
ALTER TABLE [M7InvestimentosOLAP].[dbo].[dim_clientes]
ADD segmento VARCHAR(250) NULL;




-- UPDATE c
-- -- insere os dados da última data_ref da tabela de origem
-- SET c.status_cliente = ISNULL(e.status_cliente, 0)
-- FROM [M7Investimentos].[dbo].[pap_dim_clientes] c
-- LEFT JOIN (
--     SELECT cod_xp, status_cliente
--     FROM [M7Investimentos].[dbo].[pap_positivador_enriquecido]
--     WHERE data_ref = (
--         SELECT MAX(data_ref)
--         FROM [M7Investimentos].[dbo].[pap_positivador_enriquecido]
--     )
-- ) e ON c.cod_xp = e.cod_xp;

