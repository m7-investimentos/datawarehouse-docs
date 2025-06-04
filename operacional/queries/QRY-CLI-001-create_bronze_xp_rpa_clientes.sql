CREATE TABLE [bronze].[bronze_xp_rpa_clientes] (
    [cod_xp] INT NULL,
    [nome_cliente] NVARCHAR(107) NULL,
    [telefone_cliente] NVARCHAR(50) NULL,
    [email_cliente] NVARCHAR(54) NULL,
    [patrimonio] NVARCHAR(50) NULL,
    [elegibilidade_cartao] NVARCHAR(50) NULL,
    [cpf_cnpj] NVARCHAR(50) NULL,
    [suitability] NVARCHAR(50) NULL,
    [fee_based] NVARCHAR(50) NULL,
    [segmento] NVARCHAR(50) NULL,
    [tipo_investidor] NVARCHAR(50) NULL,
    [cod_aai] NVARCHAR(50) NULL,
    [status_conta_digital] NVARCHAR(50) NULL,
    [produto] NVARCHAR(50) NULL,
    [data_carga] DATETIME2 NOT NULL DEFAULT GETDATE()
);