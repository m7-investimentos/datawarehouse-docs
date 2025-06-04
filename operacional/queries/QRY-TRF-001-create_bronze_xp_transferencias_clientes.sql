-- 1. Criar a nova tabela no schema bronze
CREATE TABLE [M7Medallion].[bronze].[bronze_xp_transferencia_clientes](
	[cod_xp] [int] NULL,
	[cod_aai_origem] [varchar](50) NULL,
	[cod_aai_destino] [varchar](50) NULL,
	[data_solicitacao] [date] NULL,
	[data_transferencia] [date] NULL,
	[status] [varchar](50) NULL,
	[data_carga] [date] NOT NULL DEFAULT (CONVERT([date],getdate()))
);