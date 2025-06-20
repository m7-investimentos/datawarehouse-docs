SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [silver].[dim_estruturas](
	[id_estrutura] [int] NOT NULL,
	[nome_estrutura] [varchar](100) NOT NULL,
	[estrutura_pai] [int] NULL,
	[observacoes] [varchar](200) NULL,
	[crm_id_lider] [varchar](20) NULL,
 CONSTRAINT [PK_dim_estruturas] PRIMARY KEY CLUSTERED 
(
	[id_estrutura] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador único da estrutura organizacional' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_estruturas', @level2type=N'COLUMN',@level2name=N'id_estrutura'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome da estrutura organizacional' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_estruturas', @level2type=N'COLUMN',@level2name=N'nome_estrutura'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador da estrutura pai (auto-relacionamento)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_estruturas', @level2type=N'COLUMN',@level2name=N'estrutura_pai'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'observações adicionais sobre a estrutura organizacional' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_estruturas', @level2type=N'COLUMN',@level2name=N'observacoes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador do líder da estrutura no crm' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_estruturas', @level2type=N'COLUMN',@level2name=N'crm_id_lider'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela dimensão que contém as informações da estrutura organizacional da empresa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_estruturas'
GO
