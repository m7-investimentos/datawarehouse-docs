SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [silver].[dim_pessoas](
	[crm_id] [varchar](20) NOT NULL,
	[nome_pessoa] [varchar](200) NOT NULL,
	[cod_aai] [varchar](50) NULL,
	[id_avenue] [varchar](50) NULL,
	[id_rd_station] [varchar](50) NULL,
	[data_nascimento] [date] NULL,
	[data_inicio_vigencia] [date] NOT NULL,
	[data_fim_vigencia] [date] NULL,
	[email_multisete] [varchar](200) NULL,
	[email_xp] [varchar](200) NULL,
	[observacoes] [varchar](200) NULL,
	[assessor_nivel] [varchar](50) NULL,
 CONSTRAINT [PK_dim_pessoas] PRIMARY KEY CLUSTERED 
(
	[crm_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador único da pessoa no crm' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'crm_id'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome completo da pessoa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'nome_pessoa'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código da pessoa no sistema da xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'cod_aai'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador da pessoa no sistema avenue' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'id_avenue'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador da pessoa no sistema rd station' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'id_rd_station'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de nascimento da pessoa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'data_nascimento'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de início da vigência da pessoa, quando entrou na empresa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'data_inicio_vigencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de fim da vigência do registro, quando saiu da empresa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'data_fim_vigencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'email da pessoa multisete' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'email_multisete'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'email da pessoa no domínio xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'email_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'observações adicionais sobre a pessoa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'observacoes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nível do assessor, se o assessor é junior, pleno ou senior' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'assessor_nivel'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela dimensão que contém as informações cadastrais dos colaboradores, assessores e funcionarios no geral' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas'
GO
