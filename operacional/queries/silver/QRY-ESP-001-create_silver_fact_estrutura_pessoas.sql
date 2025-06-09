SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [silver].[fact_estrutura_pessoas](
    [crm_id] [varchar](20) NOT NULL,
    [id_estrutura] [int] NOT NULL,
    [data_entrada] [date] NOT NULL,
    [data_saida] [date] NULL,
 CONSTRAINT [PK_fato_estrutura_pessoas] PRIMARY KEY CLUSTERED 
(
    [crm_id] ASC,
    [id_estrutura] ASC,
    [data_entrada] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [silver].[fact_estrutura_pessoas]  WITH NOCHECK ADD  CONSTRAINT [FK_fato_estrutura_pessoa_pessoas] FOREIGN KEY([crm_id])
REFERENCES [silver].[dim_pessoas] ([crm_id])
GO
ALTER TABLE [silver].[fact_estrutura_pessoas] NOCHECK CONSTRAINT [FK_fato_estrutura_pessoa_pessoas]
GO
ALTER TABLE [silver].[fact_estrutura_pessoas]  WITH NOCHECK ADD  CONSTRAINT [FK_fato_estrutura_pessoas_calendario] FOREIGN KEY([data_entrada])
REFERENCES [silver].[dim_calendario] ([data_ref])
GO
ALTER TABLE [silver].[fact_estrutura_pessoas] NOCHECK CONSTRAINT [FK_fato_estrutura_pessoas_calendario]
GO
ALTER TABLE [silver].[fact_estrutura_pessoas]  WITH NOCHECK ADD  CONSTRAINT [FK_fato_estrutura_pessoas_estrutura] FOREIGN KEY([id_estrutura])
REFERENCES [silver].[dim_estruturas] ([id_estrutura])
GO
ALTER TABLE [silver].[fact_estrutura_pessoas] CHECK CONSTRAINT [FK_fato_estrutura_pessoas_estrutura]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador único da pessoa no CRM' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_estrutura_pessoas', @level2type=N'COLUMN',@level2name=N'crm_id'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador único da estrutura organizacional' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_estrutura_pessoas', @level2type=N'COLUMN',@level2name=N'id_estrutura'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data em que a pessoa entrou na estrutura' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_estrutura_pessoas', @level2type=N'COLUMN',@level2name=N'data_entrada'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data em que a pessoa saiu da estrutura. nulo se ainda estiver na estrutura' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_estrutura_pessoas', @level2type=N'COLUMN',@level2name=N'data_saida'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela fato que contém o histórico de estrutura organizacional das pessoas. registra quando uma pessoa entrou e saiu de uma estrutura. não significa que a pessoa saiu da M7, pode ter apenas saído de uma estrutura.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_estrutura_pessoas'
GO