SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [silver].[fact_ativacoes_habilitacoes_evasoes](
	[data_ref] [date] NOT NULL,
	[cod_xp] [int] NOT NULL,
	[crm_id] [varchar](20) NOT NULL,
	[id_estrutura] [int] NOT NULL,
	[faixa_pl] [varchar](50) NULL,
	[tipo_movimentacao] [varchar](50) NULL
) ON [PRIMARY]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do registro' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código do cliente no sistema xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes', @level2type=N'COLUMN',@level2name=N'cod_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador do assessor no crm' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes', @level2type=N'COLUMN',@level2name=N'crm_id'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador da estrutura no sistema' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes', @level2type=N'COLUMN',@level2name=N'id_estrutura'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'faixa de patrimônio líquido do cliente' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes', @level2type=N'COLUMN',@level2name=N'faixa_pl'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tipo de movimentação do registro (ativação, habilitação ou evasão)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes', @level2type=N'COLUMN',@level2name=N'tipo_movimentacao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela fatos que contém os registros de ativação, habilitação e evasão de clientes' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_ativacoes_habilitacoes_evasoes'
GO
