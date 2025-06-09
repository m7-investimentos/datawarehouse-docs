SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [silver].[dim_calendario](
    [data_ref] [date] NOT NULL,
    [dia] [tinyint] NOT NULL,
    [mes] [smallint] NULL,
    [ano] [smallint] NOT NULL,
    [ano_mes] [char](6) NOT NULL,
    [nome_mes] [varchar](20) NOT NULL,
    [trimestre] [char](2) NOT NULL,
    [numero_da_semana] [tinyint] NOT NULL,
    [dia_da_semana] [varchar](20) NOT NULL,
    [dia_da_semana_num] [tinyint] NOT NULL,
    [tipo_dia] [varchar](15) NOT NULL,
    [observacoes] [varchar](200) NULL,
 CONSTRAINT [PK_dim_calendario] PRIMARY KEY CLUSTERED 
(
    [data_ref] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do calendário' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'dia do mês (1-31)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'dia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'mês do ano (1-12)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano de referência' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano e mês no formato AAAAMM' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'ano_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome do mês por extenso' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'nome_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'trimestre do ano (Q1-Q4)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'número da semana no ano (1-53)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'numero_da_semana'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome do dia da semana por extenso' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'dia_da_semana'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'número do dia da semana (1=segunda, 7=domingo)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'dia_da_semana_num'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tipo do dia (útil, sábado, domingo, feriado)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'tipo_dia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'observações adicionais sobre a data' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'observacoes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela dimensão que contém as informações de calendário para análise temporal' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario'
GO