CREATE TABLE [M7Medallion].[silver].[silver_dim_calendario] (
    data_ref            DATE           NOT NULL,
    dia                 TINYINT        NOT NULL,
    mes                 TINYINT        NOT NULL,
    ano                 SMALLINT       NOT NULL,
    ano_mes             CHAR(6)        NOT NULL,
    nome_mes            VARCHAR(20)    NOT NULL,
    trimestre           CHAR(2)        NOT NULL,
    numero_da_semana    TINYINT        NOT NULL,
    dia_da_semana       VARCHAR(20)    NOT NULL,
    dia_da_semana_num   TINYINT        NOT NULL,
    tipo_dia            VARCHAR(15)    NOT NULL,
    observacoes         VARCHAR(200)   NULL,
    CONSTRAINT PK_dim_calendario PRIMARY KEY (data_ref)
);
GO

-- descricao da tabela
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela dimensão que contém as informações de calendário para análise temporal' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_calendario';
GO

-- descricoes
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do calendário' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'data_ref';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'dia do mês (1-31)' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'dia';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'mês do ano (1-12)' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'mes';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano de referência' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'ano';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano e mês no formato AAAAMM' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'ano_mes';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome do mês por extenso' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'nome_mes';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'trimestre do ano (Q1-Q4)' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'trimestre';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'número da semana no ano (1-53)' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'numero_da_semana';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome do dia da semana por extenso' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'dia_da_semana';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'número do dia da semana (1=segunda, 7=domi)' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'dia_da_semana_num';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tipo do dia (útil, sábado, domi, feriado)' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'tipo_dia';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'observações adicionais sobre a data' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'observacoes'; 

