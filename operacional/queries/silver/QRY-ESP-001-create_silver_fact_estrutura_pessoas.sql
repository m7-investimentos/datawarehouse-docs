CREATE TABLE  [M7Medallion].[silver].[silver_fact_estrutura_pessoas] (
    crm_id                  VARCHAR(20)  NOT NULL,
    id_estrutura            INT          NOT NULL,
    data_entrada            DATE         NOT NULL,
    data_saida              DATE         NULL,

);
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela fato que contém o histórico de estrutura organizacional das pessoas. registra quando uma pessoa entrou e saiu de uma estrutura. não significa que a pessoa saiu da M7, pode ter apenas saído de uma estrutura.' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_estrutura_pessoas';
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador único da pessoa no CRM' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_estrutura_pessoas', @level2type=N'COLUMN',@level2name=N'crm_id';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador único da estrutura organizacional' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_estrutura_pessoas', @level2type=N'COLUMN',@level2name=N'id_estrutura';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data em que a pessoa entrou na estrutura' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_estrutura_pessoas', @level2type=N'COLUMN',@level2name=N'data_entrada';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data em que a pessoa saiu da estrutura. nulo se ainda estiver na estrutura' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_estrutura_pessoas', @level2type=N'COLUMN',@level2name=N'data_saida';
GO




