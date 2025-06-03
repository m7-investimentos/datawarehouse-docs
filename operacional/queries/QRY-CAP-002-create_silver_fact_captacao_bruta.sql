CREATE TABLE [M7Medallion].[silver].[silver_fact_captacao_bruta]
(
    data_ref DATE NOT NULL,
    conta_xp_cliente INT NOT NULL,
    cod_assessor VARCHAR(50) NOT NULL,
    origem_captacao VARCHAR(100) NOT NULL,
    valor_captacao DECIMAL(18,2) NOT NULL,
);

-- adicionar comentarios
EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'tabela silver que contém os dados de captação bruta por cliente com seu respectivo assessor aai. apenas os valores de captação bruta são considerados nessa tabela, os valores de resgate não são considerados. essa tabela é oriunda dos relatórios de operações no hub da xp' , 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_captacao_bruta';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'data de referência do registro' , 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_captacao_bruta', 
@level2type=N'COLUMN',
@level2name=N'data_ref';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'código da conta do cliente no sistema xp' , 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_captacao_bruta', 
@level2type=N'COLUMN',
@level2name=N'conta_xp_cliente';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'código do assessor no sistema da xp' , 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_captacao_bruta', 
@level2type=N'COLUMN',
@level2name=N'cod_assessor';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'origem da captação (ted, prev, ota....)' , 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_captacao_bruta', 
@level2type=N'COLUMN',
@level2name=N'origem_captacao';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'valor da captação bruta' , 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_captacao_bruta', 
@level2type=N'COLUMN',
@level2name=N'valor_captacao';
GO



