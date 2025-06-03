CREATE TABLE [M7Medallion].[silver].[silver_fact_resgates]
(
    data_ref DATE NOT NULL,
    conta_xp_cliente INT NOT NULL,
    cod_assessor VARCHAR(50) NOT NULL,
    origem_resgate VARCHAR(100) NOT NULL,
    resgate_bruto DECIMAL(18,2) NOT NULL,
    tipo_transferencia VARCHAR(100) NOT NULL,
    resgate_bruto_transferencia DECIMAL(18,2) NOT NULL,
    resgate_bruto_total DECIMAL(18,2) NOT NULL,
);

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'tabela silver que contém os dados de resgates e transferencias de saida. apenas os valores de saida são considerados nessa tabela', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_resgates';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'data de referência do registro' , 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_resgates', 
@level2type=N'COLUMN',
@level2name=N'data_ref';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'código da conta do cliente no sistema xp' , 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_resgates', 
@level2type=N'COLUMN',
@level2name=N'conta_xp_cliente';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'código do assessor no sistema da xp' , 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_resgates', 
@level2type=N'COLUMN',
@level2name=N'cod_assessor';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'origem do resgate (ted, prev, ota....)' , 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_resgates', 
@level2type=N'COLUMN',
@level2name=N'origem_resgate';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'valor dos resgates brutos', 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_resgates', 
@level2type=N'COLUMN',
@level2name=N'resgate_bruto';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'tipo de transferência de saida, nesse caso, todas sao saida' , 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_resgates', 
@level2type=N'COLUMN',
@level2name=N'tipo_transferencia';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'valor da transferência de saida. quanto de patrimonio o cliente tinha quando foi transferido para a M7' , 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_resgates', 
@level2type=N'COLUMN',
@level2name=N'resgate_bruto_transferencia';
GO

EXEC sys.sp_addextendedproperty 
@name=N'MS_Description', 
@value=N'resgate bruto = valor dos resgates brutos + valor da transferência de saida' , 
@level0type=N'SCHEMA',
@level0name=N'silver', 
@level1type=N'TABLE',
@level1name=N'silver_fact_resgates', 
@level2type=N'COLUMN',
@level2name=N'resgate_bruto_total';
GO

