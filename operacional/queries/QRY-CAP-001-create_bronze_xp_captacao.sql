CREATE TABLE [M7Medallion].[bronze].[bronze_xp_captacao] (
    data_ref DATE NOT NULL,
    cod_xp INT NOT NULL,
    cod_aai VARCHAR(50) NOT NULL,
    tipo_de_captacao VARCHAR(100) NOT NULL,
    sinal_captacao INT NOT NULL,
    valor_captacao DECIMAL(18,2) NOT NULL,
    data_carga DATETIME DEFAULT GETDATE(),

);



EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela stage que contém os dados de captação por cliente, tipo e se é de saida ou entrada. essa tabela é oriunda dos relatórios de operações no hub da xp' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_captacao_layer1';
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do registro' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_captacao_layer1', @level2type=N'COLUMN',@level2name=N'data_ref';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código do cliente no sistema xp' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_captacao_layer1', @level2type=N'COLUMN',@level2name=N'cod_xp';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código do assessor no sistema aai' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_captacao_layer1', @level2type=N'COLUMN',@level2name=N'cod_aai';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tipo de captação (ted, prev, ota....)' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_captacao_layer1', @level2type=N'COLUMN',@level2name=N'tipo_de_captacao';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'sinal da captação (1 para entrada, -1 para saida)' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_captacao_layer1', @level2type=N'COLUMN',@level2name=N'sinal_captacao';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor da captação ou resgate' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_captacao_layer1', @level2type=N'COLUMN',@level2name=N'valor_captacao';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data e hora de carga do registro' , @level0type=N'SCHEMA',@level0name=N'stage', @level1type=N'TABLE',@level1name=N'stage_captacao_layer1', @level2type=N'COLUMN',@level2name=N'data_carga';
GO



