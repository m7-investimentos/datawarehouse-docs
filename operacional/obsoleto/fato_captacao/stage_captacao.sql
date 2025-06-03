CREATE TABLE [M7InvestimentosOLAP].[stage].[stage_captacao_layer1] (
    data_ref DATE NOT NULL,
    cod_xp INT NOT NULL,
    cod_aai VARCHAR(50) NOT NULL,
    tipo_de_captacao VARCHAR(100) NOT NULL,
    sinal_captacao INT NOT NULL,
    valor_captacao DECIMAL(18,2) NOT NULL,
    data_carga DATETIME DEFAULT GETDATE(),
    CONSTRAINT FK_stage_captacao_01_dim_calendario
        FOREIGN KEY (data_ref) REFERENCES [M7InvestimentosOLAP].[dim].[dim_calendario](data_ref),
    CONSTRAINT FK_stage_captacao_layer1_dim_clientes
        FOREIGN KEY (cod_xp) REFERENCES [M7InvestimentosOLAP].[dim].[dim_clientes](cod_xp)
);

CREATE INDEX idx_stage_captacao_layer1_data_ref ON [M7InvestimentosOLAP].[stage].[stage_captacao_layer1](data_ref);
CREATE INDEX idx_stage_captacao_layer1_cod_xp ON [M7InvestimentosOLAP].[stage].[stage_captacao_layer1](cod_xp);
CREATE INDEX idx_stage_captacao_layer1_cod_aai ON [M7InvestimentosOLAP].[stage].[stage_captacao_layer1](cod_aai);
GO

-- verificar nome da constraint
SELECT name
FROM sys.foreign_keys
WHERE parent_object_id = OBJECT_ID('[M7InvestimentosOLAP].[stage].[stage_captacao_layer1]')
AND referenced_object_id = OBJECT_ID('[M7InvestimentosOLAP].[dim].[dim_clientes]');
GO

-- remover chave estrangeira existente
ALTER TABLE [M7InvestimentosOLAP].[stage].[stage_captacao_layer1]
DROP CONSTRAINT FK_stage_captacao_01_dim_clientes;
GO

-- adicionar chave estrangeira
ALTER TABLE [M7InvestimentosOLAP].[stage].[stage_captacao_layer1]
ADD CONSTRAINT FK_stage_captacao_layer1_dim_clientes
    FOREIGN KEY (cod_xp) REFERENCES [M7InvestimentosOLAP].[dim].[dim_clientes](cod_xp);
GO

-- adicionar comentarios
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



