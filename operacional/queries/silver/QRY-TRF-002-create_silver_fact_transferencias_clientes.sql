CREATE TABLE [M7InvestimentosOLAP].[fato].[fato_transf_clientes] (
    cod_xp INT NOT NULL,
    data_cadastro DATE NOT NULL,
    crm_id_origem VARCHAR(20) NULL,
    crm_id_destino VARCHAR(20) NULL,
    data_transferencia DATE NULL,
    tipo_transf VARCHAR(50) NULL,
    PL_transferencia DECIMAL(16,4) NOT NULL,
    

);

GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela de fatos que contém os dados de transferências de entrada, saida e internas de clientes' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_transf_clientes';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'numero da conta do cliente no sistema XP' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_transf_clientes', @level2type=N'COLUMN',@level2name=N'cod_xp';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data em que o registro foi cadastrado' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_transf_clientes', @level2type=N'COLUMN',@level2name=N'data_cadastro';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código do assessor de origem no sistema XP' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_transf_clientes', @level2type=N'COLUMN',@level2name=N'crm_id_origem';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código do assessor de destino no sistema XP' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_transf_clientes', @level2type=N'COLUMN',@level2name=N'crm_id_destino';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data em que a transferência foi efetivada' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_transf_clientes', @level2type=N'COLUMN',@level2name=N'data_transferencia';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tipo da transferência, entrada, saida ou interna' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_transf_clientes', @level2type=N'COLUMN',@level2name=N'tipo_transf';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'patrimônio líquido transferido' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_transf_clientes', @level2type=N'COLUMN',@level2name=N'PL_transferencia';
GO


