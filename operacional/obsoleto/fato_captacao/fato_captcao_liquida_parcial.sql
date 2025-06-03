CREATE TABLE [M7InvestimentosOLAP].[fato].[fato_captacao_liquida_parcial] (
    data_ref DATE NOT NULL,
    cod_xp INT NOT NULL,
    crm_id VARCHAR(20) NOT NULL,
    
    captacao_liquida_asset DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_cbx DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_cex DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_coe DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_fundos_exclusivos DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_fundos_pco DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_ota DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_prev DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_prev_aporte DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_prev_port_entrada DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_prev_port_saida DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_prev_resgate DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_rf DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_stvm DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_td DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_ted DECIMAL(18,2) NOT NULL DEFAULT 0,

    resgate_asset DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_cbx DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_cex DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_coe DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_fundos_exclusivos DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_fundos_pco DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_ota DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_prev DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_prev_aporte DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_prev_port_entrada DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_prev_port_saida DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_prev_resgate DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_rf DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_stvm DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_td DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate_ted DECIMAL(18,2) NOT NULL DEFAULT 0,
    
    captacao_liquida_parcial DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_bruta DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate DECIMAL(18,2) NOT NULL DEFAULT 0,
   
    CONSTRAINT UK_fato_captacao_liquida_parcial UNIQUE (data_ref, cod_xp),
    
    CONSTRAINT FK_fato_captacao_liquida_parcial_dim_calendario
        FOREIGN KEY (data_ref) REFERENCES [M7InvestimentosOLAP].[dim].[dim_calendario] (data_ref),
    
    CONSTRAINT FK_fato_captacao_liquida_parcial_dim_clientes
        FOREIGN KEY (cod_xp) REFERENCES [M7InvestimentosOLAP].[dim].[dim_clientes] (cod_xp),

    CONSTRAINT FK_fato_captacao_liquida_parcial_dim_pessoas
        FOREIGN KEY (crm_id) REFERENCES [M7InvestimentosOLAP].[dim].[dim_pessoas] (crm_id)
);
GO

-- descricao da tabela
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela fatos que contém os valores de captação líquida parcial por produto e cliente, nao contabiliza as transferencias.' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial';
GO

-- descricoes
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do registro' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'data_ref';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código do cliente no sistema xp' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'cod_xp';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador do assessor no crm' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'crm_id';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em asset' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_asset';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em cbx' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_cbx';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em cex' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_cex';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em coe' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_coe';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em fundos exclusivos' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_fundos_exclusivos';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em fundos pco' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_fundos_pco';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em ota' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_ota';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em previdência' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_prev';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em aporte previdência' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_prev_aporte';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em entrada de portfólio previdência' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_prev_port_entrada';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em saída de portfólio previdência' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_prev_port_saida';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em resgate previdência' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_prev_resgate';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em renda fixa' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_rf';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em stvm' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_stvm';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em td' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_td';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida em ted' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_ted';
GO

-- resgates
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em asset' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_asset';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em cbx' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_cbx';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em cex' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_cex';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em coe' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_coe';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em fundos exclusivos' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_fundos_exclusivos';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em fundos pco' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_fundos_pco';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em ota' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_ota';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em previdência' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_prev';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em aporte previdência' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_prev_aporte';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em entrada de portfólio previdência' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_prev_port_entrada';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em saída de portfólio previdência' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_prev_port_saida';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em resgate previdência' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_prev_resgate';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em renda fixa' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_rf';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em stvm' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_stvm';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em td' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_td';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate em ted' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate_ted';
GO

-- totais
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida parcial total' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_liquida_parcial';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação bruta total' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'captacao_bruta';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate total' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_liquida_parcial', @level2type=N'COLUMN',@level2name=N'resgate';
GO

-- indices
CREATE INDEX IDX_fato_captacao_liquida_parcial_data_ref 
ON [M7InvestimentosOLAP].[fato].[fato_captacao_liquida_parcial] (data_ref);
GO

CREATE INDEX IDX_fato_captacao_liquida_parcial_cod_xp 
ON [M7InvestimentosOLAP].[fato].[fato_captacao_liquida_parcial] (cod_xp);
GO

CREATE INDEX IDX_fato_captacao_liquida_parcial_crm_id 
ON [M7InvestimentosOLAP].[fato].[fato_captacao_liquida_parcial] (crm_id);
GO
