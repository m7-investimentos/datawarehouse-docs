CREATE TABLE [M7InvestimentosOLAP].[fato].[fato_captacao_consolidado] (
    data_ref DATE NOT NULL,
    crm_id VARCHAR(20) NOT NULL,
    captacao_liquida_m7 DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_xp DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_liquida_parcial DECIMAL(18,2) NOT NULL DEFAULT 0,
    captacao_bruta DECIMAL(18,2) NOT NULL DEFAULT 0,
    resgate DECIMAL(18,2) NOT NULL DEFAULT 0,
    valor_nova_conta DECIMAL(18,2) NOT NULL DEFAULT 0,
    valor_transf_escritorio DECIMAL(18,2) NOT NULL DEFAULT 0,
    valor_transf_saida DECIMAL(18,2) NOT NULL DEFAULT 0,
    valor_trasnf_interna_m7 DECIMAL(18,2) NOT NULL DEFAULT 0,
    indice_eficiencia_captacao DECIMAL(18,6) NULL,
    relacao_captacao_resgate DECIMAL(18,6) NULL,
    taxa_retencao_captacao DECIMAL(18,6) NULL,
    
   
    CONSTRAINT UK_fato_captacao_consolidado UNIQUE (data_ref, crm_id),
    
    CONSTRAINT FK_fato_captacao_consolidado_dim_calendario
        FOREIGN KEY (data_ref) REFERENCES [M7InvestimentosOLAP].[dim].[dim_calendario] (data_ref),
    
    CONSTRAINT FK_fato_captacao_consolidado_dim_pessoas
        FOREIGN KEY (crm_id) REFERENCES [M7InvestimentosOLAP].[dim].[dim_pessoas] (crm_id)
);
GO

-- descricao da tabela
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela fatos que contém os valores consolidados de captação por assessor. não está com granularidade de cliente! pois está com o acumulado das trasnfências entre escritorios, saida e entrada.' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado';
GO

-- descricoes
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do registro' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'data_ref';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador do assessor no crm' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'crm_id';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida total na m7, considerando as transferencias entre escritorios, que a xp nao contabiliza' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'captacao_liquida_m7';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida total na xp, nao considera as transferencias entre escritorios.' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'captacao_liquida_xp';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida parcial' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'captacao_liquida_parcial';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação bruta total' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'captacao_bruta';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor total de resgates' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'resgate';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor de transferencia de entrada de novas contas' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'valor_nova_conta';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor de transferências de entrada entre escritórios' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'valor_transf_escritorio';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor de transferências de saída' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'valor_transf_saida';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor de transferências internas na m7' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'valor_trasnf_interna_m7';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'índice de eficiência da captação, calculado pela relação entre captação líquida parcial e a soma de captação bruta, resgates e transferências' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'indice_eficiencia_captacao';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'relação entre captação e resgate' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'relacao_captacao_resgate';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'taxa de retenção da captação' , @level0type=N'SCHEMA',@level0name=N'fato', @level1type=N'TABLE',@level1name=N'fato_captacao_consolidado', @level2type=N'COLUMN',@level2name=N'taxa_retencao_captacao';
GO

-- indices
CREATE INDEX IDX_fato_captacao_consolidado_data_ref 
ON [M7InvestimentosOLAP].[fato].[fato_captacao_consolidado] (data_ref);
GO

CREATE INDEX IDX_fato_captacao_consolidado_crm_id 
ON [M7InvestimentosOLAP].[fato].[fato_captacao_consolidado] (crm_id); 