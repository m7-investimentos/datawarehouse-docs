-- estrutura ok,
CREATE TABLE [M7InvestimentosOLAP].[dim].[dim_estruturas] (
    id_estrutura               INT            NOT NULL,
    nome_estrutura             VARCHAR(100)   NOT NULL,
    estrutura_pai              INT            NULL,
    crm_id_lider               VARCHAR(20)    NULL,
    observacoes                VARCHAR(200)   NULL,
    CONSTRAINT PK_pap_dim_estruturas PRIMARY KEY (id_estrutura),

    CONSTRAINT FK_dim_estruturas_estrutura_pai  FOREIGN KEY (estrutura_pai) 
        REFERENCES [M7InvestimentosOLAP].[dim].[dim_estruturas] (id_estrutura),
);
GO

-- descricao da tabela
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela dimensão que contém as informações da estrutura organizacional da empresa' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_estruturas';
GO

-- descricoes
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador único da estrutura organizacional' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_estruturas', @level2type=N'COLUMN',@level2name=N'id_estrutura';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome da estrutura organizacional' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_estruturas', @level2type=N'COLUMN',@level2name=N'nome_estrutura';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador da estrutura pai (auto-relacionamento)' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_estruturas', @level2type=N'COLUMN',@level2name=N'estrutura_pai';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador do líder da estrutura no crm' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_estruturas', @level2type=N'COLUMN',@level2name=N'crm_id_lider';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'observações adicionais sobre a estrutura organizacional' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_estruturas', @level2type=N'COLUMN',@level2name=N'observacoes';
GO
