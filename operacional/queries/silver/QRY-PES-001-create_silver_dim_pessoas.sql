CREATE TABLE [M7InvestimentosOLAP].[dim].[dim_pessoas] (
    crm_id              VARCHAR(20)    NOT NULL,
    nome_pessoa         VARCHAR(200)   NOT NULL,
    cod_aai             VARCHAR(50)    NULL,
    id_avenue           VARCHAR(50)    NULL,
    id_rd_station       VARCHAR(50)    NULL,
    data_nascimento     DATE           NULL,
    data_inicio_vigencia DATE          NOT NULL,
    data_fim_vigencia   DATE           NULL,
    email_multisete     VARCHAR(200)   NULL,
    email_xp            VARCHAR(200)   NULL,
    observacoes         VARCHAR(200)   NULL,
    CONSTRAINT PK_dim_pessoas PRIMARY KEY (crm_id)
);
GO

-- descricao da tabela
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela dimensão que contém as informações cadastrais dos colaboradores, assessores e funcionarios no geral' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_pessoas';
GO

-- descricoes
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador único da pessoa no crm' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'crm_id';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome completo da pessoa' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'nome_pessoa';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código da pessoa no sistema da xp' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'cod_aai';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador da pessoa no sistema avenue' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'id_avenue';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador da pessoa no sistema rd station' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'id_rd_station';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de nascimento da pessoa' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'data_nascimento';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de início da vigência da pessoa, quando entrou na empresa' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'data_inicio_vigencia';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de fim da vigência do registro, quando saiu da empresa' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'data_fim_vigencia';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'email da pessoa multisete' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'email_multisete';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'email da pessoa no domínio xp' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'email_xp';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'observações adicionais sobre a pessoa' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'observacoes';
GO
