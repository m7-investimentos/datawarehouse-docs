CREATE TABLE [M7InvestimentosOLAP].[dim].[dim_clientes] (
       [cod_xp]                          int            NOT NULL,
       [crm_id_cliente]                  VARCHAR(20)    NOT NULL,
       [nome_cliente]                    VARCHAR(250)   NOT NULL DEFAULT '',
       [flag_grupo_familiar]             tinyint        NOT NULL,
       [grupo familiar]                  VARCHAR(250)   NULL,
       [status_cliente]                  tinyint        NOT NULL,
       [suitability]                     VARCHAR(20)    NULL,
       [tipo_investidor]                 VARCHAR(50)    NULL,
       [data_cadastro]                   DATE           NOT NULL,
       [data_cadastro_m7]                DATE           NOT NULL,
       [safra_ano_mes]                   VARCHAR(6)     NOT NULL,
       [data_nascimento]                 DATE           NULL,
       [telefone_cliente]                VARCHAR(20)    NULL,
       [cpf_cliente]                     VARCHAR(11)    NULL,
       [cnpj_cliente]                    VARCHAR(14)    NULL,
       [email_cliente]                   VARCHAR(250)   NULL,
       [profissao]                       VARCHAR(250)   NULL,
       [segmento]                        VARCHAR(50)    NULL,
       [segmento_pj]                     VARCHAR(50)    NULL,
       [genero]                          VARCHAR(1)     NOT NULL,
       [tipo_conta]                      VARCHAR(2)     NOT NULL,
       [fee_based]                       tinyint        NOT NULL,
       [patrimonio_declarado]            DECIMAL(18,2)  NULL,
       [saldo_em_conta]                  DECIMAL(18,2)  NULL,
       [patrimonio_xp_atual]             DECIMAL(18,2)  NULL,
       [patrimonio_xp_max]               DECIMAL(18,2)  NULL,
       [crm_id_assessor]                 VARCHAR(20)    NOT NULL,
       [cod_aai_assessor]                VARCHAR(20)    NOT NULL,
       [cod_aai_primeiro_assessor_m7]    VARCHAR(50)    NOT NULL,
       [qtd_assessores_m7]               int            NULL,
       [status_conta_digital]            VARCHAR(50)    NULL,
       [elegibilidade_cartao]            VARCHAR(50)    NULL,
       [produto_cartao]                  VARCHAR(50)    NULL,
       [data_ultimo_aporte]              DATE           NULL,
       [data_ultima_ordem]               DATE           NULL,
       [flag_open_investment]            tinyint        NULL,
       [saldo_open_investment]           DECIMAL(18,2)  NULL,

    CONSTRAINT PK_dim_clientes PRIMARY KEY ([cod_xp])
);
GO
-- descricao da tabela
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela dimensão que contém as informações cadastrais e patrimoniais dos clientes. maioria são valores categoricos para classificação do cliente e filtros no bi' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes';
GO

-- descricoes
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'cod_xp é o número da conta xp do cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'cod_xp';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'cód identificador do cliente em nosso crm bitrix' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'crm_id_cliente';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome do cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'nome_cliente';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'caso o cliente pertença a um grupo familiar, o valor será 1, caso contrário, será 0' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'flag_grupo_familiar';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'qual grupo familiar o cliente pertence' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'grupo familiar';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'se o cliente está ativo ou não.' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'status_cliente';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'perfil suitability do cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'suitability';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tipo de investidor do cliente, regular ou qualificado' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'tipo_investidor';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de habilitação do cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'data_cadastro';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'primeira data que o cliente apareceu em nossa base' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'data_cadastro_m7';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano e mês da safra' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'safra_ano_mes';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de nascimento do cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'data_nascimento';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'telefone do cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'telefone_cliente';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'cpf do cliente caso pj, caso pf será NULL' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'cpf_cliente';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'cnpj do cliente caso pj, caso pf será NULL' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'cnpj_cliente';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'email do cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'email_cliente';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'profissão do cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'profissao';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'segmento do cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'segmento';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'segmento do cliente caso pj, PME, Middle.' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'segmento_pj';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'genero do cliente, caso pj será NULL' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'genero';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tipo de conta do cliente, PF ou PJ' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'tipo_conta';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'se o cliente optou por fee fixo' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'fee_based';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'patrimonio declarado pelo cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'patrimonio_declarado';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'saldo em conta do cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'saldo_em_conta';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'patrimonio na conta xp atual do cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'patrimonio_xp_atual';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'máximo de patrimonio na conta xp que o cliente já teve' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'patrimonio_xp_max';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'cód identificador do crm do assessor do cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'crm_id_assessor';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'cód identificador do assessor no sistema da xp' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'cod_aai_assessor';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'cód identificador do primeiro assessor no sistema da xp' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'cod_aai_primeiro_assessor_m7';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'quantidade de assessores que o cliente já teve na M7' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'qtd_assessores_m7';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'status da conta digital do cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'status_conta_digital';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'elegibilidade do cliente para cartão' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'elegibilidade_cartao';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'produto de cartão que aquele cliente está elegível' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'produto_cartao';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data do último aporte realizado pelo cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'data_ultimo_aporte';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data da última ordem executada pelo cliente' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'data_ultima_ordem';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'flag indicando se o cliente tem Open Investment' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'flag_open_investment';
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'saldo do cliente no Open Investment em outras instituições' , @level0type=N'SCHEMA',@level0name=N'dim', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'saldo_open_investment';




