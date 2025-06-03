CREATE TABLE [M7Medallion].[bronze].[bronze_xp_open_investment_habilitacao] (
    ano_mes                                 VARCHAR(6)    NOT NULL,
    data_permissao                          DATE          NULL,
    cod_xp                                  int   NOT NULL,
    tipo_conta                              VARCHAR(20)   NOT NULL,
    cod_aai                                 VARCHAR(20)   NOT NULL,
    status_termo                            VARCHAR(50)   NOT NULL,
    instituicao                             VARCHAR(255)  NULL,
    sow                                     DECIMAL(12,6)  NOT NULL DEFAULT 0,
    auc                                     DECIMAL(16,2) NOT NULL DEFAULT 0,
    auc_atual                               DECIMAL(16,2) NOT NULL DEFAULT 0,
    rentabilidade_anual                     DECIMAL(12,6)  NOT NULL DEFAULT 0,
    grupo_clientes                          VARCHAR(50)   NULL,
    sugestao_estrategia                     VARCHAR(50)   NULL,
    data_carga                              DATE          NOT NULL DEFAULT (CAST(GETDATE() AS DATE)),
);
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela de bronze que contém os dados de OI (open investments). obtida a partir do relatório de ferramentas de produtividade do hub xp' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi';
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano e mês de referência no formato AAAAMM' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'ano_mes';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data em que o cliente autorizou o acesso aos dados em outras instituições' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'data_permissao';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'numero da conta do cliente no sistema XP' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'cod_xp';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tipo de conta do cliente, pf pj' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'tipo_conta';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código do assessor no sistema XP' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'cod_aai';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'status do termo de adesão do cliente, se ele permitiu o acesso ou não' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'status_termo';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome da instituição financeira do qual o cliente permitiu o acesso' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'instituicao';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor do SOW (Share of Wallet) do cliente' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'sow';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor do AUC (Assets Under Custody) do cliente' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'auc';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor atual do AUC (Assets Under Custody) do cliente' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'auc_atual';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'rentabilidade anual do cliente' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'rentabilidade_anual';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'grupo do cliente, uma classificacao de acordo com a xp, nao utilizamos no momento' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'grupo_clientes';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'sugestão de estratégia para o cliente, nao utilizamos no momento' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'sugestao_estrategia';
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data em que o registro foi carregado' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'bronze_oi', @level2type=N'COLUMN',@level2name=N'data_carga';
GO




