SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bronze].[xp_open_investment_habilitacao](
	[ano_mes] [varchar](6) NULL,
	[data_permissao] [date] NULL,
	[cod_xp] [int] NOT NULL,
	[tipo_conta] [varchar](20) NOT NULL,
	[cod_aai] [varchar](20) NOT NULL,
	[status_termo] [varchar](50) NOT NULL,
	[instituicao] [varchar](255) NULL,
	[sow] [decimal](12, 6) NOT NULL,
	[auc] [decimal](16, 2) NOT NULL,
	[auc_atual] [decimal](16, 2) NOT NULL,
	[grupo_clientes] [varchar](50) NULL,
	[sugestao_estrategia] [varchar](50) NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [bronze].[xp_open_investment_habilitacao] ADD  DEFAULT (CONVERT([date],getdate())) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Período no formato AAAAMM (ex: 202501). Use para agregações mensais e séries temporais. WHERE ano_mes = ''202501'' para mês específico. CAST para date: CAST(ano_mes + ''01'' AS DATE). ORDER BY para cronologia.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', @level2type=N'COLUMN',@level2name=N'ano_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data exata que cliente autorizou Open Banking. Marco para início de visibilidade. DATEDIFF para dias desde autorização. WHERE data_permissao IS NOT NULL para apenas habilitados. Base para análise de adoção.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', @level2type=N'COLUMN',@level2name=N'data_permissao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ID único do cliente XP. Chave para JOIN com todas bronze_xp_*. Use para enriquecer análises com dados de outras tabelas. COUNT DISTINCT para total de clientes com Open Banking habilitado.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', @level2type=N'COLUMN',@level2name=N'cod_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Natureza jurídica: PESSOA FÍSICA ou PESSOA JURÍDICA. WHERE tipo_conta = ''PESSOA FÍSICA'' para análises PF. GROUP BY para métricas por tipo. PJ geralmente tem maior potencial mas menor volume.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', @level2type=N'COLUMN',@level2name=N'tipo_conta'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do assessor responsável. JOIN com silver_dim_pessoas para dados do assessor. GROUP BY para análise de adoção por carteira. NULL para clientes B2C/Digital. Base para gamificação.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', @level2type=N'COLUMN',@level2name=N'cod_aai'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Status da autorização: Enviado (pendente), Recebido (autorizado). WHERE status_termo = ''Recebido'' para clientes ativos. COUNT por status para funil de conversão. Target para follow-up.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', @level2type=N'COLUMN',@level2name=N'status_termo'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Principal banco/instituição externa do cliente. Indica onde concentra patrimônio fora. GROUP BY para ranking de concorrentes. Base para campanhas direcionadas por banco de origem.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', @level2type=N'COLUMN',@level2name=N'instituicao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Share of Wallet - percentual na XP. 1.00 = 100% do patrimônio na XP (ideal), 0.50 = 50% na XP. WHERE sow < 0.3 para grandes oportunidades. AVG para SOW médio. Métrica chave para priorização.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', @level2type=N'COLUMN',@level2name=N'sow'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Assets Under Custody - valor total fora da XP em R$. SUM para potencial total de mercado. WHERE auc > 1000000 para high value. Multiplicar por (1-sow) para potencial líquido de captação.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', @level2type=N'COLUMN',@level2name=N'auc'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'AUC atualizado/mais recente em R$. Compare com auc para ver evolução. Pode diferir por movimentações ou atualização de cotações. Use este para análises atuais.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', @level2type=N'COLUMN',@level2name=N'auc_atual'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Classificação estratégica XP: defesa (risco de perder), defesa prioritario (alto valor em risco), expansão (oportunidade crescer). WHERE grupo_clientes = ''expansão'' para foco em crescimento.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', @level2type=N'COLUMN',@level2name=N'grupo_clientes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Recomendação de abordagem da XP. Ex: Ação Ativo (abordagem agressiva), Manutenção (manter relacionamento). Base para playbook comercial. GROUP BY para volume por estratégia.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', @level2type=N'COLUMN',@level2name=N'sugestao_estrategia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Timestamp de carga no DW. WHERE data_carga = (SELECT MAX(data_carga)...) para dados mais recentes. Controle de atualização. Geralmente mensal.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Dados brutos de habilitação de clientes na Open Investment. Registra quando clientes foram habilitados para operar através dessa corretora parceira' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao'
GO
