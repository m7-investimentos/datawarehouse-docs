SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bronze].[xp_captacao](
	[data_ref] [date] NOT NULL,
	[cod_xp] [int] NOT NULL,
	[cod_aai] [varchar](50) NOT NULL,
	[tipo_de_captacao] [varchar](100) NOT NULL,
	[sinal_captacao] [int] NOT NULL,
	[valor_captacao] [decimal](18, 2) NOT NULL,
	[data_carga] [datetime] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [bronze].[xp_captacao] ADD  DEFAULT (getdate()) FOR [data_carga]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data real da captação/resgate. Use para análises temporais. JOIN com silver_dim_calendario por esta data. WHERE data_ref BETWEEN para períodos. GROUP BY YEAR(data_ref), MONTH(data_ref) para agregações mensais. Base para todas métricas temporais.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_captacao', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Identificador único e permanente do cliente XP. Numérico como string. Use para rastrear cliente ao longo do tempo. JOIN com outras tabelas bronze por este campo. COUNT DISTINCT para clientes únicos. Nunca muda mesmo com transferência de assessor.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_captacao', @level2type=N'COLUMN',@level2name=N'cod_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do assessor responsável. Formato variado: numérico (20471) ou alfanumérico (A22507). JOIN com silver_dim_pessoas.cod_aai para nome. Agrupe por este campo para análises por assessor. NULL indica cliente sem assessor atribuído.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_captacao', @level2type=N'COLUMN',@level2name=N'cod_aai'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Canal/método da movimentação. Valores: ted (transferência bancária tradicional), pix (transferência instantânea), prev (previdência privada), ota (oferta pública/IPO), doc. GROUP BY para análise de canais. WHERE tipo_de_captacao = ''ted'' para TEDs. Minúsculas sempre.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_captacao', @level2type=N'COLUMN',@level2name=N'tipo_de_captacao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Multiplicador matemático: 1=entrada de recursos, -1=saída/resgate. Use SUM(valor_captacao * sinal_captacao) para captação líquida. WHERE sinal_captacao = 1 para apenas entradas. Facilita cálculos sem CASE WHEN.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_captacao', @level2type=N'COLUMN',@level2name=N'sinal_captacao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Valor em R$ sempre positivo. Multiplique por sinal_captacao para obter valor real com sinal. SUM para totais, AVG para ticket médio. WHERE valor_captacao >= 100000 para grandes movimentações. Precisão 4 decimais mas geralmente inteiro.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_captacao', @level2type=N'COLUMN',@level2name=N'valor_captacao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Timestamp de inserção no DW. Use para auditoria e identificar reprocessamentos. WHERE data_carga = MAX(data_carga) para dados mais recentes. Diferente de data_ref que é quando ocorreu a operação. Formato: YYYY-MM-DD HH:MM:SS.' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_captacao', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Dados brutos de captação vindos da XP. Contém movimentações de entrada de recursos (TED, Previdência, etc) dos clientes por assessor AAI' , @level0type=N'SCHEMA',@level0name=N'bronze', @level1type=N'TABLE',@level1name=N'xp_captacao'
GO
