-- ==============================================================================
-- QRY-NPS-001-create_xp_nps_envios
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: dados@m7investimentos.com.br
-- Revisor: dados@m7investimentos.com.br
-- Tags: [ddl, bronze, xp, nps, pesquisa, satisfacao]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.xp_nps_envios para armazenar dados brutos de 
envios de pesquisas NPS (Net Promoter Score) extraídos do Hub XP. Contém 
informações sobre o envio, abertura e status de resposta das pesquisas.

Casos de uso:
- Tracking de pesquisas NPS enviadas
- Análise de taxa de abertura e resposta
- Monitoramento de satisfação por assessor
- Base para cálculo do NPS consolidado

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 300 envios/mês
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros necessários para execução:
N/A - Script DDL sem parâmetros

Exemplo de uso:
USE M7Medallion;
GO
-- Executar script completo
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela criada: bronze.xp_nps_envios

Colunas principais:
- survey_id: ID único da pesquisa
- cod_assessor: Código do assessor avaliado
- customer_id: ID do cliente respondente
- data_entrega/data_resposta: Datas de controle
- survey_status: Status atual da pesquisa
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
N/A

Pré-requisitos:
- Schema bronze deve existir
- Permissões CREATE TABLE no schema bronze
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
USE M7Medallion;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- ==============================================================================
-- 6. VERIFICAÇÃO E LIMPEZA
-- ==============================================================================

-- Verificar se a tabela já existe
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[xp_nps_envios]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.xp_nps_envios já existe. Dropando...';
    DROP TABLE [bronze].[xp_nps_envios];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[xp_nps_envios](
	[survey_id] [varchar](50) NULL,
	[cod_assessor] [varchar](20) NULL,
	[customer_id] [varchar](50) NULL,
	[codigo_escritorio] [varchar](20) NULL,
	[data_entrega] [date] NULL,
	[data_resposta] [date] NULL,
	[invitation_opened_date] [datetime] NULL,
	[survey_start_date] [datetime] NULL,
	[resposta_app_nps] [varchar](10) NULL,
	[survey_status] [varchar](50) NULL,
	[pesquisa_relacionamento] [varchar](100) NULL,
	[email] [varchar](255) NULL,
	[invitation_opened] [varchar](10) NULL,
	[sampling_exclusion_cause] [varchar](255) NULL,
	[last_page_seen] [varchar](100) NULL,
	[nps_app_survey_id_original] [varchar](50) NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY];
GO

ALTER TABLE [bronze].[xp_nps_envios] ADD DEFAULT (CONVERT([date],getdate())) FOR [data_carga];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por survey_id
CREATE NONCLUSTERED INDEX [IX_bronze_nps_envios_survey]
ON [bronze].[xp_nps_envios] ([survey_id])
WHERE [survey_id] IS NOT NULL;
GO

-- Índice para busca por assessor e data
CREATE NONCLUSTERED INDEX [IX_bronze_nps_envios_assessor_data]
ON [bronze].[xp_nps_envios] ([cod_assessor], [data_entrega])
INCLUDE ([survey_status], [data_resposta]);
GO

-- Índice para análise de status
CREATE NONCLUSTERED INDEX [IX_bronze_nps_envios_status]
ON [bronze].[xp_nps_envios] ([survey_status])
INCLUDE ([data_entrega], [data_resposta]);
GO

-- Índice para busca por cliente
CREATE NONCLUSTERED INDEX [IX_bronze_nps_envios_customer]
ON [bronze].[xp_nps_envios] ([customer_id])
WHERE [customer_id] IS NOT NULL;
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela com dados de envios de pesquisas NPS (Net Promoter Score) extraídos do Hub XP', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios';
GO

-- Documentação das colunas
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID único da pesquisa NPS. Chave para JOIN com tabela de respostas. Use para rastrear pesquisa individual.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'survey_id';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor sendo avaliado. JOIN com silver_dim_pessoas para nome. GROUP BY para análise por assessor.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'cod_assessor';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID do cliente que recebeu a pesquisa. Use para análise de frequência de pesquisas por cliente.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'customer_id';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do escritório/unidade. Use para análise regional ou por escritório.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'codigo_escritorio';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de envio da pesquisa. Use para análise temporal e cálculo de tempo de resposta.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'data_entrega';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data em que a pesquisa foi respondida. NULL indica não respondida. DATEDIFF com data_entrega para tempo de resposta.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'data_resposta';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Timestamp de abertura do convite da pesquisa. Use para taxa de abertura.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'invitation_opened_date';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Timestamp de início do preenchimento da pesquisa. Indica engajamento real.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'survey_start_date';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Resposta do NPS via aplicativo. Pode diferir da resposta web.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'resposta_app_nps';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Status atual da pesquisa. Valores: "Enviado", "Aberto", "Iniciado", "Completo", "Parcial". WHERE para filtrar por status.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'survey_status';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tipo de pesquisa de relacionamento. Identifica o contexto da avaliação.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'pesquisa_relacionamento';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Email do destinatário da pesquisa. Dado sensível - usar com cuidado.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'email';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Flag indicando se o convite foi aberto. "1"=Sim, "0"=Não.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'invitation_opened';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Motivo de exclusão da amostragem. NULL indica incluído na amostra.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'sampling_exclusion_cause';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Última página visualizada na pesquisa. Use para análise de abandono.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'last_page_seen';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID original da pesquisa no aplicativo NPS. Use para rastreamento entre sistemas.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'nps_app_survey_id_original';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de carga dos dados no DW. Use para auditoria e controle de processo.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_envios', 
    @level2type=N'COLUMN',@level2name=N'data_carga';
GO

-- ==============================================================================
-- 10. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                    | Descrição
--------|------------|--------------------------|--------------------------------------------
1.0.0   | 2025-01-17 | arquitetura.dados       | Criação inicial da tabela

*/

-- ==============================================================================
-- 11. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Esta tabela contém apenas envios, não as respostas detalhadas
- survey_id é a chave para JOIN com tabela de respostas
- Muitos campos podem ser NULL dependendo do status
- Email é dado sensível - aplicar políticas de segurança

Troubleshooting comum:
1. Survey sem resposta: Normal, verificar survey_status
2. Datas NULL: Depende do status da pesquisa
3. Duplicação: survey_id deve ser único por envio

Queries úteis:
-- Taxa de resposta por assessor
SELECT 
    cod_assessor,
    COUNT(*) as total_enviado,
    COUNT(data_resposta) as total_respondido,
    CAST(COUNT(data_resposta) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as taxa_resposta
FROM bronze.xp_nps_envios
WHERE data_entrega >= DATEADD(MONTH, -1, GETDATE())
GROUP BY cod_assessor
ORDER BY taxa_resposta DESC;

-- Tempo médio de resposta
SELECT 
    cod_assessor,
    AVG(DATEDIFF(DAY, data_entrega, data_resposta)) as dias_medio_resposta,
    MIN(DATEDIFF(DAY, data_entrega, data_resposta)) as min_dias,
    MAX(DATEDIFF(DAY, data_entrega, data_resposta)) as max_dias
FROM bronze.xp_nps_envios
WHERE data_resposta IS NOT NULL
    AND data_entrega >= DATEADD(MONTH, -3, GETDATE())
GROUP BY cod_assessor;

-- Funil de engajamento
SELECT 
    survey_status,
    COUNT(*) as quantidade,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) as percentual
FROM bronze.xp_nps_envios
WHERE data_entrega >= DATEADD(MONTH, -1, GETDATE())
GROUP BY survey_status
ORDER BY 
    CASE survey_status
        WHEN 'Enviado' THEN 1
        WHEN 'Aberto' THEN 2
        WHEN 'Iniciado' THEN 3
        WHEN 'Parcial' THEN 4
        WHEN 'Completo' THEN 5
        ELSE 6
    END;

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
PRINT 'Tabela bronze.xp_nps_envios criada com sucesso!';
GO
