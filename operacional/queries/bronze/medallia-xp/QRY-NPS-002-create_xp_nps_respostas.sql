-- ==============================================================================
-- QRY-NPS-001-create_xp_nps_respostas
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: dados@m7investimentos.com.br
-- Revisor: dados@m7investimentos.com.br
-- Tags: [ddl, bronze, xp, nps, respostas, satisfacao, feedback]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.xp_nps_respostas para armazenar dados brutos de 
respostas de pesquisas NPS (Net Promoter Score) extraídos do Hub XP. Contém as 
notas, comentários e múltiplas variações de NPS para diferentes canais (XP, 
Clear, Rico) e momentos do cliente (onboarding, aniversário).

Casos de uso:
- Armazenamento de respostas detalhadas de NPS
- Análise de satisfação por assessor e canal
- Text mining de comentários e feedback
- Cálculo de NPS consolidado e por segmento

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 300 respostas/mês
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
Tabela criada: bronze.xp_nps_respostas

Colunas principais:
- survey_id: ID único da pesquisa (JOIN com envios)
- cod_assessor: Código do assessor avaliado
- data_resposta: Data da resposta
- Múltiplas colunas de NPS por canal/momento
- Comentários em texto livre
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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[xp_nps_respostas]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.xp_nps_respostas já existe. Dropando...';
    DROP TABLE [bronze].[xp_nps_respostas];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[xp_nps_respostas](
	[survey_id] [varchar](50) NULL,
	[id_usuario] [varchar](50) NULL,
	[customer_id] [varchar](50) NULL,
	[cod_assessor] [varchar](20) NULL,
	[codigo_escritorio] [varchar](20) NULL,
	[data_resposta] [date] NULL,
	[delivered_on_date] [date] NULL,
	[data_cadastro] [date] NULL,
	[faixa_net] [varchar](50) NULL,
	[faixa_pld] [varchar](50) NULL,
	[area] [varchar](100) NULL,
	[canal] [varchar](50) NULL,
	[suitability] [varchar](50) NULL,
	[area_transacional] [varchar](100) NULL,
	[xp_assessores_survey_type] [varchar](100) NULL,
	[pesquisa_relacionamento] [varchar](100) NULL,
	[xp_aniversario_nps_assessor] [float] NULL,
	[xp_onboarding_nps] [float] NULL,
	[clear_onboarding_nps] [float] NULL,
	[rico_onboarding_nps] [float] NULL,
	[xp_aniversario_nps_xp] [float] NULL,
	[clear_aniversario_nps] [float] NULL,
	[rico_aniversario_nps] [float] NULL,
	[transacional_satisfacao_assessor] [float] NULL,
	[transacional_comentario_satisfacao] [nvarchar](max) NULL,
	[xp_onboarding_comentario] [nvarchar](max) NULL,
	[clear_onboarding_comentario] [nvarchar](max) NULL,
	[rico_onboarding_comentario] [nvarchar](max) NULL,
	[xp_aniversario_comentario_assessor] [nvarchar](max) NULL,
	[xp_aniversario_comentario_xp] [nvarchar](max) NULL,
	[clear_aniversario_comentario] [nvarchar](max) NULL,
	[rico_aniversario_comentario] [nvarchar](max) NULL,
	[open_comment] [nvarchar](max) NULL,
	[clear_razao_nps] [varchar](255) NULL,
	[rico_razao_nps] [varchar](255) NULL,
	[xp_razao_nps] [varchar](255) NULL,
	[clear_aniversario_razao_nps] [varchar](255) NULL,
	[clear_onboarding_razao_nps] [varchar](255) NULL,
	[rico_aniversario_razao_nps] [varchar](255) NULL,
	[rico_onboarding_razao_nps] [varchar](255) NULL,
	[xp_aniversario_razao_nps_assessor] [varchar](255) NULL,
	[xp_aniversario_razao_nps_xp] [varchar](255) NULL,
	[xp_onboarding_razao_nps] [varchar](255) NULL,
	[xp_aniversario_email_amigo_familiar] [varchar](255) NULL,
	[xp_aniversario_recomendaria_assessor] [varchar](50) NULL,
	[link_to_response] [varchar](500) NULL,
	[link_to_response_2] [varchar](500) NULL,
	[pld] [varchar](50) NULL,
	[genero] [varchar](20) NULL,
	[usuarios_ativos] [varchar](50) NULL,
	[cliente_max] [varchar](50) NULL,
	[cliente_max_simulacao] [varchar](50) NULL,
	[persona] [varchar](100) NULL,
	[faixa_atendimento] [varchar](100) NULL,
	[grupo_atendimento] [varchar](100) NULL,
	[topics_tagged_original] [varchar](500) NULL,
	[unit_secundaria_digital] [varchar](100) NULL,
	[first_name] [varchar](100) NULL,
	[email] [varchar](255) NULL,
	[previdencia_atual] [varchar](100) NULL,
	[receber_comparativo_proposta] [varchar](50) NULL,
	[valor_previdencia] [decimal](18, 2) NULL,
	[voce_gostaria_responder_duas_questoes] [varchar](10) NULL,
	[global_relacionamento_subatributo] [varchar](255) NULL,
	[rico_relacionamento_subatributo] [varchar](255) NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
GO

ALTER TABLE [bronze].[xp_nps_respostas] ADD DEFAULT (CONVERT([date],getdate())) FOR [data_carga];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por survey_id
CREATE NONCLUSTERED INDEX [IX_bronze_nps_respostas_survey]
ON [bronze].[xp_nps_respostas] ([survey_id])
WHERE [survey_id] IS NOT NULL;
GO

-- Índice para busca por assessor e data
CREATE NONCLUSTERED INDEX [IX_bronze_nps_respostas_assessor_data]
ON [bronze].[xp_nps_respostas] ([cod_assessor], [data_resposta])
INCLUDE ([xp_aniversario_nps_assessor], [xp_onboarding_nps]);
GO

-- Índice para análise por canal
CREATE NONCLUSTERED INDEX [IX_bronze_nps_respostas_canal]
ON [bronze].[xp_nps_respostas] ([canal])
INCLUDE ([data_resposta]);
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela com dados de respostas de pesquisas NPS (Net Promoter Score) extraídos do Hub XP. Contém múltiplas variações de notas NPS para diferentes canais (XP, Clear, Rico) e momentos (onboarding, aniversário)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_respostas';
GO

-- Documentação das colunas principais (seleção das mais importantes)
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID único da pesquisa. JOIN com xp_nps_envios para dados de envio. Chave primária para resposta.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_respostas', 
    @level2type=N'COLUMN',@level2name=N'survey_id';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor avaliado. JOIN com dim_pessoas para nome. Base para cálculo de NPS por assessor.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_respostas', 
    @level2type=N'COLUMN',@level2name=N'cod_assessor';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data em que a pesquisa foi respondida. Base para análises temporais e tendências.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_respostas', 
    @level2type=N'COLUMN',@level2name=N'data_resposta';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'NPS do assessor na pesquisa de aniversário XP. Escala 0-10. 9-10=Promotor, 7-8=Neutro, 0-6=Detrator.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_respostas', 
    @level2type=N'COLUMN',@level2name=N'xp_aniversario_nps_assessor';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'NPS do onboarding XP. Mede satisfação inicial do cliente. Crítico para retenção.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_respostas', 
    @level2type=N'COLUMN',@level2name=N'xp_onboarding_nps';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Comentário livre sobre satisfação transacional. Texto não estruturado para análise qualitativa.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_respostas', 
    @level2type=N'COLUMN',@level2name=N'transacional_comentario_satisfacao';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Faixa de patrimônio líquido do cliente. Use para segmentação de análises NPS.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_respostas', 
    @level2type=N'COLUMN',@level2name=N'faixa_net';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Canal de atendimento: XP, Clear, Rico. Determina qual marca está sendo avaliada.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_respostas', 
    @level2type=N'COLUMN',@level2name=N'canal';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Perfil de investidor do respondente. Valores: Conservador, Moderado, Arrojado.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_respostas', 
    @level2type=N'COLUMN',@level2name=N'suitability';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de carga dos dados no DW. Use para controle de processamento.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_nps_respostas', 
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
- Múltiplas colunas NPS por combinação canal/momento
- Comentários em NVARCHAR(MAX) para textos longos
- Muitas colunas podem ser NULL dependendo do tipo de pesquisa
- Links para resposta original mantidos para auditoria

Troubleshooting comum:
1. NPS NULL: Normal, nem todas pesquisas têm todas variações
2. Comentários truncados: Verificar encoding na origem
3. Performance em comentários: Considerar full-text index

Queries úteis:
-- Cálculo de NPS por assessor
WITH nps_calc AS (
    SELECT 
        cod_assessor,
        CASE 
            WHEN xp_aniversario_nps_assessor >= 9 THEN 'Promotor'
            WHEN xp_aniversario_nps_assessor >= 7 THEN 'Neutro'
            ELSE 'Detrator'
        END as categoria,
        COUNT(*) as qtd
    FROM bronze.xp_nps_respostas
    WHERE xp_aniversario_nps_assessor IS NOT NULL
        AND data_resposta >= DATEADD(MONTH, -3, GETDATE())
    GROUP BY cod_assessor, 
        CASE 
            WHEN xp_aniversario_nps_assessor >= 9 THEN 'Promotor'
            WHEN xp_aniversario_nps_assessor >= 7 THEN 'Neutro'
            ELSE 'Detrator'
        END
)
SELECT 
    cod_assessor,
    SUM(CASE WHEN categoria = 'Promotor' THEN qtd ELSE 0 END) * 100.0 / SUM(qtd) -
    SUM(CASE WHEN categoria = 'Detrator' THEN qtd ELSE 0 END) * 100.0 / SUM(qtd) as NPS
FROM nps_calc
GROUP BY cod_assessor
ORDER BY NPS DESC;

-- Análise de comentários negativos
SELECT 
    cod_assessor,
    data_resposta,
    xp_aniversario_nps_assessor,
    xp_aniversario_comentario_assessor
FROM bronze.xp_nps_respostas
WHERE xp_aniversario_nps_assessor <= 6
    AND xp_aniversario_comentario_assessor IS NOT NULL
    AND LEN(xp_aniversario_comentario_assessor) > 10
ORDER BY data_resposta DESC;

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
PRINT 'Tabela bronze.xp_nps_respostas criada com sucesso!';
GO
