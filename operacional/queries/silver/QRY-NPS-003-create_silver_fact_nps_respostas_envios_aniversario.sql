-- ==============================================================================
-- QRY-NPS-003-CREATE_SILVER_FACT_NPS_RESPOSTAS_ENVIOS_ANIVERSARIO
-- ==============================================================================
-- Tipo: DDL - Create Table
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [ddl, silver, fact_table, nps, pesquisa, satisfacao, aniversario]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela fato fact_nps_respostas_envios_aniversario no schema silver.
           Armazena dados consolidados de pesquisas NPS (Net Promoter Score) enviadas
           em aniversários de relacionamento, incluindo respostas, classificações e
           comentários dos clientes sobre assessores e a XP.

Casos de uso:
- Análise de satisfação dos clientes com assessores
- Acompanhamento de NPS por assessor e período
- Identificação de promotores, neutros e detratores
- Análise de comentários e razões das notas
- Base para ações de melhoria e reconhecimento

Frequência de execução: Única (criação da tabela)
Volume esperado de linhas: ~50.000 registros/ano
*/

-- ==============================================================================
-- 2. ESTRUTURA DA TABELA
-- ==============================================================================
/*
Colunas da tabela:

| Coluna                      | Tipo           | Nullable | Descrição                                              |
|-----------------------------|----------------|----------|--------------------------------------------------------|
| survey_id                   | VARCHAR(20)    | NOT NULL | Identificador único da pesquisa (PK)                  |
| conta_xp_cliente            | VARCHAR(20)    | NOT NULL | Código da conta XP do cliente                         |
| cod_assessor                | VARCHAR(20)    | NOT NULL | Código do assessor responsável                        |
| data_entrega                | DATE           | NULL     | Data de envio da pesquisa                             |
| data_resposta               | DATE           | NULL     | Data da resposta do cliente                           |
| data_inicio_survey          | DATETIME       | NULL     | Data/hora de início do preenchimento                  |
| survey_status               | VARCHAR(50)    | NULL     | Status da pesquisa (completed, delivered, etc)        |
| convite_aberto              | CHAR(3)        | NULL     | Se o convite foi aberto (sim/nao)                    |
| nps_assessor                | DECIMAL(3,1)   | NULL     | Nota NPS do assessor (0-10)                           |
| nps_xp                      | DECIMAL(3,1)   | NULL     | Nota NPS da XP (0-10)                                 |
| recomendaria_assessor       | CHAR(3)        | NULL     | Se recomendaria o assessor (sim/nao)                  |
| classificacao_nps_assessor  | VARCHAR(10)    | NULL     | Classificação: Promotor/Neutro/Detrator              |
| classificacao_nps_xp        | VARCHAR(10)    | NULL     | Classificação: Promotor/Neutro/Detrator              |
| comentario_assessor         | NVARCHAR(MAX)  | NULL     | Comentário sobre o assessor                           |
| comentario_xp               | NVARCHAR(MAX)  | NULL     | Comentário sobre a XP                                 |
| razao_nps                   | NVARCHAR(100)  | NULL     | Razão principal da nota                               |
| razao_nps_assessor          | NVARCHAR(100)  | NULL     | Razão específica assessor                             |
| razao_nps_xp                | NVARCHAR(100)  | NULL     | Razão específica XP                                   |
| topicos_relevantes          | NVARCHAR(500)  | NULL     | Tópicos identificados nos comentários                 |
| data_carga                  | DATETIME       | NOT NULL | Data/hora da carga (default: GETDATE())               |

Chave primária: survey_id
*/

-- ==============================================================================
-- 3. DEPENDÊNCIAS
-- ==============================================================================
/*
Schemas necessários:
- silver: Schema onde a tabela será criada
- bronze: Schema de origem dos dados

Tabelas de origem (bronze):
- [bronze].[xp_nps_respostas]: Respostas das pesquisas NPS
  * Contém notas, comentários e classificações
  * Campos principais: survey_id, customer_id, nps_assessor, nps_xp
  
- [bronze].[xp_nps_envios]: Dados de envio das pesquisas
  * Contém datas de envio e status
  * Campos principais: survey_id, data_entrega, survey_status

Views relacionadas:
- [silver].[vw_nps_respostas_envios_aniversario]: View que consolida envios e respostas
  * Une dados das duas tabelas bronze
  * Calcula classificações (Promotor/Neutro/Detrator)
  * Fonte para a procedure de carga

Processo ETL:
- Procedure: [silver].[prc_bronze_to_silver_fact_nps_respostas_envios_aniversario]
- Execução: Mensal ou conforme chegada de novos dados
- Modo: Full load (TRUNCATE/INSERT)

Pré-requisitos:
- Schema silver deve existir
- Tabelas bronze devem estar atualizadas
- Usuário deve ter permissão CREATE TABLE no schema silver
*/

-- ==============================================================================
-- 4. SCRIPT DE CRIAÇÃO
-- ==============================================================================

-- Configurações iniciais
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Criação da tabela
CREATE TABLE [silver].[fact_nps_respostas_envios_aniversario](
	[survey_id] [varchar](20) NOT NULL,
	[conta_xp_cliente] [varchar](20) NOT NULL,
	[cod_assessor] [varchar](20) NOT NULL,
	[data_entrega] [date] NULL,
	[data_resposta] [date] NULL,
	[data_inicio_survey] [datetime] NULL,
	[survey_status] [varchar](50) NULL,
	[convite_aberto] [char](3) NULL,
	[nps_assessor] [decimal](3, 1) NULL,
	[nps_xp] [decimal](3, 1) NULL,
	[recomendaria_assessor] [char](3) NULL,
	[classificacao_nps_assessor] [varchar](10) NULL,
	[classificacao_nps_xp] [varchar](10) NULL,
	[comentario_assessor] [nvarchar](max) NULL,
	[comentario_xp] [nvarchar](max) NULL,
	[razao_nps] [nvarchar](100) NULL,
	[razao_nps_assessor] [nvarchar](100) NULL,
	[razao_nps_xp] [nvarchar](100) NULL,
	[topicos_relevantes] [nvarchar](500) NULL,
	[data_carga] [datetime] NOT NULL,
 CONSTRAINT [PK_fact_nps_respostas_envios_aniversario] PRIMARY KEY CLUSTERED 
(
	[survey_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

-- ==============================================================================
-- 5. CONSTRAINTS E DEFAULTS
-- ==============================================================================

-- Default para data_carga
ALTER TABLE [silver].[fact_nps_respostas_envios_aniversario] ADD  DEFAULT (getdate()) FOR [data_carga]
GO

-- ==============================================================================
-- 6. DOCUMENTAÇÃO DAS COLUNAS (EXTENDED PROPERTIES)
-- ==============================================================================

-- As extended properties já estão definidas abaixo
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Identificador único da pesquisa NPS' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'survey_id'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Número da conta XP do cliente' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'conta_xp_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do assessor responsável pelo cliente' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de envio da pesquisa NPS ao cliente' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'data_entrega'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data em que o cliente respondeu a pesquisa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'data_resposta'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data e hora de início do preenchimento da pesquisa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'data_inicio_survey'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Status atual da pesquisa (completed, delivered_and_reminded, delivery_bounced, delivered, expired, not_sampled)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'survey_status'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Indica se o convite foi aberto pelo cliente (sim/nao)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'convite_aberto'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nota NPS (0-10) atribuída ao assessor' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'nps_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nota NPS (0-10) atribuída à XP' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'nps_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Indica se o cliente recomendaria o assessor (sim/nao)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'recomendaria_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Classificação NPS do assessor: Promotor (9-10), Neutro (7-8), Detrator (0-6)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'classificacao_nps_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Classificação NPS da XP: Promotor (9-10), Neutro (7-8), Detrator (0-6)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'classificacao_nps_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Comentário em texto livre do cliente sobre o assessor' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'comentario_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Comentário em texto livre do cliente sobre a XP' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'comentario_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Razão principal para a nota NPS atribuída' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'razao_nps'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Razão específica para a nota NPS do assessor (ex: cordialidade, conhecimento técnico, etc)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'razao_nps_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Razão específica para a nota NPS da XP (ex: rentabilidade, plataforma, educação financeira)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'razao_nps_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tópicos ou categorias identificadas nos comentários através de análise de texto' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'topicos_relevantes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data e hora de carga dos dados na tabela (preenchido automaticamente)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela fato contendo dados consolidados de pesquisas NPS de aniversário de relacionamento com clientes' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_nps_respostas_envios_aniversario'
GO

-- ==============================================================================
-- 7. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-20 | [Nome]         | Criação inicial da tabela

*/

-- ==============================================================================
-- 8. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Chave primária em survey_id garante unicidade da pesquisa
- Campos de texto (comentários) usam NVARCHAR(MAX) para suportar textos longos
- Classificação NPS segue padrão: Promotor (9-10), Neutro (7-8), Detrator (0-6)
- data_carga preenchida automaticamente na inserção
- Muitos campos são nullable pois nem todas pesquisas são respondidas

Status possíveis da pesquisa:
- completed: Pesquisa respondida completamente
- delivered: Pesquisa entregue mas não respondida
- delivered_and_reminded: Entregue e lembrete enviado
- delivery_bounced: Falha na entrega
- expired: Prazo expirado
- not_sampled: Não selecionado para amostra

Interpretação do NPS:
- Score = (% Promotores) - (% Detratores)
- Varia de -100 a +100
- Acima de 50: Excelente
- Entre 0 e 50: Bom
- Abaixo de 0: Precisa melhorar

Recomendações:
1. Criar índice em cod_assessor para consultas por assessor
2. Criar índice em data_resposta para análises temporais
3. Criar índice em survey_status para filtros de status
4. Implementar rotina de análise de sentimento nos comentários
5. Considerar partição por ano para grandes volumes

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
