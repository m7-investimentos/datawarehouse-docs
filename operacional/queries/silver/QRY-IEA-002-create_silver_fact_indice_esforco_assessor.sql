-- ==============================================================================
-- QRY-IEA-002-CREATE_SILVER_FACT_INDICE_ESFORCO_ASSESSOR
-- ==============================================================================
-- Tipo: DDL - Create Table
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [ddl, silver, fact_table, indice_esforco, assessor, performance]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela fato fact_indice_esforco_assessor no schema silver.
           Armazena métricas de performance e esforço dos assessores, incluindo
           indicadores de prospecção e relacionamento, bem como médias móveis
           acumuladas para análise de tendências.

Casos de uso:
- Acompanhamento de performance mensal dos assessores
- Análise de esforço em prospecção vs relacionamento
- Identificação de tendências de performance (3, 6, 12 meses)
- Base para remuneração variável e metas
- Suporte a dashboards de gestão comercial

Frequência de execução: Única (criação da tabela)
Volume esperado de linhas: ~3.000 registros/mês (~250 assessores x 12 meses)
*/

-- ==============================================================================
-- 2. ESTRUTURA DA TABELA
-- ==============================================================================
/*
Colunas da tabela:

| Coluna                                           | Tipo          | Nullable | Descrição                                              |
|--------------------------------------------------|---------------|----------|--------------------------------------------------------|
| ano_mes                                          | VARCHAR(6)    | NOT NULL | Ano e mês de referência (YYYYMM)                     |
| cod_assessor                                     | VARCHAR(20)   | NOT NULL | Código do assessor                                    |
| indice_esforco_assessor                          | DECIMAL(18,8) | NOT NULL | Índice geral de esforço (IEA)                         |
| indice_esforco_assessor_acum_3_meses             | DECIMAL(18,8) | NULL     | Média móvel 3 meses do IEA                           |
| indice_esforco_assessor_acum_6_meses             | DECIMAL(18,8) | NULL     | Média móvel 6 meses do IEA                           |
| indice_esforco_assessor_acum_12_meses            | DECIMAL(18,8) | NULL     | Média móvel 12 meses do IEA                          |
| esforco_prospeccao                               | DECIMAL(18,8) | NOT NULL | Índice de esforço em prospecção                       |
| esforco_relacionamento                           | DECIMAL(18,8) | NOT NULL | Índice de esforço em relacionamento                  |
| prospeccao_captacao_de_novos_clientes_por_aai    | DECIMAL(16,2) | NOT NULL | Valor captado de novos clientes                       |
| prospeccao_atingimento_lead_starts               | DECIMAL(18,8) | NOT NULL | % atingimento meta lead starts                        |
| prospeccao_atingimento_habilitacoes              | DECIMAL(18,8) | NOT NULL | % atingimento meta habilitações                       |
| prospeccao_atingimento_conversao                 | DECIMAL(18,8) | NOT NULL | % atingimento meta conversão                          |
| prospeccao_atingimento_carteiras_simuladas_novos | DECIMAL(18,8) | NOT NULL | % atingimento meta carteiras simuladas                |
| relacionamento_captacao_da_base                  | DECIMAL(18,2) | NOT NULL | Valor captado da base existente                       |
| relacionamento_atingimento_contas_aportarem      | DECIMAL(18,8) | NOT NULL | % atingimento meta contas com aporte                  |
| relacionamento_atingimento_ordens_enviadas       | DECIMAL(18,8) | NOT NULL | % atingimento meta ordens enviadas                    |
| relacionamento_atingimento_contas_acessadas_hub  | DECIMAL(18,8) | NOT NULL | % atingimento meta contas acessadas no hub            |
| data_carga                                       | DATETIME      | NOT NULL | Data/hora da carga dos dados (default: GETDATE())     |

Chave composta sugerida: (ano_mes, cod_assessor)
*/

-- ==============================================================================
-- 3. DEPENDÊNCIAS
-- ==============================================================================
/*
Schemas necessários:
- silver: Schema onde a tabela será criada
- bronze: Schema de origem dos dados

Tabelas de origem (bronze):
- [bronze].[xp_iea]: Fonte de dados do Índice de Esforço do Assessor
  * Contém métricas mensais calculadas pelo sistema XP
  * Campos mapeados diretamente para esta tabela

Views relacionadas:
- [silver].[vw_fact_indice_esforco_assessor]: View que calcula médias móveis
  * Processa dados da bronze e adiciona cálculos de janelas móveis
  * Fonte para a procedure de carga

Processo ETL:
- Procedure: [silver].[prc_bronze_to_silver_fact_indice_esforco_assessor]
- Execução: Mensal (após fechamento do mês)
- Modo: Full load (TRUNCATE/INSERT)

Pré-requisitos:
- Schema silver deve existir
- Tabela bronze.xp_iea deve estar atualizada
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
CREATE TABLE [silver].[fact_indice_esforco_assessor](
	[ano_mes] [varchar](6) NOT NULL,
	[cod_assessor] [varchar](20) NOT NULL,
	[indice_esforco_assessor] [decimal](18, 8) NOT NULL,
	[indice_esforco_assessor_acum_3_meses] [decimal](18, 8) NULL,
	[indice_esforco_assessor_acum_6_meses] [decimal](18, 8) NULL,
	[indice_esforco_assessor_acum_12_meses] [decimal](18, 8) NULL,
	[esforco_prospeccao] [decimal](18, 8) NOT NULL,
	[esforco_relacionamento] [decimal](18, 8) NOT NULL,
	[prospeccao_captacao_de_novos_clientes_por_aai] [decimal](16, 2) NOT NULL,
	[prospeccao_atingimento_lead_starts] [decimal](18, 8) NOT NULL,
	[prospeccao_atingimento_habilitacoes] [decimal](18, 8) NOT NULL,
	[prospeccao_atingimento_conversao] [decimal](18, 8) NOT NULL,
	[prospeccao_atingimento_carteiras_simuladas_novos] [decimal](18, 8) NOT NULL,
	[relacionamento_captacao_da_base] [decimal](18, 2) NOT NULL,
	[relacionamento_atingimento_contas_aportarem] [decimal](18, 8) NOT NULL,
	[relacionamento_atingimento_ordens_enviadas] [decimal](18, 8) NOT NULL,
	[relacionamento_atingimento_contas_acessadas_hub] [decimal](18, 8) NOT NULL,
	[data_carga] [datetime] NOT NULL
) ON [PRIMARY]
GO

-- ==============================================================================
-- 5. CONSTRAINTS E DEFAULTS
-- ==============================================================================

-- Default para data_carga
ALTER TABLE [silver].[fact_indice_esforco_assessor] ADD  DEFAULT (getdate()) FOR [data_carga]
GO

-- ==============================================================================
-- 6. DOCUMENTAÇÃO DAS COLUNAS (EXTENDED PROPERTIES)
-- ==============================================================================

-- Documentação da coluna ano_mes
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ano e mês de referência no formato YYYYMM. exemplo: 202412 para dezembro/2024' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'ano_mes'
GO

-- Documentação da coluna cod_assessor
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'código do assessor no sistema. relaciona com dim_pessoas.cod_aai' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO

-- Documentação da coluna indice_esforco_assessor
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'índice geral de esforço do assessor (IEA). métrica composta que considera prospecção e relacionamento' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'indice_esforco_assessor'
GO

-- Documentação das médias móveis
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'média móvel simples do IEA dos últimos 3 meses incluindo o mês atual' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'indice_esforco_assessor_acum_3_meses'
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'média móvel simples do IEA dos últimos 6 meses incluindo o mês atual' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'indice_esforco_assessor_acum_6_meses'
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'média móvel simples do IEA dos últimos 12 meses incluindo o mês atual' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'indice_esforco_assessor_acum_12_meses'
GO

-- Documentação dos esforços
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'índice de esforço em atividades de prospecção (captação de novos clientes)' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'esforco_prospeccao'
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'índice de esforço em atividades de relacionamento (base existente)' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'esforco_relacionamento'
GO

-- Documentação das métricas de prospecção
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'valor total captado de novos clientes pelo assessor no mês em reais' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'prospeccao_captacao_de_novos_clientes_por_aai'
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'percentual de atingimento da meta de lead starts (primeiros contatos)' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'prospeccao_atingimento_lead_starts'
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'percentual de atingimento da meta de habilitações de novos clientes' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'prospeccao_atingimento_habilitacoes'
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'percentual de atingimento da meta de conversão de prospects em clientes' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'prospeccao_atingimento_conversao'
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'percentual de atingimento da meta de carteiras simuladas para novos clientes' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'prospeccao_atingimento_carteiras_simuladas_novos'
GO

-- Documentação das métricas de relacionamento
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'valor total captado da base existente de clientes em reais' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'relacionamento_captacao_da_base'
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'percentual de atingimento da meta de contas que realizaram aportes' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'relacionamento_atingimento_contas_aportarem'
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'percentual de atingimento da meta de ordens enviadas pela base' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'relacionamento_atingimento_ordens_enviadas'
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'percentual de atingimento da meta de contas que acessaram o hub' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'relacionamento_atingimento_contas_acessadas_hub'
GO

-- Documentação da coluna data_carga
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'data e hora da carga dos dados. preenchida automaticamente via default getdate()' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor', 
    @level2type=N'COLUMN',@level2name=N'data_carga'
GO

-- Documentação da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'tabela fato que armazena métricas mensais de performance dos assessores. inclui índice de esforço do assessor (IEA), métricas de prospecção e relacionamento, bem como médias móveis para análise de tendências. base para remuneração variável e acompanhamento de metas.' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_indice_esforco_assessor'
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
- Tabela não possui chave primária definida - considerar (ano_mes, cod_assessor)
- Médias móveis calculadas pela view vw_fact_indice_esforco_assessor
- Valores de atingimento são percentuais (0.5 = 50%, 1.0 = 100%)
- Valores monetários em reais brasileiros
- data_carga preenchida automaticamente na inserção

Interpretação das métricas:
- IEA > 1.0: Assessor superou as metas
- IEA = 1.0: Assessor atingiu exatamente as metas
- IEA < 1.0: Assessor ficou abaixo das metas

Médias móveis:
- 3 meses: Visão de curto prazo (trimestre)
- 6 meses: Visão de médio prazo (semestre)
- 12 meses: Visão de longo prazo (anual)

Recomendações:
1. Criar PK composta em (ano_mes, cod_assessor)
2. Criar índice em cod_assessor para consultas por assessor
3. Criar índice em ano_mes para consultas temporais
4. Implementar checks para valores entre 0 e N (atingimento)
5. Considerar particionamento por ano_mes para grandes volumes

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
