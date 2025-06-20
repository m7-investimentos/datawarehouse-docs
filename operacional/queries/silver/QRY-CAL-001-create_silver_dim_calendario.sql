-- ==============================================================================
-- QRY-CAL-001-create_silver_dim_calendario
-- ==============================================================================
-- Tipo: DDL - CREATE TABLE
-- Versão: 1.0.0
-- Última atualização: 2024-11-28
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [dimensão, calendário, temporal, silver]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela dimensão calendário para análises temporais no Data Warehouse.
Esta tabela contém informações detalhadas sobre datas, permitindo análises por diferentes
granularidades temporais (dia, mês, trimestre, semestre, ano).

Casos de uso:
- Análises temporais de indicadores de performance
- Segmentação de dados por períodos (trimestre, semestre)
- Identificação de dias úteis vs não úteis
- Join com tabelas fato para análises temporais

Frequência de execução: Única (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: ~7.300 registros (20 anos)
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Não aplicável - Script DDL de criação de tabela
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Colunas criadas:

| Coluna              | Tipo         | Descrição                           | Exemplo           |
|---------------------|--------------|-------------------------------------|-------------------|
| data_ref            | DATE         | Data de referência (PK)             | 2024-03-15        |
| dia                 | TINYINT      | Dia do mês (1-31)                   | 15                |
| mes                 | SMALLINT     | Mês do ano (1-12)                   | 3                 |
| ano                 | SMALLINT     | Ano de referência                   | 2024              |
| ano_mes             | CHAR(6)      | Ano e mês (AAAAMM)                  | 202403            |
| nome_mes            | VARCHAR(20)  | Nome do mês por extenso             | Março             |
| trimestre           | CHAR(2)      | Trimestre (Q1-Q4)                   | Q1                |
| numero_da_semana    | TINYINT      | Número da semana no ano (1-53)      | 11                |
| dia_da_semana       | VARCHAR(20)  | Nome do dia por extenso             | Sexta-feira       |
| dia_da_semana_num   | TINYINT      | Número do dia (1=seg, 7=dom)        | 6                 |
| tipo_dia            | VARCHAR(15)  | Classificação do dia                | útil              |
| observacoes         | VARCHAR(200) | Observações adicionais              | Feriado Nacional  |

Chave primária: data_ref
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- Nenhuma (criação inicial)

Funções/Procedures chamadas:
- sys.sp_addextendedproperty: Adição de metadados descritivos

Pré-requisitos:
- Schema silver deve existir
- Permissões CREATE TABLE no schema silver
- Permissões para adicionar extended properties
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ==============================================================================
-- 6. CRIAÇÃO DA TABELA
-- ==============================================================================
CREATE TABLE [silver].[dim_calendario](
	[data_ref] [date] NOT NULL,
	[dia] [tinyint] NOT NULL,
	[mes] [smallint] NULL,
	[ano] [smallint] NOT NULL,
	[ano_mes] [char](6) NOT NULL,
	[nome_mes] [varchar](20) NOT NULL,
	[trimestre] [char](2) NOT NULL,
	[numero_da_semana] [tinyint] NOT NULL,
	[dia_da_semana] [varchar](20) NOT NULL,
	[dia_da_semana_num] [tinyint] NOT NULL,
	[tipo_dia] [varchar](15) NOT NULL,
	[observacoes] [varchar](200) NULL,
 CONSTRAINT [PK_dim_calendario] PRIMARY KEY CLUSTERED 
(
	[data_ref] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- ==============================================================================
-- 7. DOCUMENTAÇÃO DOS CAMPOS (EXTENDED PROPERTIES)
-- ==============================================================================
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do calendário' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'dia do mês (1-31)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'dia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'mês do ano (1-12)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano de referência' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano e mês no formato AAAAMM' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'ano_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome do mês por extenso' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'nome_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'trimestre do ano (Q1-Q4)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'número da semana no ano (1-53)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'numero_da_semana'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome do dia da semana por extenso' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'dia_da_semana'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'número do dia da semana (1=segunda, 7=domingo)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'dia_da_semana_num'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tipo do dia (útil, sábado, domingo, feriado)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'tipo_dia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'observações adicionais sobre a data' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario', @level2type=N'COLUMN',@level2name=N'observacoes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela dimensão que contém as informações de calendário para análise temporal' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_calendario'
GO

-- ==============================================================================
-- 8. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2024-11-28 | [Nome]         | Criação inicial da tabela

*/

-- ==============================================================================
-- 9. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Esta tabela deve ser populada através da procedure prc_load_silver_dim_calendario
- A coluna tipo_dia permite identificação rápida de dias úteis para cálculos
- A primary key clustered em data_ref otimiza joins por data
- Considerar criar índices não clustered em ano_mes para queries agregadas

Troubleshooting comum:
1. Erro de duplicação de chave: Verificar se a data já existe antes de inserir
2. Valores NULL em campos NOT NULL: Garantir que a procedure de carga popule todos campos obrigatórios

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/