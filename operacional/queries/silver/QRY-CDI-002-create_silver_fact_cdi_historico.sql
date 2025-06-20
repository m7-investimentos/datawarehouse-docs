-- ==============================================================================
-- QRY-CDI-002-CREATE_SILVER_FACT_CDI_HISTORICO
-- ==============================================================================
-- Tipo: DDL - Create Table
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [ddl, silver, fact_table, cdi, taxas, historico]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela fato fact_cdi_historico no schema silver para 
           armazenar dados históricos de taxas CDI com cálculos acumulados
           para diferentes períodos (mês, trimestre, semestre, ano e janelas móveis).

Casos de uso:
- Armazenar histórico diário de taxas CDI
- Base para cálculos de rentabilidade de produtos financeiros
- Análises comparativas de performance vs CDI
- Cálculo de benchmarks e indicadores
- Suporte a relatórios gerenciais e dashboards

Frequência de execução: Única (criação da tabela)
Volume esperado de linhas: ~7.000 registros históricos + ~250 registros/ano
*/

-- ==============================================================================
-- 2. ESTRUTURA DA TABELA
-- ==============================================================================
/*
Colunas da tabela:

| Coluna              | Tipo           | Nullable | Descrição                                                        |
|---------------------|----------------|----------|------------------------------------------------------------------|
| data_ref            | DATE           | NOT NULL | Data de referência da taxa CDI                                  |
| ano_mes             | VARCHAR(6)     | NULL     | Ano e mês no formato YYYYMM                                     |
| ano                 | INT            | NULL     | Ano extraído da data_ref                                        |
| mes_num             | INT            | NULL     | Número do mês (1-12)                                            |
| trimestre           | VARCHAR(2)     | NULL     | Identificador do trimestre (Q1-Q4)                               |
| semestre            | VARCHAR(2)     | NULL     | Identificador do semestre (S1-S2)                                |
| taxa_cdi_dia        | DECIMAL(18,8)  | NOT NULL | Taxa CDI do dia em formato decimal                               |
| taxa_cdi_mes        | DECIMAL(18,8)  | NOT NULL | Taxa CDI acumulada no mês corrente                              |
| taxa_cdi_3_meses    | DECIMAL(18,8)  | NOT NULL | Taxa CDI acumulada últimos 3 meses (janela móvel)               |
| taxa_cdi_6_meses    | DECIMAL(18,8)  | NOT NULL | Taxa CDI acumulada últimos 6 meses (janela móvel)               |
| taxa_cdi_12_meses   | DECIMAL(18,8)  | NOT NULL | Taxa CDI acumulada últimos 12 meses (janela móvel)              |
| taxa_cdi_trimestre  | DECIMAL(18,8)  | NOT NULL | Taxa CDI acumulada no trimestre corrente (período fixo)         |
| taxa_cdi_semestre   | DECIMAL(18,8)  | NOT NULL | Taxa CDI acumulada no semestre corrente (período fixo)          |
| taxa_cdi_ano        | DECIMAL(18,8)  | NOT NULL | Taxa CDI acumulada no ano corrente (YTD)                         |

Chave primária: Não definida (considerar data_ref como PK)
Índices: A definir baseado em padrões de consulta
*/

-- ==============================================================================
-- 3. DEPENDÊNCIAS
-- ==============================================================================
/*
Schemas necessários:
- silver: Schema onde a tabela será criada
- bronze: Schema de origem dos dados

Tabelas de origem (bronze):
- [bronze].[bc_cdi_historico]: Fonte oficial de taxas CDI do Banco Central
  * Campo utilizado: data_ref, taxa_cdi
  * Origem: API do Banco Central ou arquivo importado
  * Atualização: Diária (dias úteis)

Processo ETL:
- View: [silver].[vw_fact_cdi_historico] - Realiza todos os cálculos acumulados
- Procedure: [silver].[prc_bronze_to_silver_fact_cdi_historico] - Carga dados da view
- Execução: Diária (full load com TRUNCATE/INSERT)

Pré-requisitos:
- Schema silver deve existir
- Usuário deve ter permissão CREATE TABLE no schema silver
- Tabela bronze.bc_cdi_historico deve estar populada
- View vw_fact_cdi_historico deve ser criada após esta tabela
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
CREATE TABLE [silver].[fact_cdi_historico](
	[data_ref] [date] NOT NULL,
	[ano_mes] [varchar](6) NULL,
	[ano] [int] NULL,
	[mes_num] [int] NULL,
	[trimestre] [varchar](2) NULL,
	[semestre] [varchar](2) NULL,
	[taxa_cdi_dia] [decimal](18, 8) NOT NULL,
	[taxa_cdi_mes] [decimal](18, 8) NOT NULL,
	[taxa_cdi_3_meses] [decimal](18, 8) NOT NULL,
	[taxa_cdi_6_meses] [decimal](18, 8) NOT NULL,
	[taxa_cdi_12_meses] [decimal](18, 8) NOT NULL,
	[taxa_cdi_trimestre] [decimal](18, 8) NOT NULL,
	[taxa_cdi_semestre] [decimal](18, 8) NOT NULL,
	[taxa_cdi_ano] [decimal](18, 8) NOT NULL
) ON [PRIMARY]
GO

-- ==============================================================================
-- 5. DOCUMENTAÇÃO DAS COLUNAS (EXTENDED PROPERTIES)
-- ==============================================================================

-- Documentação da coluna data_ref
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'data de referência da taxa cdi herdada da tabela bronze. mantém integridade referencial com bronze.bc_cdi_historico. chave para joins com outras tabelas do dw', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'data_ref'
GO

-- Documentação da coluna ano_mes
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ano e mês concatenados no formato yyyymm (ex: 202506 para junho/2025). facilita agrupamentos e análises mensais. útil para particionamento e otimização de queries', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'ano_mes'
GO

-- Documentação da coluna ano
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ano extraído da data_ref no formato numérico (ex: 2025). facilita filtros e análises anuais', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'ano'
GO

-- Documentação da coluna mes_num
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'número do mês extraído da data_ref (1=janeiro até 12=dezembro). facilita ordenação cronológica e filtros por mês', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'mes_num'
GO

-- Documentação da coluna trimestre
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'identificador do trimestre no formato qn (q1=jan-mar, q2=abr-jun, q3=jul-set, q4=out-dez). facilita análises e agrupamentos trimestrais', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'trimestre'
GO

-- Documentação da coluna semestre
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'identificador do semestre no formato sn (s1=jan-jun, s2=jul-dez). facilita análises e agrupamentos semestrais', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'semestre'
GO

-- Documentação da coluna taxa_cdi_dia
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'taxa cdi diária em formato decimal. conversão da taxa bronze dividida por 100 (ex: 0.00054266 = 0,054266% a.d.). base para todos os cálculos de taxas acumuladas', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'taxa_cdi_dia'
GO

-- Documentação da coluna taxa_cdi_mes
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'taxa cdi acumulada no mês corrente até a data_ref. soma simples das taxas diárias do mês (não é juros compostos). reinicia a cada novo mês. usado para cálculo de rentabilidade mensal', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'taxa_cdi_mes'
GO

-- Documentação da coluna taxa_cdi_3_meses
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'taxa cdi acumulada dos últimos 3 meses (rolling window). soma das taxas diárias dos últimos ~90 dias. pode ter inconsistências no início da série histórica quando não há 3 meses completos de dados', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'taxa_cdi_3_meses'
GO

-- Documentação da coluna taxa_cdi_6_meses
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'taxa cdi acumulada dos últimos 6 meses (rolling window). soma das taxas diárias dos últimos ~180 dias. pode ter inconsistências no início da série histórica quando não há 6 meses completos de dados', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'taxa_cdi_6_meses'
GO

-- Documentação da coluna taxa_cdi_12_meses
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'taxa cdi acumulada dos últimos 12 meses (rolling window). soma das taxas diárias dos últimos ~365 dias. representa o "cdi anual" mais comumente usado. pode ter inconsistências no início da série histórica', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'taxa_cdi_12_meses'
GO

-- Documentação da coluna taxa_cdi_trimestre
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'taxa cdi acumulada no trimestre corrente (período fixo). soma das taxas desde o início do trimestre atual. reinicia a cada mudança de trimestre. diferente de taxa_cdi_3_meses que é rolling', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'taxa_cdi_trimestre'
GO

-- Documentação da coluna taxa_cdi_semestre
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'taxa cdi acumulada no semestre corrente (período fixo). soma das taxas desde o início do semestre atual. reinicia a cada mudança de semestre. diferente de taxa_cdi_6_meses que é rolling', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'taxa_cdi_semestre'
GO

-- Documentação da coluna taxa_cdi_ano
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'taxa cdi acumulada no ano corrente - year to date (ytd). soma das taxas desde 01 de janeiro do ano atual. reinicia todo início de ano. diferente de taxa_cdi_12_meses que é rolling 12 months', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'taxa_cdi_ano'
GO

-- Documentação da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'tabela silver fato que processa e enriquece os dados brutos de cdi da camada bronze. calcula automaticamente taxas acumuladas para diferentes períodos (mês, trimestre, semestre, ano, 3/6/12 meses). facilita análises de rentabilidade e cálculos baseados em cdi. sincronizada diariamente com a tabela bronze via processo etl', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_cdi_historico'
GO

-- ==============================================================================
-- 6. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-20 | [Nome]         | Criação inicial da tabela

*/

-- ==============================================================================
-- 7. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Tabela não possui chave primária definida - data_ref deve ser única
- Taxas são armazenadas em formato decimal (não percentual)
- Cálculos acumulados são realizados pela view vw_fact_cdi_historico
- Considera apenas dias úteis (quando há cotação CDI)
- Janelas móveis (3, 6, 12 meses) vs períodos fixos (trimestre, semestre, ano)

Diferenças entre tipos de acumulação:
- Janelas móveis: Sempre consideram N meses para trás da data atual
- Períodos fixos: Reiniciam no início de cada período (Q1, S1, etc.)

Recomendações:
1. Adicionar constraint de chave primária em data_ref
2. Criar índice em ano_mes para consultas agregadas mensais
3. Criar índice em (ano, mes_num) para consultas com filtros temporais
4. Adicionar constraint CHECK para valores >= 0 nas colunas de taxa
5. Considerar particionamento por ano para grandes volumes

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
