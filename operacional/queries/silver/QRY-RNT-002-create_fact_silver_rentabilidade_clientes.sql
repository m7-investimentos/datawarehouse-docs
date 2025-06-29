-- ==============================================================================
-- QRY-RNT-002-create_fact_silver_rentabilidade_clientes
-- ==============================================================================
-- Tipo: DDL - CREATE TABLE
-- Versão: 1.0.0
-- Última atualização: 2024-11-28
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [fato, rentabilidade, performance, silver]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela fato de rentabilidade de clientes no Data Warehouse.
Esta tabela contém dados processados de rentabilidade mensal e acumulada dos
portfólios dos clientes, com valores já convertidos de percentual para decimal,
prontos para cálculos e análises.

Casos de uso:
- Análise de performance de carteiras por período
- Cálculo de rentabilidade acumulada (3, 6, 12 meses)
- Comparação de rentabilidade entre clientes e períodos
- Relatórios de performance para assessores
- Análise de volatilidade e consistência de retornos
- Dashboards de acompanhamento mensal

Frequência de execução: Única (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: ~50.000 registros/mês (clientes ativos x meses)
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

| Coluna                             | Tipo         | Descrição                                                    | Exemplo      |
|------------------------------------|--------------|--------------------------------------------------------------|--------------|  
| conta_xp_cliente                   | INT          | Código único do cliente (PK parte 1)                        | 123456       |
| ano_mes                            | INT          | Período YYYYMM (PK parte 2)                                 | 202401       |
| ano                                | INT          | Ano de referência                                            | 2024         |
| semestre                           | VARCHAR(2)   | Semestre (S1, S2)                                           | 'S1'         |
| trimestre                          | VARCHAR(2)   | Trimestre (Q1-Q4)                                           | 'Q1'         |
| mes_num                            | INT          | Número do mês (1-12)                                        | 3            |
| mes                                | VARCHAR(5)   | Nome abreviado do mês                                       | 'Mar'        |
| rentabilidade                      | DECIMAL(18,8)| Rentabilidade mensal em decimal                             | 0.01580000   |
| rentabilidade_acumulada_3_meses    | DECIMAL(18,8)| Rentab. acum. 3 meses (janela móvel)                       | 0.04852300   |
| rentabilidade_acumulada_6_meses    | DECIMAL(18,8)| Rentab. acum. 6 meses (janela móvel)                       | 0.10234500   |
| rentabilidade_acumulada_12_meses   | DECIMAL(18,8)| Rentab. acum. 12 meses (janela móvel)                      | 0.22457800   |
| rentabilidade_acumulada_trimestre  | DECIMAL(18,8)| Rentab. acum. trimestre atual (YTD trimestral)              | 0.04852300   |
| rentabilidade_acumulada_semestre   | DECIMAL(18,8)| Rentab. acum. semestre atual (YTD semestral)               | 0.10234500   |
| rentabilidade_acumulada_ano        | DECIMAL(18,8)| Rentab. acum. ano atual (YTD)                               | 0.10234500   |
| data_carga                         | DATETIME     | Timestamp da carga (default: GETDATE())                      | 2024-03-15   |

Chave primária: Composta (conta_xp_cliente, ano_mes) - CLUSTERED
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- Nenhuma (criação inicial)

Tabelas de origem (para carga via procedure):
- bronze.xp_portfolio_rentabilidade: Dados brutos de rentabilidade
- bronze.xp_clientes: Dados de clientes (para validação)

Funções/Procedures chamadas:
- sys.sp_addextendedproperty: Adição de metadados descritivos

Pré-requisitos:
- Schema silver deve existir
- Permissões CREATE TABLE no schema silver
- Permissões para adicionar extended properties
- Executar procedure prc_bronze_to_silver_fact_rentabilidade_clientes após criação
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
CREATE TABLE [silver].[fact_rentabilidade_clientes](
	[conta_xp_cliente] [int] NOT NULL,
	[ano_mes] [int] NOT NULL,
	[ano] [int] NULL,
	[semestre] [varchar](2) NULL,
	[trimestre] [varchar](2) NULL,
	[mes_num] [int] NULL,
	[mes] [varchar](5) NULL,
	[rentabilidade] [decimal](18, 8) NULL,
	[rentabilidade_acumulada_3_meses] [decimal](18, 8) NULL,
	[rentabilidade_acumulada_6_meses] [decimal](18, 8) NULL,
	[rentabilidade_acumulada_12_meses] [decimal](18, 8) NULL,
	[rentabilidade_acumulada_trimestre] [decimal](18, 8) NULL,
	[rentabilidade_acumulada_semestre] [decimal](18, 8) NULL,
	[rentabilidade_acumulada_ano] [decimal](18, 8) NULL,
	[data_carga] [datetime] NULL,
 CONSTRAINT [PK_fact_rentabilidade_clientes] PRIMARY KEY CLUSTERED 
(
	[conta_xp_cliente] ASC,
	[ano_mes] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- ==============================================================================
-- 7. CONSTRAINTS E DEFAULTS
-- ==============================================================================
ALTER TABLE [silver].[fact_rentabilidade_clientes] ADD  DEFAULT (getdate()) FOR [data_carga]
GO

-- ==============================================================================
-- 8. DOCUMENTAÇÃO DOS CAMPOS (EXTENDED PROPERTIES)
-- ==============================================================================
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código único do cliente na XP. Parte da chave primária composta (conta_xp_cliente + ano_mes).' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'conta_xp_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Período no formato YYYYMM (202501 = Janeiro/2025). Calculado como (ano * 100 + mes_num). Parte da chave primária composta.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'ano_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Ano de referência (formato: YYYY). Usado para agregações e filtros anuais.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Identificador do semestre (S1 = Jan-Jun, S2 = Jul-Dez). Campo calculado para facilitar agregações semestrais.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Identificador do trimestre (Q1 = Jan-Mar, Q2 = Abr-Jun, Q3 = Jul-Set, Q4 = Out-Dez). Campo calculado para análises trimestrais.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Número do mês (1-12). Mantido para compatibilidade e cálculos.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'mes_num'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome abreviado do mês (Jan, Fev, Mar, etc). Mantido da bronze para visualização.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade mensal EM DECIMAL. IMPORTANTE: Valor já convertido de percentual para decimal (bronze.portfolio_rentabilidade / 100). Ex: 10.58% na bronze = 0.1058 aqui na silver. Pronto para uso em cálculos.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade acumulada dos últimos 3 meses (janela móvel) EM DECIMAL. Calculada usando a fórmula de juros compostos: ((1+r1)*(1+r2)*(1+r3))-1. Inclui o mês atual + 2 meses anteriores.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade_acumulada_3_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade acumulada dos últimos 6 meses (janela móvel) EM DECIMAL. Calculada com juros compostos incluindo mês atual + 5 meses anteriores. Valor já em decimal, pronto para uso.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade_acumulada_6_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade acumulada dos últimos 12 meses (janela móvel) EM DECIMAL. Calculada com juros compostos dos últimos 12 meses. Importante para análise de performance anualizada.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade_acumulada_12_meses'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade acumulada do trimestre ATUAL (não é janela móvel) EM DECIMAL. Reseta no início de cada trimestre (Jan, Abr, Jul, Out). Ex: em março, acumula Jan+Fev+Mar; em abril, começa novo acumulado.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade_acumulada_trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade acumulada do semestre ATUAL (não é janela móvel) EM DECIMAL. Reseta em Janeiro e Julho. Acumula progressivamente dentro do semestre fiscal.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade_acumulada_semestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Rentabilidade acumulada do ano ATUAL (YTD - Year to Date) EM DECIMAL. Calculada dinamicamente pela view, não confia no campo da bronze. Reseta em Janeiro de cada ano.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'rentabilidade_acumulada_ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Timestamp de quando o registro foi inserido/atualizado na silver. Preenchido automaticamente com GETDATE(). Para dados históricos preservados, mantém a data_carga original.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela silver com dados processados de rentabilidade. Um registro único por cliente/mês. Inclui cálculos de rentabilidade acumulada em múltiplas janelas temporais. Valores já convertidos para decimal.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_rentabilidade_clientes'
GO

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2024-11-28 | [Nome]         | Criação inicial da tabela

*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- VALORES EM DECIMAL: Todos os campos de rentabilidade estão em formato decimal (não percentual)
- Conversão: bronze.portfolio_rentabilidade / 100 = silver.rentabilidade
- Rentabilidades acumuladas usam fórmula de juros compostos: ((1+r1)*(1+r2)...(1+rn))-1
- Janelas móveis (3,6,12 meses) vs. períodos fiscais (trimestre, semestre, ano)
- Chave primária composta garante unicidade por cliente/mês
- Campo data_carga preserva histórico de processamento

Diferenças entre acumulados:
- Janelas móveis (3,6,12): Sempre consideram N meses anteriores + mês atual
- Períodos fiscais (Q,S,Y): Resetam no início do período (Jan, Abr, Jul, Out para Q)

Troubleshooting comum:
1. Valores em percentual: Verificar conversão na procedure de carga
2. Acumulados incorretos: Validar fórmula de juros compostos e NULLs
3. Duplicação de chave: Verificar unicidade conta_xp_cliente + ano_mes

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
