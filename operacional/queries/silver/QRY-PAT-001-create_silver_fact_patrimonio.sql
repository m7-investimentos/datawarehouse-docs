-- ==============================================================================
-- QRY-PAT-001 - Criação da Tabela fact_patrimonio
-- ==============================================================================
-- Tipo: Query
-- Versão: 1.0.0
-- Última atualização: 2025-01-13
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [silver, fact, patrimonio, cliente, open-investment]
-- Status: rascunho
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela de fatos fact_patrimonio no schema silver para armazenar 
informações sobre o patrimônio dos clientes, incluindo patrimônio na XP, patrimônio 
declarado e investimentos em outras instituições (Open Investment).

Casos de uso:
- Análise de share of wallet dos clientes
- Acompanhamento da evolução patrimonial
- Identificação de oportunidades de captação
- Relatórios de patrimônio consolidado

Frequência de execução: Única (criação de tabela)
Tempo médio de execução: < 5 segundos
Volume esperado de linhas: Aproximadamente 1M registros/mês
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Esta query não requer parâmetros de entrada por ser uma DDL de criação de tabela.
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Colunas da tabela:

| Coluna                      | Tipo           | Descrição                                                                        | Exemplo           |
|-----------------------------|----------------|----------------------------------------------------------------------------------|-------------------|
| data_ref                    | DATE           | Data de referência para os dados de patrimônio                                  | 2025-01-13        |
| conta_xp_cliente            | INTEGER        | Número da conta do cliente na XP                                                | 12345678          |
| patrimonio_xp               | DECIMAL(18,2)  | Valor do patrimônio do cliente na XP                                            | 150000.50         |
| patrimonio_declarado        | DECIMAL(18,2)  | Valor do patrimônio declarado pelo cliente                                      | 500000.00         |
| share_of_wallet             | DECIMAL(18,2)  | Percentil do cliente na carteira de investimentos comparado com seu patrimônio  | 30.00             |
| patrimonio_open_investment  | DECIMAL(18,2)  | Valor do investimento do cliente em outras instituições                         | 350000.00         |

Chave primária: Não definida (considerar adicionar PK composta em data_ref + conta_xp_cliente)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Schemas necessários:
- silver: Schema onde a tabela será criada

Pré-requisitos:
- Permissão CREATE TABLE no schema silver
- Schema silver deve existir
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- Configurações específicas do SQL Server
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ==============================================================================
-- 6. CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [silver].[fact_patrimonio](
	[data_ref] [date] NOT NULL,
	[conta_xp_cliente] [int] NOT NULL,
	[patrimonio_xp] [decimal](18, 2) NULL,
	[patrimonio_declarado] [decimal](18, 2) NULL,
	[share_of_wallet] [decimal](18, 2) NULL,
	[patrimonio_open_investment] [decimal](18, 2) NULL
) ON [PRIMARY]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de referência para os dados de patrimônio' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Número da conta do cliente na XP' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio', @level2type=N'COLUMN',@level2name=N'conta_xp_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Valor do patrimônio do cliente na XP' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio', @level2type=N'COLUMN',@level2name=N'patrimonio_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Valor do patrimônio declarado pelo cliente' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio', @level2type=N'COLUMN',@level2name=N'patrimonio_declarado'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'percentil do cliente na carteira de investimentos comparado com o seu patrimonio declarado' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio', @level2type=N'COLUMN',@level2name=N'share_of_wallet'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Valor do investimento do cliente em outras instituições' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio', @level2type=N'COLUMN',@level2name=N'patrimonio_open_investment'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela de fatos contendo informações sobre o patrimônio dos clientes, incluindo patrimônio na XP, patrimônio declarado e investimentos em outras instituicoes' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_patrimonio'
GO
