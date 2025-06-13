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

-- ==============================================================================
-- 7. DOCUMENTAÇÃO DAS COLUNAS (EXTENDED PROPERTIES)
-- ==============================================================================

-- Documentação da coluna data_ref
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de referência para os dados de patrimônio',
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'fact_patrimonio',
    @level2type=N'COLUMN', @level2name=N'data_ref'
GO

-- Documentação da coluna conta_xp_cliente
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Número da conta do cliente na XP',
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'fact_patrimonio',
    @level2type=N'COLUMN', @level2name=N'conta_xp_cliente'
GO

-- Documentação da coluna patrimonio_xp
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Valor do patrimônio do cliente na XP',
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'fact_patrimonio',
    @level2type=N'COLUMN', @level2name=N'patrimonio_xp'
GO

-- Documentação da coluna patrimonio_declarado
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Valor do patrimônio declarado pelo cliente',
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'fact_patrimonio',
    @level2type=N'COLUMN', @level2name=N'patrimonio_declarado'
GO

-- Documentação da coluna share_of_wallet
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Percentil do cliente na carteira de investimentos comparado com o seu patrimônio declarado',
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'fact_patrimonio',
    @level2type=N'COLUMN', @level2name=N'share_of_wallet'
GO

-- Documentação da coluna patrimonio_open_investment
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Valor do investimento do cliente em outras instituições',
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'fact_patrimonio',
    @level2type=N'COLUMN', @level2name=N'patrimonio_open_investment'
GO

-- Documentação da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela de fatos contendo informações sobre o patrimônio dos clientes, incluindo patrimônio na XP, patrimônio declarado e investimentos em outras instituições',
    @level0type=N'SCHEMA', @level0name=N'silver',
    @level1type=N'TABLE', @level1name=N'fact_patrimonio'
GO

-- ==============================================================================
-- 8. ÍNDICES RECOMENDADOS (COMENTADOS PARA IMPLEMENTAÇÃO FUTURA)
-- ==============================================================================
/*
-- Índice clustered na chave primária composta
CREATE CLUSTERED INDEX IX_fact_patrimonio_data_conta 
ON [silver].[fact_patrimonio] ([data_ref], [conta_xp_cliente])
GO

-- Índice para consultas por conta
CREATE NONCLUSTERED INDEX IX_fact_patrimonio_conta 
ON [silver].[fact_patrimonio] ([conta_xp_cliente])
INCLUDE ([patrimonio_xp], [patrimonio_declarado], [share_of_wallet])
GO

-- Índice para análises de share of wallet
CREATE NONCLUSTERED INDEX IX_fact_patrimonio_share 
ON [silver].[fact_patrimonio] ([share_of_wallet])
WHERE [share_of_wallet] IS NOT NULL
GO
*/

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-13 | [Nome]         | Criação inicial da tabela fact_patrimonio

*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- A tabela não possui chave primária definida. Recomenda-se adicionar uma PK composta 
  em (data_ref, conta_xp_cliente) para garantir unicidade
- O campo share_of_wallet representa o percentual do patrimônio declarado que está 
  investido na XP (patrimonio_xp / patrimonio_declarado * 100)
- Dados de Open Investment são obtidos através de APIs externas e podem ter defasagem
- Considerar particionamento por data_ref para tabelas com grande volume de dados

Troubleshooting comum:
1. Erro de permissão: Verificar se o usuário tem permissão CREATE TABLE no schema silver
2. Schema não existe: Executar CREATE SCHEMA silver antes desta query
3. Tipos de dados: DECIMAL(18,2) suporta valores até 9.999.999.999.999.999,99

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/