-- ==============================================================================
-- QRY-RNT-001-create_xperformance_rentabilidade_cliente
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: dados@m7investimentos.com.br
-- Revisor: dados@m7investimentos.com.br
-- Tags: [ddl, bronze, xperformance, rentabilidade, cliente, portfolio]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.xperformance_rentabilidade_cliente para armazenar 
dados brutos de rentabilidade mensal dos portfolios dos clientes extraídos dos 
relatórios XPerformance. Alimentada mensalmente com dados do ano vigente + 2 anos 
anteriores.

Casos de uso:
- Análise de performance de carteiras
- Comparação de rentabilidade vs benchmarks
- Identificação de clientes com baixa/alta performance
- Base para relatórios de rentabilidade

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 1300 registros/mês
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
Tabela criada: bronze.xperformance_rentabilidade_cliente

Colunas principais:
- conta_xp_cliente: Código do cliente
- data_relatorio: Data de extração (versão)
- ano/mes_num: Período de referência
- portfolio_rentabilidade: Rentabilidade mensal (%)
- acumulado_ano: Rentabilidade YTD (%)
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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[xperformance_rentabilidade_cliente]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.xperformance_rentabilidade_cliente já existe. Dropando...';
    DROP TABLE [bronze].[xperformance_rentabilidade_cliente];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[xperformance_rentabilidade_cliente](
	[conta_xp_cliente] [int] NOT NULL,
	[data_relatorio] [date] NOT NULL,
	[ano] [int] NOT NULL,
	[mes] [varchar](5) NOT NULL,
	[mes_num] [int] NOT NULL,
	[portfolio_rentabilidade] [decimal](18, 4) NULL,
	[acumulado_ano] [decimal](18, 4) NULL,
	[data_carga] [datetime] NOT NULL
) ON [PRIMARY];
GO

ALTER TABLE [bronze].[xperformance_rentabilidade_cliente] ADD DEFAULT (getdate()) FOR [data_carga];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por cliente e período
CREATE CLUSTERED INDEX [IX_bronze_rent_cliente_periodo]
ON [bronze].[xperformance_rentabilidade_cliente] ([conta_xp_cliente], [ano], [mes_num]);
GO

-- Índice para busca por data de relatório
CREATE NONCLUSTERED INDEX [IX_bronze_rent_data_relatorio]
ON [bronze].[xperformance_rentabilidade_cliente] ([data_relatorio])
INCLUDE ([conta_xp_cliente], [ano], [mes_num]);
GO

-- Índice para análise de performance
CREATE NONCLUSTERED INDEX [IX_bronze_rent_performance]
ON [bronze].[xperformance_rentabilidade_cliente] ([ano], [mes_num])
INCLUDE ([portfolio_rentabilidade])
WHERE [portfolio_rentabilidade] IS NOT NULL;
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela bronze com dados brutos de rentabilidade de clientes extraídos dos relatórios XPerformance. Alimentada mensalmente com dados do ano vigente + 2 anos anteriores. Contém duplicatas por data_relatorio.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente';
GO

-- Documentação das colunas
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código único do cliente na XP. Identificador principal do cliente no sistema.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', 
    @level2type=N'COLUMN',@level2name=N'conta_xp_cliente';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de extração do relatório (último dia útil do mês). Fundamental para identificar a versão mais recente dos dados, pois o mesmo período pode aparecer em múltiplos relatórios.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', 
    @level2type=N'COLUMN',@level2name=N'data_relatorio';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Ano de referência da rentabilidade (formato: YYYY). Junto com mes_num forma a chave temporal.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', 
    @level2type=N'COLUMN',@level2name=N'ano';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Nome abreviado do mês em português (Jan, Fev, Mar, etc). Usado para visualização.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', 
    @level2type=N'COLUMN',@level2name=N'mes';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Número do mês (1-12). Usado para ordenação e cálculos temporais.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', 
    @level2type=N'COLUMN',@level2name=N'mes_num';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Rentabilidade mensal do portfolio do cliente EM PERCENTUAL. ATENÇÃO: Valor vem como percentual (ex: 10.5800 = 10.58%). Para uso em cálculos, SEMPRE dividir por 100 para converter em decimal (10.5800/100 = 0.10580).', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', 
    @level2type=N'COLUMN',@level2name=N'portfolio_rentabilidade';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Rentabilidade acumulada no ano EM PERCENTUAL. CUIDADO: Este campo apresenta inconsistências nos dados de origem (valores incorretos). Recomenda-se recalcular dinamicamente. Valor em percentual que deve ser dividido por 100 para uso em cálculos.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', 
    @level2type=N'COLUMN',@level2name=N'acumulado_ano';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Timestamp de quando o registro foi inserido na tabela bronze. Usado para auditoria e controle de carga.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xperformance_rentabilidade_cliente', 
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
- CRÍTICO: Rentabilidades estão em PERCENTUAL, não em decimal
- Sempre dividir por 100 antes de usar em cálculos
- Campo acumulado_ano tem inconsistências - recalcular
- Múltiplas versões do mesmo período por data_relatorio
- Usar MAX(data_relatorio) para versão mais recente

Troubleshooting comum:
1. Rentabilidade absurda: Verificar se não esqueceu de dividir por 100
2. Acumulado incorreto: Recalcular usando produto das rentabilidades mensais
3. Duplicatas: Normal, filtrar por MAX(data_relatorio)

Queries úteis:
-- Rentabilidade mensal mais recente
WITH RentRecente AS (
    SELECT 
        conta_xp_cliente,
        ano,
        mes_num,
        portfolio_rentabilidade / 100.0 as rent_decimal,
        ROW_NUMBER() OVER (PARTITION BY conta_xp_cliente, ano, mes_num 
                          ORDER BY data_relatorio DESC) as rn
    FROM bronze.xperformance_rentabilidade_cliente
    WHERE portfolio_rentabilidade IS NOT NULL
)
SELECT * FROM RentRecente WHERE rn = 1;

-- Cálculo correto de rentabilidade acumulada no ano
WITH RentMensal AS (
    SELECT 
        conta_xp_cliente,
        ano,
        mes_num,
        portfolio_rentabilidade / 100.0 as rent_decimal,
        ROW_NUMBER() OVER (PARTITION BY conta_xp_cliente, ano, mes_num 
                          ORDER BY data_relatorio DESC) as rn
    FROM bronze.xperformance_rentabilidade_cliente
    WHERE portfolio_rentabilidade IS NOT NULL
),
RentAcumulada AS (
    SELECT 
        conta_xp_cliente,
        ano,
        mes_num,
        rent_decimal,
        EXP(SUM(LOG(1 + rent_decimal)) OVER (
            PARTITION BY conta_xp_cliente, ano 
            ORDER BY mes_num 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) - 1 as rent_acum_correta
    FROM RentMensal
    WHERE rn = 1
)
SELECT 
    conta_xp_cliente,
    ano,
    mes_num,
    rent_decimal * 100 as rent_mes_pct,
    rent_acum_correta * 100 as rent_acum_pct
FROM RentAcumulada
ORDER BY conta_xp_cliente, ano, mes_num;

-- Ranking de performance mensal
SELECT 
    ano,
    mes_num,
    conta_xp_cliente,
    portfolio_rentabilidade,
    RANK() OVER (PARTITION BY ano, mes_num 
                ORDER BY portfolio_rentabilidade DESC) as ranking
FROM (
    SELECT 
        conta_xp_cliente,
        ano,
        mes_num,
        portfolio_rentabilidade,
        ROW_NUMBER() OVER (PARTITION BY conta_xp_cliente, ano, mes_num 
                          ORDER BY data_relatorio DESC) as rn
    FROM bronze.xperformance_rentabilidade_cliente
    WHERE portfolio_rentabilidade IS NOT NULL
) t
WHERE rn = 1
ORDER BY ano DESC, mes_num DESC, ranking;

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
PRINT 'Tabela bronze.xperformance_rentabilidade_cliente criada com sucesso!';
GO
