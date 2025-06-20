-- ==============================================================================
-- QRY-OPI-002-create_xp_open_investment_extrato
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: dados@m7investimentos.com.br
-- Revisor: dados@m7investimentos.com.br
-- Tags: [ddl, bronze, xp, open_investment, extrato, portfolio, patrimonio externo]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.xp_open_investment_extrato para armazenar dados 
brutos do extrato de investimentos via Open Investment. Contém o detalhamento 
das posições dos clientes em outras instituições financeiras, permitindo análise 
de patrimônio externo e oportunidades de migração.

Casos de uso:
- Mapeamento de patrimônio em concorrentes
- Análise de portfolio externo por produto
- Identificação de oportunidades de captação
- Inteligência competitiva de mercado

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 1000 posições
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
Tabela criada: bronze.xp_open_investment_extrato

Colunas principais:
- cod_conta: Código do cliente (=cod_xp)
- instituicao_bancaria: Banco/corretora concorrente
- produtos/sub_produtos: Tipo de investimento
- valor_liquido: Valor disponível para resgate
- cod_assessor: Assessor responsável
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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[xp_open_investment_extrato]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.xp_open_investment_extrato já existe. Dropando...';
    DROP TABLE [bronze].[xp_open_investment_extrato];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[xp_open_investment_extrato](
	[segmento] [varchar](20) NULL,
	[cod_assessor] [varchar](20) NULL,
	[cod_conta] [int] NULL,
	[cod_matriz] [smallint] NULL,
	[instituicao_bancaria] [varchar](500) NULL,
	[produtos] [varchar](100) NULL,
	[sub_produtos] [varchar](100) NULL,
	[ativo] [varchar](500) NULL,
	[valor_bruto] [decimal](18, 6) NULL,
	[valor_liquido] [decimal](18, 6) NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY];
GO

ALTER TABLE [bronze].[xp_open_investment_extrato] ADD DEFAULT (CONVERT([date],getdate())) FOR [data_carga];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por cliente
CREATE NONCLUSTERED INDEX [IX_bronze_opi_ext_cliente]
ON [bronze].[xp_open_investment_extrato] ([cod_conta])
INCLUDE ([instituicao_bancaria], [valor_liquido]);
GO

-- Índice para busca por assessor
CREATE NONCLUSTERED INDEX [IX_bronze_opi_ext_assessor]
ON [bronze].[xp_open_investment_extrato] ([cod_assessor])
INCLUDE ([cod_conta], [valor_liquido])
WHERE [cod_assessor] IS NOT NULL;
GO

-- Índice para análise por instituição
CREATE NONCLUSTERED INDEX [IX_bronze_opi_ext_instituicao]
ON [bronze].[xp_open_investment_extrato] ([instituicao_bancaria])
INCLUDE ([valor_liquido], [produtos]);
GO

-- Índice para análise por produto
CREATE NONCLUSTERED INDEX [IX_bronze_opi_ext_produto]
ON [bronze].[xp_open_investment_extrato] ([produtos], [sub_produtos])
INCLUDE ([valor_liquido]);
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Dados brutos do extrato de investimentos da Open Investment (corretora parceira). Contém movimentações e posições de clientes que operam através dessa corretora', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato';
GO

-- Documentação das colunas
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Modelo de atendimento do cliente. B2B=atendido por assessor (maioria), B2C=direto sem assessor, Digital=100% online. WHERE segmento = ''B2B'' para clientes com assessor. Determina estratégia de relacionamento e comissionamento.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', 
    @level2type=N'COLUMN',@level2name=N'segmento';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor com prefixo A (ex: A22507). NULL para B2C/Digital. JOIN com silver_dim_pessoas.cod_aai removendo o A. GROUP BY para análise de patrimônio externo por assessor. Base para cálculo de potencial de migração.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', 
    @level2type=N'COLUMN',@level2name=N'cod_assessor';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código XP do cliente igual cod_xp em outras tabelas. Chave para JOIN com bronze_xp_captacao, bronze_xp_positivador. Use para cruzar patrimônio externo com interno. COUNT DISTINCT para clientes únicos com open banking.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', 
    @level2type=N'COLUMN',@level2name=N'cod_conta';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do escritório/matriz do assessor. JOIN com silver_dim_estruturas para hierarquia. GROUP BY para análise por escritório. Identifica qual unidade M7 é responsável. Importante para metas regionais.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', 
    @level2type=N'COLUMN',@level2name=N'cod_matriz';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Nome completo do banco/instituição concorrente. Valores frequentes: BANCO BRADESCO SA, ITAÚ UNIBANCO S.A., BANCO DO BRASIL SA. GROUP BY para market share por instituição. WHERE LIKE ''%BRADESCO%'' para específico banco. Target para campanhas.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', 
    @level2type=N'COLUMN',@level2name=N'instituicao_bancaria';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Categoria macro do investimento. Valores: Renda Fixa, Fundos, Renda Variável, Previdência. GROUP BY para distribuição de portfolio externo. WHERE produtos = ''Renda Fixa'' para conservadores. Base para estratégia de abordagem.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', 
    @level2type=N'COLUMN',@level2name=N'produtos';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Subcategoria quando aplicável. Ex: Bancário (CDB), DI (fundos), Multimercado. Pode ser NULL. Combine com produtos para análise detalhada. Indica sofisticação do investidor. Útil para ofertas direcionadas.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', 
    @level2type=N'COLUMN',@level2name=N'sub_produtos';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Nome completo/descrição do investimento. Para CDB inclui vencimento (ex: CDB 2025-12-31), para fundos nome completo. Texto livre muito variado. Use LIKE para buscas. Nível mais granular de detalhe disponível.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', 
    @level2type=N'COLUMN',@level2name=N'ativo';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Valor total do investimento antes de impostos em R$. SUM por cliente para patrimônio bruto externo total. Compare com valor_líquido para estimar IR. Base para cálculo de potencial máximo de captação.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', 
    @level2type=N'COLUMN',@level2name=N'valor_bruto';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Valor após IR disponível para resgate em R$. Use este para potencial real de migração. (valor_bruto - valor_líquido) = imposto estimado. SUM por cliente e instituição para priorização de abordagem.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', 
    @level2type=N'COLUMN',@level2name=N'valor_liquido';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de captura via Open Banking. Indica frescor dos dados. WHERE data_carga >= DATEADD(day,-30,GETDATE()) para dados recentes. MAX(data_carga) para última posição. Atualização não é diária.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_extrato', 
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
- cod_conta = cod_xp para JOIN com outras tabelas
- Valores em decimal(18,6) para precisão em grandes volumes
- Um cliente pode ter múltiplas linhas (uma por ativo)
- Instituição bancária pode ter variações de nome

Troubleshooting comum:
1. Duplicação: Normal, uma linha por ativo do cliente
2. Valor negativo: Possível em alguns derivativos
3. Assessor NULL: Cliente B2C ou Digital

Queries úteis:
-- Patrimônio externo por assessor
SELECT 
    cod_assessor,
    COUNT(DISTINCT cod_conta) as qtd_clientes,
    COUNT(*) as qtd_posicoes,
    SUM(valor_liquido) as patrimonio_externo_total,
    AVG(valor_liquido) as ticket_medio_posicao
FROM bronze.xp_open_investment_extrato
WHERE cod_assessor IS NOT NULL
    AND data_carga = (SELECT MAX(data_carga) FROM bronze.xp_open_investment_extrato)
GROUP BY cod_assessor
ORDER BY patrimonio_externo_total DESC;

-- Market share por instituição
SELECT 
    instituicao_bancaria,
    COUNT(DISTINCT cod_conta) as qtd_clientes,
    SUM(valor_liquido) as volume_total,
    CAST(SUM(valor_liquido) * 100.0 / SUM(SUM(valor_liquido)) OVER() AS DECIMAL(5,2)) as market_share
FROM bronze.xp_open_investment_extrato
WHERE data_carga = (SELECT MAX(data_carga) FROM bronze.xp_open_investment_extrato)
GROUP BY instituicao_bancaria
ORDER BY volume_total DESC;

-- Análise de produtos concorrentes
SELECT 
    produtos,
    sub_produtos,
    COUNT(DISTINCT cod_conta) as qtd_clientes,
    COUNT(*) as qtd_posicoes,
    SUM(valor_liquido) as volume_total,
    AVG(valor_liquido) as ticket_medio
FROM bronze.xp_open_investment_extrato
WHERE data_carga = (SELECT MAX(data_carga) FROM bronze.xp_open_investment_extrato)
GROUP BY produtos, sub_produtos
ORDER BY volume_total DESC;

-- Clientes com maior potencial por assessor
SELECT TOP 100
    cod_assessor,
    cod_conta,
    SUM(valor_liquido) as patrimonio_externo,
    COUNT(DISTINCT instituicao_bancaria) as qtd_instituicoes,
    COUNT(DISTINCT produtos) as qtd_produtos
FROM bronze.xp_open_investment_extrato
WHERE cod_assessor IS NOT NULL
    AND data_carga = (SELECT MAX(data_carga) FROM bronze.xp_open_investment_extrato)
GROUP BY cod_assessor, cod_conta
ORDER BY patrimonio_externo DESC;

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
PRINT 'Tabela bronze.xp_open_investment_extrato criada com sucesso!';
GO
