-- ==============================================================================
-- QRY-DIV-001-create_xp_diversificacao
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: dados@m7investimentos.com.br
-- Revisor: dados@m7investimentos.com.br
-- Tags: [ddl, bronze, xp, diversificacao, auc, ativos]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.xp_diversificacao para armazenar dados brutos 
de diversificação dos clientes. Contém a composição detalhada da carteira de 
investimentos por cliente, produto e ativo, obtida dos relatórios de operações 
no Hub XP.

Casos de uso:
- Staging inicial de posições de investimentos
- Análise de diversificação de carteira
- Cálculo de concentração por produto/emissor
- Base para gestão de riscos e compliance

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 300k-500k registros/mês
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
Tabela criada: bronze.xp_diversificacao

Colunas principais:
- data_ref: Data de referência das posições
- cod_xp: Código do cliente
- cod_aai: Código do assessor
- produto/sub_produto: Classificação do investimento
- ativo: Nome do ativo
- net: Valor líquido da posição
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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[xp_diversificacao]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.xp_diversificacao já existe. Dropando...';
    DROP TABLE [bronze].[xp_diversificacao];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[xp_diversificacao](
	[data_ref] [date] NOT NULL,
	[cod_aai] [varchar](50) NULL,
	[cod_xp] [int] NOT NULL,
	[produto] [varchar](50) NULL,
	[sub_produto] [varchar](50) NULL,
	[produto_em_garantia] [varchar](10) NULL,
	[cnpj_fundo] [varchar](14) NULL,
	[ativo] [varchar](255) NULL,
	[emissor] [varchar](255) NULL,
	[data_de_vencimento] [date] NULL,
	[quantidade] [decimal](18, 4) NULL,
	[net] [decimal](18, 4) NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY];
GO

ALTER TABLE [bronze].[xp_diversificacao] ADD DEFAULT (CONVERT([date],getdate())) FOR [data_carga];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por data e cliente
CREATE NONCLUSTERED INDEX [IX_bronze_div_data_cliente]
ON [bronze].[xp_diversificacao] ([data_ref], [cod_xp])
INCLUDE ([produto], [net]);
GO

-- Índice para busca por assessor
CREATE NONCLUSTERED INDEX [IX_bronze_div_assessor]
ON [bronze].[xp_diversificacao] ([cod_aai], [data_ref])
INCLUDE ([cod_xp], [produto], [net]);
GO

-- Índice para análise por produto
CREATE NONCLUSTERED INDEX [IX_bronze_div_produto]
ON [bronze].[xp_diversificacao] ([produto], [sub_produto])
INCLUDE ([net]);
GO

-- Índice para busca por emissor
CREATE NONCLUSTERED INDEX [IX_bronze_div_emissor]
ON [bronze].[xp_diversificacao] ([emissor])
WHERE [emissor] IS NOT NULL;
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela de stage que contém os dados de diversificação dos clientes. Obtida nos relatórios de operações no Hub XP', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao';
GO

-- Documentação das colunas
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de referência dos dados', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao', 
    @level2type=N'COLUMN',@level2name=N'data_ref';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código único que identifica o assessor do cliente', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao', 
    @level2type=N'COLUMN',@level2name=N'cod_aai';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Número da conta do cliente na XP', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao', 
    @level2type=N'COLUMN',@level2name=N'cod_xp';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Produto do ativo', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao', 
    @level2type=N'COLUMN',@level2name=N'produto';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Sub-produto do ativo', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao', 
    @level2type=N'COLUMN',@level2name=N'sub_produto';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Indicador se o produto está em garantia', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao', 
    @level2type=N'COLUMN',@level2name=N'produto_em_garantia';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'CNPJ do fundo', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao', 
    @level2type=N'COLUMN',@level2name=N'cnpj_fundo';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Nome do ativo', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao', 
    @level2type=N'COLUMN',@level2name=N'ativo';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Nome do emissor do ativo', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao', 
    @level2type=N'COLUMN',@level2name=N'emissor';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de vencimento do ativo', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao', 
    @level2type=N'COLUMN',@level2name=N'data_de_vencimento';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Quantidade do ativo', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao', 
    @level2type=N'COLUMN',@level2name=N'quantidade';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Valor líquido do ativo', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao', 
    @level2type=N'COLUMN',@level2name=N'net';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data em que o registro foi carregado', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_diversificacao', 
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
- Snapshot de posições por data_ref (não é histórico completo)
- Um cliente pode ter múltiplos ativos do mesmo produto
- Net representa o valor líquido de mercado
- Produtos em garantia podem estar bloqueados para operações
- CNPJ do fundo NULL para ativos que não são fundos

Troubleshooting comum:
1. Volume alto: Considerar particionamento por data_ref
2. Duplicação: Combinação data_ref + cod_xp + ativo deve ser única
3. Performance: Atualizar estatísticas após grandes cargas

Queries úteis:
-- Diversificação por produto
SELECT 
    cod_xp,
    produto,
    COUNT(DISTINCT ativo) as qtd_ativos,
    SUM(net) as valor_total,
    SUM(net) * 100.0 / SUM(SUM(net)) OVER (PARTITION BY cod_xp) as percentual
FROM bronze.xp_diversificacao
WHERE data_ref = (SELECT MAX(data_ref) FROM bronze.xp_diversificacao)
GROUP BY cod_xp, produto
ORDER BY cod_xp, valor_total DESC;

-- Concentração por emissor
SELECT 
    cod_aai,
    emissor,
    COUNT(DISTINCT cod_xp) as qtd_clientes,
    SUM(net) as exposicao_total
FROM bronze.xp_diversificacao
WHERE data_ref = (SELECT MAX(data_ref) FROM bronze.xp_diversificacao)
    AND emissor IS NOT NULL
GROUP BY cod_aai, emissor
HAVING SUM(net) > 1000000
ORDER BY exposicao_total DESC;

-- Produtos mais comuns
SELECT TOP 20
    produto,
    sub_produto,
    COUNT(DISTINCT cod_xp) as qtd_clientes,
    COUNT(DISTINCT ativo) as qtd_ativos,
    SUM(net) as volume_total
FROM bronze.xp_diversificacao
WHERE data_ref = (SELECT MAX(data_ref) FROM bronze.xp_diversificacao)
GROUP BY produto, sub_produto
ORDER BY volume_total DESC;

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
PRINT 'Tabela bronze.xp_diversificacao criada com sucesso!';
GO
