-- ==============================================================================
-- QRY-CLI-001-create_xp_rpa_clientes
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: dados@m7investimentos.com.br
-- Revisor: dados@m7investimentos.com.br
-- Tags: [ddl, bronze, xp, rpa, clientes, carteira]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.xp_rpa_clientes para armazenar dados brutos de 
clientes vindos do RPA (Relatório de Performance de Assessor) da XP. Contém 
a base completa de clientes ativos por assessor com informações de perfil e 
patrimônio.

Casos de uso:
- Staging inicial da base de clientes
- Análise de carteira por assessor
- Segmentação de clientes por perfil
- Base para cálculos de patrimônio e fee

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 40 linhas por mês
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
Tabela criada: bronze.xp_rpa_clientes

Colunas principais:
- cod_xp: Código único do cliente
- nome_cliente: Nome completo ou razão social
- CPF/CNPJ: Documento do cliente para cruzamento de bases
- cod_aai: Código do assessor responsável
- segmento: Classificação do cliente
- suitability: Perfil de investidor
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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[xp_rpa_clientes]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.xp_rpa_clientes já existe. Dropando...';
    DROP TABLE [bronze].[xp_rpa_clientes];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[xp_rpa_clientes](
	[cod_xp] [int] NULL,
	[nome_cliente] [nvarchar](107) NULL,
	[telefone_cliente] [nvarchar](50) NULL,
	[email_cliente] [nvarchar](54) NULL,
	[patrimonio] [decimal](18, 2) NULL,
	[elegibilidade_cartao] [nvarchar](50) NULL,
	[cpf_cnpj] [nvarchar](50) NULL,
	[suitability] [nvarchar](50) NULL,
	[fee_based] [nvarchar](50) NULL,
	[segmento] [nvarchar](50) NULL,
	[tipo_investidor] [nvarchar](50) NULL,
	[cod_aai] [nvarchar](50) NULL,
	[status_conta_digital] [nvarchar](50) NULL,
	[produto] [nvarchar](50) NULL,
	[data_carga] [datetime2](7) NOT NULL
) ON [PRIMARY];
GO

ALTER TABLE [bronze].[xp_rpa_clientes] ADD DEFAULT (getdate()) FOR [data_carga];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por cliente
CREATE NONCLUSTERED INDEX [IX_bronze_rpa_clientes_cod_xp]
ON [bronze].[xp_rpa_clientes] ([cod_xp])
INCLUDE ([nome_cliente], [patrimonio], [cod_aai]);
GO

-- Índice para busca por assessor
CREATE NONCLUSTERED INDEX [IX_bronze_rpa_clientes_assessor]
ON [bronze].[xp_rpa_clientes] ([cod_aai])
INCLUDE ([cod_xp], [patrimonio], [segmento]);
GO

-- Índice para análise por segmento
CREATE NONCLUSTERED INDEX [IX_bronze_rpa_clientes_segmento]
ON [bronze].[xp_rpa_clientes] ([segmento])
INCLUDE ([cod_xp], [patrimonio]);
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Dados brutos de clientes vindos do RPA (Relatório de Performance de Assessor) da XP. Contém base de clientes ativos por assessor', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes';
GO

-- Documentação das colunas
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID único do cliente. Chave para joins com outras tabelas. COUNT DISTINCT por assessor para tamanho de carteira. Base para análises individuais de cliente.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'cod_xp';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Nome completo ou razão social. Use com cuidado - dados pessoais. WHERE nome_cliente LIKE para buscas específicas. ORDER BY para listagens alfabéticas.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'nome_cliente';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Telefone de contato do cliente', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'telefone_cliente';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Email de contato do cliente', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'email_cliente';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Valor do patrimônio do cliente na XP', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'patrimonio';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Indicador de elegibilidade para cartão de crédito XP', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'elegibilidade_cartao';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'CPF ou CNPJ mascarado/anonimizado. LEN() = 14 para CNPJ, 11 para CPF. Não use para joins. Permite identificar PF vs PJ. Dados sensíveis protegidos.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'cpf_cnpj';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Perfil de investidor do cliente conforme questionário suitability', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'suitability';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Indicador se o cliente é fee based (taxa fixa)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'fee_based';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Segmento de classificação do cliente (Varejo, Private, etc)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'segmento';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tipo de investidor (Pessoa Física ou Pessoa Jurídica)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'tipo_investidor';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor dono da carteira. JOIN com silver_dim_pessoas. GROUP BY para análise por assessor. Base para distribuição de clientes e análise de concentração.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'cod_aai';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Status da conta digital do cliente na XP', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'status_conta_digital';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Principal produto investido pelo cliente', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
    @level2type=N'COLUMN',@level2name=N'produto';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data e hora em que os dados foram carregados no sistema', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_rpa_clientes', 
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
- Snapshot da base de clientes ativos (não é histórico)
- CPF/CNPJ mascarado por segurança
- Patrimônio pode estar NULL para clientes novos
- Telefone e email são dados sensíveis - usar com cuidado
- Segmentos típicos: Varejo, Private, Ultra High

Troubleshooting comum:
1. Cliente sem assessor: cod_aai NULL indica cliente órfão
2. Patrimônio zerado: Normal para clientes recém cadastrados
3. Duplicação: Usar MAX(data_carga) para snapshot mais recente

Queries úteis:
-- Distribuição de patrimônio por assessor
SELECT 
    cod_aai,
    COUNT(DISTINCT cod_xp) as qtd_clientes,
    SUM(patrimonio) as patrimonio_total,
    AVG(patrimonio) as ticket_medio
FROM bronze.xp_rpa_clientes
WHERE patrimonio > 0
GROUP BY cod_aai
ORDER BY patrimonio_total DESC;

-- Análise por segmento
SELECT 
    segmento,
    COUNT(*) as qtd_clientes,
    SUM(patrimonio) as patrimonio_total,
    AVG(patrimonio) as patrimonio_medio
FROM bronze.xp_rpa_clientes
GROUP BY segmento
ORDER BY patrimonio_total DESC;

-- Clientes fee based
SELECT 
    fee_based,
    COUNT(*) as qtd,
    SUM(patrimonio) as patrimonio
FROM bronze.xp_rpa_clientes
GROUP BY fee_based;

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
PRINT 'Tabela bronze.xp_rpa_clientes criada com sucesso!';
GO
