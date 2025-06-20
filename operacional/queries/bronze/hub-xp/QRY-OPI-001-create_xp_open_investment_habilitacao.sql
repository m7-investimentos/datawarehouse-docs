-- ==============================================================================
-- QRY-OPI-001-create_xp_open_investment_habilitacao
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: arquitetura.dados@m7investimentos.com.br
-- Revisor: arquitetura.dados@m7investimentos.com.br
-- Tags: [ddl, bronze, xp, open_investment, open_banking, habilitacao]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.xp_open_investment_habilitacao para armazenar 
dados brutos de habilitação de clientes na Open Investment. Registra quando 
clientes autorizaram compartilhamento de dados bancários, permitindo visibilidade 
do patrimônio externo e oportunidades de captação ou dos clientes que enviaram para outras instituicoes.

Casos de uso:
- Tracking de adoção do Open Banking
- Identificação de patrimônio fora da XP
- Cálculo de Share of Wallet (SOW)
- Priorização de abordagem comercial

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 300 habilitações
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
Tabela criada: bronze.xp_open_investment_habilitacao

Colunas principais:
- cod_xp: Código do cliente
- data_permissao: Data da autorização
- sow: Share of Wallet (% na XP)
- auc: Assets Under Custody (valor fora)
- grupo_clientes: Classificação estratégica
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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[xp_open_investment_habilitacao]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.xp_open_investment_habilitacao já existe. Dropando...';
    DROP TABLE [bronze].[xp_open_investment_habilitacao];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[xp_open_investment_habilitacao](
	[ano_mes] [varchar](6) NULL,
	[data_permissao] [date] NULL,
	[cod_xp] [int] NOT NULL,
	[tipo_conta] [varchar](20) NOT NULL,
	[cod_aai] [varchar](20) NOT NULL,
	[status_termo] [varchar](50) NOT NULL,
	[instituicao] [varchar](255) NULL,
	[sow] [decimal](12, 6) NOT NULL,
	[auc] [decimal](16, 2) NOT NULL,
	[auc_atual] [decimal](16, 2) NOT NULL,
	[grupo_clientes] [varchar](50) NULL,
	[sugestao_estrategia] [varchar](50) NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY];
GO

ALTER TABLE [bronze].[xp_open_investment_habilitacao] ADD DEFAULT (CONVERT([date],getdate())) FOR [data_carga];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por cliente
CREATE NONCLUSTERED INDEX [IX_bronze_opi_hab_cliente]
ON [bronze].[xp_open_investment_habilitacao] ([cod_xp])
INCLUDE ([data_permissao], [sow], [auc]);
GO

-- Índice para busca por assessor
CREATE NONCLUSTERED INDEX [IX_bronze_opi_hab_assessor]
ON [bronze].[xp_open_investment_habilitacao] ([cod_aai])
INCLUDE ([cod_xp], [sow], [auc], [grupo_clientes]);
GO

-- Índice para análise por status
CREATE NONCLUSTERED INDEX [IX_bronze_opi_hab_status]
ON [bronze].[xp_open_investment_habilitacao] ([status_termo])
INCLUDE ([data_permissao]);
GO

-- Índice para análise por grupo estratégico
CREATE NONCLUSTERED INDEX [IX_bronze_opi_hab_grupo]
ON [bronze].[xp_open_investment_habilitacao] ([grupo_clientes])
WHERE [grupo_clientes] IS NOT NULL;
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Dados brutos de habilitação de clientes na Open Investment. Registra quando clientes foram habilitados para operar através dessa corretora parceira', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao';
GO

-- Documentação das colunas
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Período no formato AAAAMM (ex: 202501). Use para agregações mensais e séries temporais. WHERE ano_mes = ''202501'' para mês específico. CAST para date: CAST(ano_mes + ''01'' AS DATE). ORDER BY para cronologia.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', 
    @level2type=N'COLUMN',@level2name=N'ano_mes';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data exata que cliente autorizou Open Banking. Marco para início de visibilidade. DATEDIFF para dias desde autorização. WHERE data_permissao IS NOT NULL para apenas habilitados. Base para análise de adoção.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', 
    @level2type=N'COLUMN',@level2name=N'data_permissao';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'ID único do cliente XP. Chave para JOIN com todas bronze_xp_*. Use para enriquecer análises com dados de outras tabelas. COUNT DISTINCT para total de clientes com Open Banking habilitado.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', 
    @level2type=N'COLUMN',@level2name=N'cod_xp';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Natureza jurídica: PESSOA FÍSICA ou PESSOA JURÍDICA. WHERE tipo_conta = ''PESSOA FÍSICA'' para análises PF. GROUP BY para métricas por tipo. PJ geralmente tem maior potencial mas menor volume.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', 
    @level2type=N'COLUMN',@level2name=N'tipo_conta';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor responsável. JOIN com silver_dim_pessoas para dados do assessor. GROUP BY para análise de adoção por carteira. NULL para clientes B2C/Digital. Base para gamificação.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', 
    @level2type=N'COLUMN',@level2name=N'cod_aai';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Status da autorização: Enviado (pendente), Recebido (autorizado). WHERE status_termo = ''Recebido'' para clientes ativos. COUNT por status para funil de conversão. Target para follow-up.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', 
    @level2type=N'COLUMN',@level2name=N'status_termo';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Principal banco/instituição externa do cliente. Indica onde concentra patrimônio fora. GROUP BY para ranking de concorrentes. Base para campanhas direcionadas por banco de origem.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', 
    @level2type=N'COLUMN',@level2name=N'instituicao';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Share of Wallet - percentual na XP. 1.00 = 100% do patrimônio na XP (ideal), 0.50 = 50% na XP. WHERE sow < 0.3 para grandes oportunidades. AVG para SOW médio. Métrica chave para priorização.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', 
    @level2type=N'COLUMN',@level2name=N'sow';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Assets Under Custody - valor total fora da XP em R$. SUM para potencial total de mercado. WHERE auc > 1000000 para high value. Multiplicar por (1-sow) para potencial líquido de captação.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', 
    @level2type=N'COLUMN',@level2name=N'auc';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'AUC atualizado/mais recente em R$. Compare com auc para ver evolução. Pode diferir por movimentações ou atualização de cotações. Use este para análises atuais.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', 
    @level2type=N'COLUMN',@level2name=N'auc_atual';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Classificação estratégica XP: defesa (risco de perder), defesa prioritario (alto valor em risco), expansão (oportunidade crescer). WHERE grupo_clientes = ''expansão'' para foco em crescimento.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', 
    @level2type=N'COLUMN',@level2name=N'grupo_clientes';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Recomendação de abordagem da XP. Ex: Ação Ativo (abordagem agressiva), Manutenção (manter relacionamento). Base para playbook comercial. GROUP BY para volume por estratégia.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', 
    @level2type=N'COLUMN',@level2name=N'sugestao_estrategia';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Timestamp de carga no DW. WHERE data_carga = (SELECT MAX(data_carga)...) para dados mais recentes. Controle de atualização. Geralmente mensal.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_open_investment_habilitacao', 
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
- SOW = Share of Wallet (% do patrimônio total na XP)
- AUC = Assets Under Custody (valor em outras instituições)
- Status "Recebido" indica cliente ativo no Open Banking
- Grupo_clientes é classificação estratégica da XP

Troubleshooting comum:
1. SOW > 1: Erro de cálculo na origem, considerar como 1
2. AUC negativo: Inconsistência, filtrar registros
3. Data_permissao NULL com status Recebido: Dado incompleto

Queries úteis:
-- Potencial de captação por assessor
SELECT 
    cod_aai,
    COUNT(DISTINCT cod_xp) as qtd_clientes_ob,
    SUM(auc * (1 - sow)) as potencial_captacao,
    AVG(sow) as sow_medio,
    SUM(CASE WHEN grupo_clientes = 'expansão' THEN 1 ELSE 0 END) as qtd_expansao
FROM bronze.xp_open_investment_habilitacao
WHERE status_termo = 'Recebido'
    AND data_carga = (SELECT MAX(data_carga) FROM bronze.xp_open_investment_habilitacao)
GROUP BY cod_aai
ORDER BY potencial_captacao DESC;

-- Análise por instituição concorrente
SELECT 
    instituicao,
    COUNT(DISTINCT cod_xp) as qtd_clientes,
    SUM(auc) as volume_total,
    AVG(sow) as sow_medio,
    SUM(auc * (1 - sow)) as potencial
FROM bronze.xp_open_investment_habilitacao
WHERE status_termo = 'Recebido'
    AND instituicao IS NOT NULL
GROUP BY instituicao
ORDER BY volume_total DESC;

-- Evolução de habilitações
SELECT 
    YEAR(data_permissao) as ano,
    MONTH(data_permissao) as mes,
    COUNT(*) as novas_habilitacoes,
    SUM(COUNT(*)) OVER (ORDER BY YEAR(data_permissao), MONTH(data_permissao)) as acumulado
FROM bronze.xp_open_investment_habilitacao
WHERE data_permissao IS NOT NULL
GROUP BY YEAR(data_permissao), MONTH(data_permissao)
ORDER BY ano, mes;

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
PRINT 'Tabela bronze.xp_open_investment_habilitacao criada com sucesso!';
GO
