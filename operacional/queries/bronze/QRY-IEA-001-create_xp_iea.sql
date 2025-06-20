-- ==============================================================================
-- QRY-IEA-001-create_xp_iea
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: dados@m7investimentos.com.br
-- Revisor: dados@m7investimentos.com.br
-- Tags: [ddl, bronze, xp, iea, performance, kpi]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.xp_iea para armazenar dados do IEA (Índice de 
Esforço do Assessor). Contém métricas mensais de performance dos assessores incluindo captação, prospecção e relacionamento.

Casos de uso:
- Armazenamento de KPIs mensais por assessor
- Base para cálculo de comissões e bonificações
- Análise de performance comercial
- Tracking de metas e atingimentos

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 300 registros/ano
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
Tabela criada: bronze.xp_iea

Colunas principais:
- ano_mes: Período de referência (YYYYMM)
- cod_assessor: Código do assessor
- iea_final: Índice consolidado
- captacao_liquida: Captação total do período
- Métricas de prospecção e relacionamento
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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[xp_iea]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.xp_iea já existe. Dropando...';
    DROP TABLE [bronze].[xp_iea];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[xp_iea](
	[ano_mes] [varchar](6) NOT NULL,
	[cod_assessor] [varchar](20) NOT NULL,
	[iea_final] [decimal](9, 6) NOT NULL,
	[captacao_liquida] [decimal](18, 2) NOT NULL,
	[esforco_prospeccao] [decimal](9, 6) NOT NULL,
	[captacao_de_novos_clientes_por_aai] [decimal](16, 2) NOT NULL,
	[atingimento_lead_starts] [decimal](9, 6) NOT NULL,
	[atingimento_habilitacoes] [decimal](9, 6) NOT NULL,
	[atingimento_conversao] [decimal](9, 6) NOT NULL,
	[atingimento_carteiras_simuladas_novos] [decimal](9, 6) NOT NULL,
	[esforco_relacionamento] [decimal](9, 6) NOT NULL,
	[captacao_da_base] [decimal](18, 2) NOT NULL,
	[atingimento_contas_aportarem] [decimal](9, 6) NOT NULL,
	[atingimento_ordens_enviadas] [decimal](9, 6) NOT NULL,
	[atingimento_contas_acessadas_hub] [decimal](9, 6) NOT NULL,
	[data_carga] [datetime] NOT NULL
) ON [PRIMARY];
GO

-- Adicionar defaults
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT ((0)) FOR [iea_final];
GO
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT ((0)) FOR [captacao_liquida];
GO
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT ((0)) FOR [esforco_prospeccao];
GO
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT ((0)) FOR [captacao_de_novos_clientes_por_aai];
GO
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT ((0)) FOR [atingimento_lead_starts];
GO
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT ((0)) FOR [atingimento_habilitacoes];
GO
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT ((0)) FOR [atingimento_conversao];
GO
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT ((0)) FOR [atingimento_carteiras_simuladas_novos];
GO
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT ((0)) FOR [esforco_relacionamento];
GO
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT ((0)) FOR [captacao_da_base];
GO
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT ((0)) FOR [atingimento_contas_aportarem];
GO
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT ((0)) FOR [atingimento_ordens_enviadas];
GO
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT ((0)) FOR [atingimento_contas_acessadas_hub];
GO
ALTER TABLE [bronze].[xp_iea] ADD DEFAULT (getdate()) FOR [data_carga];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice único para chave natural
CREATE UNIQUE CLUSTERED INDEX [PK_bronze_iea]
ON [bronze].[xp_iea] ([ano_mes], [cod_assessor])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, 
      IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, 
      ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];
GO

-- Índice para busca por assessor
CREATE NONCLUSTERED INDEX [IX_bronze_iea_assessor]
ON [bronze].[xp_iea] ([cod_assessor])
INCLUDE ([iea_final], [captacao_liquida]);
GO

-- Índice para análise temporal
CREATE NONCLUSTERED INDEX [IX_bronze_iea_periodo]
ON [bronze].[xp_iea] ([ano_mes])
INCLUDE ([iea_final], [captacao_liquida]);
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela fato com os indicadores do IEA (Índice de Esforço do Assessor), processados a partir da stage_iea.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea';
GO

-- Documentação das colunas
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de referência dos dados (formato ano-mês).', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'ano_mes';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor no CRM.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'cod_assessor';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Índice de Esforço do Assessor (IEA) final.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'iea_final';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Captação líquida total do período.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'captacao_liquida';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Esforço do assessor na etapa de prospecção.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'esforco_prospeccao';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Captação de novos clientes por assessor.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'captacao_de_novos_clientes_por_aai';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Atingimento da meta de lead starts.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'atingimento_lead_starts';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Atingimento da meta de habilitações.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'atingimento_habilitacoes';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Atingimento da meta de conversões.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'atingimento_conversao';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Atingimento de carteiras simuladas para novos clientes.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'atingimento_carteiras_simuladas_novos';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Esforço do assessor na etapa de relacionamento.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'esforco_relacionamento';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Captação da base de clientes ativa.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'captacao_da_base';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Atingimento de contas com aporte.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'atingimento_contas_aportarem';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Atingimento de ordens enviadas.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'atingimento_ordens_enviadas';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Atingimento de contas acessadas no Hub.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
    @level2type=N'COLUMN',@level2name=N'atingimento_contas_acessadas_hub';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data e hora de carga dos dados na tabela.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_iea', 
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
- IEA calculado mensalmente (não há dados diários)
- Valores default 0 para evitar NULLs em métricas
- Chave única: ano_mes + cod_assessor
- Métricas expressas em decimal para permitir percentuais
- Esforço varia de 0 a 1 (0% a 100%)

Troubleshooting comum:
1. Duplicação: Verificar combinação ano_mes/cod_assessor
2. Valores zerados: Normal para assessores inativos no período
3. Performance: Atualizar estatísticas após cargas mensais

Queries úteis:
-- Ranking IEA por mês
SELECT 
    ano_mes,
    cod_assessor,
    iea_final,
    captacao_liquida,
    RANK() OVER (PARTITION BY ano_mes ORDER BY iea_final DESC) as ranking
FROM bronze.xp_iea
WHERE ano_mes = '202501'
ORDER BY iea_final DESC;

-- Evolução temporal por assessor
SELECT 
    cod_assessor,
    ano_mes,
    iea_final,
    captacao_liquida,
    LAG(iea_final) OVER (PARTITION BY cod_assessor ORDER BY ano_mes) as iea_mes_anterior,
    iea_final - LAG(iea_final) OVER (PARTITION BY cod_assessor ORDER BY ano_mes) as variacao
FROM bronze.xp_iea
WHERE cod_assessor = 'SEU_CODIGO'
ORDER BY ano_mes DESC;

-- Médias por componente
SELECT 
    ano_mes,
    AVG(esforco_prospeccao) as media_prospeccao,
    AVG(esforco_relacionamento) as media_relacionamento,
    AVG(iea_final) as media_iea,
    SUM(captacao_liquida) as captacao_total
FROM bronze.xp_iea
GROUP BY ano_mes
ORDER BY ano_mes DESC;

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
PRINT 'Tabela bronze.xp_iea criada com sucesso!';
GO
