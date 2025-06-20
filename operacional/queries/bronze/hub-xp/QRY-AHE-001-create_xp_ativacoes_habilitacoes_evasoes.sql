-- ==============================================================================
-- QRY-AHE-001-create_xp_ativacoes_habilitacoes_evasoes
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: dados@m7investimentos.com.br
-- Revisor: dados@m7investimentos.com.br
-- Tags: [ddl, bronze, xp, ativacoes, habilitacoes, evasoes]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.xp_ativacoes_habilitacoes_evasoes para armazenar 
dados brutos de movimentações de clientes (ativações, habilitações e evasões) 
vindos dos relatórios da XP.

Casos de uso:
- Staging inicial de movimentações de clientes
- Histórico de todas as movimentações por assessor
- Base para análise de fluxo de clientes
- Cálculo de métricas de retenção e aquisição de clientes

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 100 registros/mês
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
Tabela criada: bronze.xp_ativacoes_habilitacoes_evasoes

Colunas principais:
- data_ref: Data de referência da movimentação
- cod_xp: Código do cliente na XP
- cod_aai: Código do assessor
- tipo_movimentacao: Tipo (ativação, habilitação, evasão)
- faixa_pl: Faixa de patrimônio líquido
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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[xp_ativacoes_habilitacoes_evasoes]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.xp_ativacoes_habilitacoes_evasoes já existe. Dropando...';
    DROP TABLE [bronze].[xp_ativacoes_habilitacoes_evasoes];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[xp_ativacoes_habilitacoes_evasoes](
	[data_ref] [date] NOT NULL,
	[cod_xp] [varchar](20) NOT NULL,
	[cod_aai] [varchar](20) NOT NULL,
	[faixa_pl] [varchar](50) NULL,
	[tipo_movimentacao] [varchar](20) NOT NULL,
	[data_carga] [datetime] NULL
) ON [PRIMARY];
GO

ALTER TABLE [bronze].[xp_ativacoes_habilitacoes_evasoes] ADD DEFAULT (getdate()) FOR [data_carga];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por data e assessor
CREATE NONCLUSTERED INDEX [IX_bronze_ahe_data_assessor]
ON [bronze].[xp_ativacoes_habilitacoes_evasoes] ([data_ref], [cod_aai])
INCLUDE ([cod_xp], [tipo_movimentacao]);
GO

-- Índice para busca por cliente
CREATE NONCLUSTERED INDEX [IX_bronze_ahe_cliente]
ON [bronze].[xp_ativacoes_habilitacoes_evasoes] ([cod_xp])
INCLUDE ([data_ref], [tipo_movimentacao]);
GO

-- Índice para análise por tipo de movimentação
CREATE NONCLUSTERED INDEX [IX_bronze_ahe_tipo_mov]
ON [bronze].[xp_ativacoes_habilitacoes_evasoes] ([tipo_movimentacao], [data_ref]);
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela bronze para staging de movimentações de clientes (ativações, habilitações e evasões) da XP', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_ativacoes_habilitacoes_evasoes';
GO

-- Documentação das colunas
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de referência da movimentação. Use para análises temporais. JOIN com silver_dim_calendario por esta data. WHERE data_ref BETWEEN para períodos. GROUP BY YEAR(data_ref), MONTH(data_ref) para agregações mensais.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_ativacoes_habilitacoes_evasoes', 
    @level2type=N'COLUMN',@level2name=N'data_ref';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Identificador único do cliente XP. Use para rastrear cliente ao longo do tempo. JOIN com outras tabelas bronze por este campo. COUNT DISTINCT para clientes únicos. Nunca muda mesmo com transferência de assessor.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_ativacoes_habilitacoes_evasoes', 
    @level2type=N'COLUMN',@level2name=N'cod_xp';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor responsável. Formato variado: numérico (20471) ou alfanumérico (A22507). JOIN com silver_dim_pessoas.cod_aai para nome. Agrupe por este campo para análises por assessor.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_ativacoes_habilitacoes_evasoes', 
    @level2type=N'COLUMN',@level2name=N'cod_aai';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Faixa de patrimônio líquido do cliente. Valores típicos: "Até 10k", "10k-100k", "100k-500k", "500k-1M", "Acima de 1M". Use para segmentação de clientes. NULL indica sem classificação.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_ativacoes_habilitacoes_evasoes', 
    @level2type=N'COLUMN',@level2name=N'faixa_pl';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tipo da movimentação. Valores: "ATIVACAO" (cliente novo), "HABILITACAO" (cliente ativou operações), "EVASAO" (cliente saiu). GROUP BY para análise por tipo. Base para métricas de fluxo.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_ativacoes_habilitacoes_evasoes', 
    @level2type=N'COLUMN',@level2name=N'tipo_movimentacao';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Timestamp de inserção no DW. Use para auditoria e identificar reprocessamentos. WHERE data_carga = MAX(data_carga) para dados mais recentes. Formato: YYYY-MM-DD HH:MM:SS.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_ativacoes_habilitacoes_evasoes', 
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
- Esta tabela armazena dados brutos de movimentações de clientes
- Ativações: Clientes novos que entraram na base
- Habilitações: Clientes que ativaram operações
- Evasões: Clientes que saíram da base
- Faixa PL pode ser NULL para clientes sem classificação

Troubleshooting comum:
1. Duplicação de registros: Verificar combinação data_ref/cod_xp/tipo_movimentacao
2. Performance: Criar estatísticas nos índices após grandes cargas
3. Análise de fluxo: Usar tipo_movimentacao para calcular net adds

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
PRINT 'Tabela bronze.xp_ativacoes_habilitacoes_evasoes criada com sucesso!';
GO
