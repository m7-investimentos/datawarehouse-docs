-- ==============================================================================
-- QRY-CAP-001-create_xp_captacao
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: dados@m7investimentos.com.br
-- Revisor: dados@m7investimentos.com.br
-- Tags: [ddl, bronze, xp, captacao, ted, resgates]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.xp_captacao para armazenar dados brutos de 
captação e resgate de recursos vindos da XP. Contém movimentações de entrada 
e saída de recursos dos clientes por assessor AAI.

Casos de uso:
- Staging inicial de movimentações financeiras
- Tracking de canais de captação (TED, PIX, etc)
- Base para métricas de performance comercial Captação líquida

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 1k registros/mês
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
Tabela criada: bronze.xp_captacao

Colunas principais:
- data_ref: Data da captação/resgate
- cod_xp: Código do cliente
- cod_aai: Código do assessor
- tipo_de_captacao: Canal utilizado
- sinal_captacao: 1 para entrada, -1 para saída
- valor_captacao: Valor em R$
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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[xp_captacao]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.xp_captacao já existe. Dropando...';
    DROP TABLE [bronze].[xp_captacao];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[xp_captacao](
	[data_ref] [date] NOT NULL,
	[cod_xp] [int] NOT NULL,
	[cod_aai] [varchar](50) NOT NULL,
	[tipo_de_captacao] [varchar](100) NOT NULL,
	[sinal_captacao] [int] NOT NULL,
	[valor_captacao] [decimal](18, 2) NOT NULL,
	[data_carga] [datetime] NOT NULL
) ON [PRIMARY];
GO

ALTER TABLE [bronze].[xp_captacao] ADD DEFAULT (getdate()) FOR [data_carga];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por data e assessor
CREATE NONCLUSTERED INDEX [IX_bronze_captacao_data_assessor]
ON [bronze].[xp_captacao] ([data_ref], [cod_aai])
INCLUDE ([valor_captacao], [sinal_captacao]);
GO

-- Índice para busca por cliente
CREATE NONCLUSTERED INDEX [IX_bronze_captacao_cliente]
ON [bronze].[xp_captacao] ([cod_xp])
INCLUDE ([data_ref], [valor_captacao]);
GO

-- Índice para análise por tipo de captação
CREATE NONCLUSTERED INDEX [IX_bronze_captacao_tipo]
ON [bronze].[xp_captacao] ([tipo_de_captacao], [data_ref])
INCLUDE ([valor_captacao], [sinal_captacao]);
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Dados brutos de captação vindos da XP. Contém movimentações de entrada de recursos (TED, Previdência, etc) dos clientes por assessor AAI', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_captacao';
GO

-- Documentação das colunas
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data real da captação/resgate. Use para análises temporais. JOIN com silver_dim_calendario por esta data. WHERE data_ref BETWEEN para períodos. GROUP BY YEAR(data_ref), MONTH(data_ref) para agregações mensais. Base para todas métricas temporais.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_captacao', 
    @level2type=N'COLUMN',@level2name=N'data_ref';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Identificador único e permanente do cliente XP. Numérico como string. Use para rastrear cliente ao longo do tempo. JOIN com outras tabelas bronze por este campo. COUNT DISTINCT para clientes únicos. Nunca muda mesmo com transferência de assessor.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_captacao', 
    @level2type=N'COLUMN',@level2name=N'cod_xp';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor responsável. Formato variado: numérico (20471) ou alfanumérico (A22507). JOIN com silver_dim_pessoas.cod_aai para nome. Agrupe por este campo para análises por assessor. NULL indica cliente sem assessor atribuído.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_captacao', 
    @level2type=N'COLUMN',@level2name=N'cod_aai';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Canal/método da movimentação. Valores: ted (transferência bancária tradicional), pix (transferência instantânea), prev (previdência privada), ota (oferta pública/IPO), doc. GROUP BY para análise de canais. WHERE tipo_de_captacao = ''ted'' para TEDs. Minúsculas sempre.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_captacao', 
    @level2type=N'COLUMN',@level2name=N'tipo_de_captacao';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Multiplicador matemático: 1=entrada de recursos, -1=saída/resgate. Use SUM(valor_captacao * sinal_captacao) para captação líquida. WHERE sinal_captacao = 1 para apenas entradas. Facilita cálculos sem CASE WHEN.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_captacao', 
    @level2type=N'COLUMN',@level2name=N'sinal_captacao';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Valor em R$ sempre positivo. Multiplique por sinal_captacao para obter valor real com sinal. SUM para totais, AVG para ticket médio. WHERE valor_captacao >= 100000 para grandes movimentações. Precisão 4 decimais mas geralmente inteiro.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_captacao', 
    @level2type=N'COLUMN',@level2name=N'valor_captacao';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Timestamp de inserção no DW. Use para auditoria e identificar reprocessamentos. WHERE data_carga = MAX(data_carga) para dados mais recentes. Diferente de data_ref que é quando ocorreu a operação. Formato: YYYY-MM-DD HH:MM:SS.', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_captacao', 
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
- Valor sempre positivo, use sinal_captacao para determinar entrada/saída
- Captação líquida = SUM(valor_captacao * sinal_captacao)
- Tipos de captação em minúsculas: ted, pix, prev, ota, doc
- Volume alto: criar particionamento por data_ref se necessário

Troubleshooting comum:
1. Captação negativa: Normal, indica mais resgates que entradas
2. Cliente sem assessor: cod_aai pode ser NULL ou vazio
3. Performance: Usar índices apropriados e estatísticas atualizadas

Queries úteis:
-- Captação líquida por assessor/mês
SELECT 
    cod_aai,
    YEAR(data_ref) as ano,
    MONTH(data_ref) as mes,
    SUM(valor_captacao * sinal_captacao) as captacao_liquida,
    SUM(CASE WHEN sinal_captacao = 1 THEN valor_captacao ELSE 0 END) as entradas,
    SUM(CASE WHEN sinal_captacao = -1 THEN valor_captacao ELSE 0 END) as saidas
FROM bronze.xp_captacao
GROUP BY cod_aai, YEAR(data_ref), MONTH(data_ref)
ORDER BY cod_aai, ano DESC, mes DESC;

-- Análise por canal
SELECT 
    tipo_de_captacao,
    COUNT(*) as qtd_operacoes,
    SUM(valor_captacao * sinal_captacao) as volume_liquido
FROM bronze.xp_captacao
WHERE data_ref >= DATEADD(MONTH, -1, GETDATE())
GROUP BY tipo_de_captacao
ORDER BY volume_liquido DESC;

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
PRINT 'Tabela bronze.xp_captacao criada com sucesso!';
GO
