-- ==============================================================================
-- QRY-CAP-005-create_gold_captacao_liquida_cliente
-- ==============================================================================
-- Tipo: Create Table
-- Versão: 1.0.0
-- Última atualização: 2025-01-16
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [captação, cliente, mensal, tabela, gold]
-- Status: desenvolvimento
-- Banco de Dados: SQL Server
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Tabela materializada que armazena dados consolidados de captação líquida
           por cliente, com métricas de comportamento e tendências. Esta tabela
           é atualizada diariamente via procedure para otimizar performance de
           consultas e análises de relacionamento.

Casos de uso:
- Análise histórica de comportamento individual de clientes
- Dashboards de relacionamento com alta performance
- Base para modelos preditivos de churn e resgate
- Segmentação avançada de clientes por padrão de investimento
- Relatórios executivos de carteira por assessor

Frequência de atualização: Diária (via procedure)
Tempo médio de carga: ~2-3 minutos
Volume esperado de linhas: ~500.000-1.000.000 registros/ano
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Não aplicável - tabela de armazenamento
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Estrutura idêntica à view [gold].[view_captacao_liquida_cliente]
com adição de metadados de controle:
- data_carga: Timestamp da última atualização
- hash_registro: Hash para controle de mudanças (calculado pela procedure)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Views utilizadas:
- [gold].[view_captacao_liquida_cliente]: View fonte dos dados

Procedures relacionadas:
- [dbo].[prc_gold_performance_to_table_captacao_liquida_cliente]: Atualiza a tabela

Pré-requisitos:
- View deve estar criada e funcional
- Permissões de CREATE TABLE no schema gold
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- Tabela com compressão de página para otimizar armazenamento
-- Índices clustered e non-clustered para performance de consulta
-- Particionamento por data_ref pode ser considerado para volumes maiores

-- ==============================================================================
-- 6. IMPLEMENTAÇÃO DA TABELA
-- ==============================================================================

-- Remover tabela existente se necessário
IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'[gold].[captacao_liquida_cliente]'))
    DROP TABLE [gold].[captacao_liquida_cliente]
GO

CREATE TABLE [gold].[captacao_liquida_cliente](
    -- Dimensões temporais
    [data_ref] [date] NOT NULL,
    [ano] [int] NOT NULL,
    [mes] [int] NOT NULL,
    [nome_mes] [varchar](20) NULL,
    [trimestre] [char](2) NULL,
    
    -- Dimensão cliente
    [conta_xp_cliente] [int] NOT NULL,
    [nome_cliente] [varchar](200) NULL,
    [tipo_cliente] [varchar](12) NULL,
    [grupo_cliente] [varchar](100) NULL,
    [segmento_cliente] [varchar](50) NULL,
    [status_cliente] [varchar](50) NULL,
    [faixa_etaria] [varchar](50) NULL,
    [codigo_cliente_crm] [varchar](100) NULL,
    
    -- Dimensão assessor
    [cod_assessor] [varchar](50) NOT NULL,
    [nome_assessor] [varchar](200) NULL,
    [assessor_nivel] [varchar](50) NULL,
    [assessor_status] [varchar](50) NULL,
    [codigo_assessor_crm] [varchar](20) NULL,
    [nome_estrutura] [varchar](100) NULL,
    
    -- Métricas de Captação Bruta
    [captacao_bruta_xp] [decimal](18, 2) NULL,
    [captacao_bruta_transferencia] [decimal](18, 2) NULL,
    [captacao_bruta_total] [decimal](18, 2) NULL,
    
    -- Métricas de Resgate Bruto
    [resgate_bruto_xp] [decimal](18, 2) NULL,
    [resgate_bruto_transferencia] [decimal](18, 2) NULL,
    [resgate_bruto_total] [decimal](18, 2) NULL,
    
    -- Métricas de Captação Líquida
    [captacao_liquida_xp] [decimal](18, 2) NULL,
    [captacao_liquida_transferencia] [decimal](18, 2) NULL,
    [captacao_liquida_total] [decimal](18, 2) NULL,
    
    -- Métricas de Operações
    [qtd_operacoes_aporte] [int] NULL,
    [qtd_operacoes_resgate] [int] NULL,
    [ticket_medio_aporte] [decimal](18, 2) NULL,
    [ticket_medio_resgate] [decimal](18, 2) NULL,
    
    -- Métricas de Relacionamento
    [meses_como_cliente] [int] NULL,
    [primeira_captacao] [date] NULL,
    [ultima_captacao] [date] NULL,
    [ultimo_resgate] [date] NULL,
    
    -- Metadados de controle
    [data_carga] [datetime] NOT NULL DEFAULT GETDATE(),
    [hash_registro] [varbinary](32) NULL,
    
 CONSTRAINT [PK_captacao_liquida_cliente] PRIMARY KEY CLUSTERED 
(
    [data_ref] ASC,
    [conta_xp_cliente] ASC,
    [cod_assessor] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
WITH (DATA_COMPRESSION = PAGE)
GO

-- ==============================================================================
-- 7. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para consultas por assessor e período
CREATE NONCLUSTERED INDEX [IX_captacao_liquida_cliente_assessor_periodo] 
ON [gold].[captacao_liquida_cliente]
(
    [cod_assessor] ASC,
    [ano] ASC,
    [mes] ASC
)
INCLUDE ([nome_cliente], [captacao_liquida_total], [meses_como_cliente])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

-- Índice para análise de comportamento de clientes
CREATE NONCLUSTERED INDEX [IX_captacao_liquida_cliente_comportamento] 
ON [gold].[captacao_liquida_cliente]
(
    [captacao_liquida_total] DESC,
    [ano] ASC,
    [mes] ASC
)
INCLUDE ([conta_xp_cliente], [nome_cliente], [cod_assessor], [qtd_operacoes_aporte], [qtd_operacoes_resgate])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

-- Índice para segmentação de clientes
CREATE NONCLUSTERED INDEX [IX_captacao_liquida_cliente_segmentacao] 
ON [gold].[captacao_liquida_cliente]
(
    [tipo_cliente] ASC,
    [grupo_cliente] ASC,
    [ano] ASC,
    [mes] ASC
)
INCLUDE ([captacao_liquida_total], [ticket_medio_aporte])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

-- Índice para análise de churn (clientes em risco)
CREATE NONCLUSTERED INDEX [IX_captacao_liquida_cliente_churn] 
ON [gold].[captacao_liquida_cliente]
(
    [ultimo_resgate] DESC,
    [ultima_captacao] DESC
)
WHERE ([captacao_liquida_total] < 0)
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

-- ==============================================================================
-- 8. PROPRIEDADES ESTENDIDAS
-- ==============================================================================

-- Dimensões temporais
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência (último dia do mês)' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano de referência' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'mês de referência' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome do mês por extenso' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'nome_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'trimestre do ano' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'trimestre'
GO

-- Dimensão cliente
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código da conta do cliente' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'conta_xp_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome do cliente' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'nome_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tipo de cliente (PF/PJ)' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'tipo_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'grupo econômico do cliente' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'grupo_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'segmentação do cliente' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'segmento_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'status do cliente (ativo/inativo)' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'status_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'faixa etária do cliente' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'faixa_etaria'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código CRM do cliente' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'codigo_cliente_crm'
GO

-- Dimensão assessor
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código do assessor responsável' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome do assessor' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'nome_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nível do assessor' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'assessor_nivel'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'status do assessor (ativo/inativo)' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'assessor_status'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código CRM do assessor' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'codigo_assessor_crm'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'estrutura do assessor' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'nome_estrutura'
GO

-- Métricas de Captação
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação bruta via XP' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'captacao_bruta_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação bruta via transferência' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'captacao_bruta_transferencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'total de captação bruta' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'captacao_bruta_total'
GO

-- Métricas de Resgate
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgates via XP' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'resgate_bruto_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgates via transferência' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'resgate_bruto_transferencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'total de resgates' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'resgate_bruto_total'
GO

-- Métricas de Captação Líquida
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida XP' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'captacao_liquida_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida transferência' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'captacao_liquida_transferencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida total' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'captacao_liquida_total'
GO

-- Métricas de Operações
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'número de operações de aporte no mês' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'qtd_operacoes_aporte'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'número de operações de resgate no mês' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'qtd_operacoes_resgate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor médio por operação de aporte' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'ticket_medio_aporte'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor médio por operação de resgate' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'ticket_medio_resgate'
GO

-- Métricas de Relacionamento
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tempo de relacionamento em meses' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'meses_como_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data da primeira captação' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'primeira_captacao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data da última captação' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'ultima_captacao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data do último resgate' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'ultimo_resgate'
GO

-- Metadados
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data e hora da última carga' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'hash do registro para controle de mudanças' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente', @level2type=N'COLUMN',@level2name=N'hash_registro'
GO

-- Tabela
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela que armazena dados consolidados de captação líquida por cliente com métricas de comportamento e tendências' , @level0type=N'SCHEMA',@level0name=N'gold', @level1type=N'TABLE',@level1name=N'captacao_liquida_cliente'
GO

-- GRANT SELECT ON [gold].[captacao_liquida_cliente] TO [role_gold_read]
-- GO

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor              | Descrição
--------|------------|--------------------|-----------------------------------------
1.0.0   | 2025-01-16 | Bruno Chiaramonti  | Criação inicial da tabela
*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Tabela atualizada diariamente via procedure
- Compressão de página ativada para otimizar armazenamento
- Hash calculado pela procedure para controle de mudanças incrementais
- Índices otimizados para cenários mais comuns de análise
- Volume significativamente maior que a tabela de assessores

Troubleshooting comum:
1. Espaço em disco: Monitorar crescimento mensal (~50-100MB/mês)
2. Performance de carga: Se > 5 minutos, considerar particionamento
3. Dados duplicados: Verificar chave primária (data_ref, conta_xp_cliente, cod_assessor)
4. Memória: Ajustar batch size na procedure se necessário

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/