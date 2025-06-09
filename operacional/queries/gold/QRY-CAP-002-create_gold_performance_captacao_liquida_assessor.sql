-- ==============================================================================
-- QRY-CAP-002-create_gold_performance_captacao_liquida_assessor
-- ==============================================================================
-- Tipo: Create Table
-- Versão: 1.0.0
-- Última atualização: 2025-01-06
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [captação, assessor, mensal, tabela, gold]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: gold_performance
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Tabela materializada que armazena dados consolidados de captação líquida
           por assessor, com métricas de performance e análise comportamental de
           clientes. Esta tabela é atualizada diariamente via procedure.

Casos de uso:
- Análise histórica de performance de assessores
- Dashboards executivos com alta performance
- Relatórios de tendências e comparativos
- Base para modelos preditivos de captação

Frequência de atualização: Diária (via procedure)
Tempo médio de carga: ~30 segundos
Volume esperado de linhas: ~10.000 registros/ano
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
Estrutura idêntica à view [gold_performance].[captacao_liquida_assessor]
com adição de metadados de controle:
- data_carga: Timestamp da última atualização
- hash_registro: Hash para controle de mudanças (calculado pela procedure)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Views utilizadas:
- [gold_performance].[view_captacao_liquida_assessor]: View fonte dos dados

Procedures relacionadas:
- [dbo].[prc_gold_performance_to_table_captacao_liquida_assessor]: Atualiza a tabela

Pré-requisitos:
- View deve estar criada e funcional
- Permissões de CREATE TABLE no schema gold_performance
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- Tabela com compressão de página para otimizar armazenamento
-- Índices clustered e non-clustered para performance de consulta

-- ==============================================================================
-- 6. IMPLEMENTAÇÃO DA TABELA
-- ==============================================================================

-- Remover tabela existente se necessário
IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'[gold_performance].[captacao_liquida_assessor]'))
    DROP TABLE [gold_performance].[captacao_liquida_assessor]
GO

CREATE TABLE [gold_performance].[captacao_liquida_assessor](
    -- Dimensões temporais
    [data_ref] [date] NOT NULL,
    [ano] [int] NOT NULL,
    [mes] [int] NOT NULL,
    [nome_mes] [varchar](20) NULL,
    [trimestre] [char](2) NULL,
    
    -- Dimensão assessor
    [cod_assessor] [varchar](50) NOT NULL,
    [nome_assessor] [varchar](200) NULL,
    [assessor_nivel] [varchar](50) NULL,
    [codigo_assessor_crm] [varchar](20) NULL,
    [assessor_status] [varchar](50) NULL,
    
    -- Dimensão estrutura
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
    
    -- Métricas de Clientes e Tickets
    [qtd_clientes_aportando] [int] NULL,
    [qtd_clientes_resgatando] [int] NULL,
    [ticket_medio_aporte] [decimal](18, 2) NULL,
    [ticket_medio_resgate] [decimal](18, 2) NULL,
    
    -- Análise de comportamento de clientes
    [qtd_clientes_apenas_aportando] [int] NULL,
    [qtd_clientes_apenas_resgatando] [int] NULL,
    [qtd_clientes_aporte_e_resgate] [int] NULL,
    
    -- Metadados de controle
    [data_carga] [datetime] NOT NULL DEFAULT GETDATE(),
    [hash_registro] [varbinary](32) NULL,
    
 CONSTRAINT [PK_captacao_liquida_assessor] PRIMARY KEY CLUSTERED 
(
    [data_ref] ASC,
    [cod_assessor] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
WITH (DATA_COMPRESSION = PAGE)
GO

-- ==============================================================================
-- 7. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para consultas por período
CREATE NONCLUSTERED INDEX [IX_captacao_liquida_assessor_periodo] 
ON [gold_performance].[captacao_liquida_assessor]
(
    [ano] ASC,
    [mes] ASC
)
INCLUDE ([cod_assessor], [nome_assessor], [captacao_liquida_total])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

-- Índice para consultas por estrutura
CREATE NONCLUSTERED INDEX [IX_captacao_liquida_assessor_estrutura] 
ON [gold_performance].[captacao_liquida_assessor]
(
    [nome_estrutura] ASC,
    [ano] ASC,
    [mes] ASC
)
INCLUDE ([captacao_liquida_total], [qtd_clientes_aportando])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

-- Índice para ranking de performance
CREATE NONCLUSTERED INDEX [IX_captacao_liquida_assessor_performance] 
ON [gold_performance].[captacao_liquida_assessor]
(
    [captacao_liquida_total] DESC,
    [ano] ASC,
    [mes] ASC
)
INCLUDE ([cod_assessor], [nome_assessor], [nome_estrutura])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

-- ==============================================================================
-- 8. PROPRIEDADES ESTENDIDAS
-- ==============================================================================

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência (último dia do mês)' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ano de referência' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'ano'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'mês de referência' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome do mês por extenso' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'nome_mes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'trimestre do ano' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'trimestre'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código do assessor' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome completo do assessor' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'nome_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nível do assessor' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'assessor_nivel'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código CRM do assessor' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'codigo_assessor_crm'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'status do assessor (ativo/inativo)' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'assessor_status'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome da estrutura do assessor' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'nome_estrutura'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação bruta via XP' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'captacao_bruta_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação bruta via transferência' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'captacao_bruta_transferencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'total de captação bruta' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'captacao_bruta_total'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgates via XP' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'resgate_bruto_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgates via transferência' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'resgate_bruto_transferencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'total de resgates' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'resgate_bruto_total'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida XP' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'captacao_liquida_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida transferência' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'captacao_liquida_transferencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'captação líquida total' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'captacao_liquida_total'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'quantidade de clientes únicos que fizeram aportes' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'qtd_clientes_aportando'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'quantidade de clientes únicos que fizeram resgates' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'qtd_clientes_resgatando'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor médio por operação de aporte' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'ticket_medio_aporte'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor médio por operação de resgate' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'ticket_medio_resgate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'clientes que só aportaram' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'qtd_clientes_apenas_aportando'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'clientes que só resgataram' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'qtd_clientes_apenas_resgatando'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'clientes que fizeram ambos' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'qtd_clientes_aporte_e_resgate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data e hora da última carga' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'hash do registro para controle de mudanças' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor', @level2type=N'COLUMN',@level2name=N'hash_registro'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela que armazena dados consolidados de captação líquida por assessor com métricas de performance e análise comportamental de clientes' , @level0type=N'SCHEMA',@level0name=N'gold_performance', @level1type=N'TABLE',@level1name=N'captacao_liquida_assessor'
GO
-- GRANT SELECT ON [gold_performance].[captacao_liquida_assessor] TO [role_gold_read]
-- GO

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor              | Descrição
--------|------------|--------------------|-----------------------------------------
1.0.0   | 2025-01-06 | Bruno Chiaramonti  | Criação inicial da tabela
*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Tabela atualizada diariamente via procedure
- Compressão de página ativada para otimizar armazenamento
- Hash calculado pela procedure para controle de mudanças incrementais
- Índices otimizados para consultas mais comuns

Troubleshooting comum:
1. Espaço em disco: Monitorar crescimento mensal (~1MB/mês)
2. Performance de carga: Se > 1 minuto, verificar estatísticas das tabelas fonte
3. Dados duplicados: Verificar chave primária (data_ref, cod_assessor)

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/