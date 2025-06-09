-- ==============================================================================
-- QRY-CLI-004-create_silver_fact_cliente_perfil_historico
-- ==============================================================================
-- Tipo: Create Table
-- Versão: 1.0.0
-- Última atualização: 2025-01-06
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [cliente, perfil, histórico, fato, silver]
-- Status: desenvolvimento
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Tabela fato que armazena o histórico de perfil e patrimônio dos 
           clientes ao longo do tempo. Permite análise temporal de evolução
           patrimonial, mudanças de perfil e comportamento de investimento.

Casos de uso:
- Análise de evolução patrimonial dos clientes
- Tracking de mudanças de perfil (suitability)
- Análise de share of wallet entre XP e Open Investment
- Identificação de mudanças de assessor
- Análise de segmentação ao longo do tempo

Frequência de atualização: Mensal
Tempo médio de carga: ~5 minutos
Volume esperado de linhas: ~2.4M registros/ano (200k clientes x 12 meses)
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
Granularidade: Um registro por cliente por mês
Chave primária: (conta_xp_cliente, data_ref)

Tipos de modelo_remuneracao:
- 'Commission Based' (padrão)
- 'Fee Based'

Share of Wallet:
- Percentual do patrimônio total do cliente que está na XP
- Calculado como: patrimonio_xp / (patrimonio_xp + patrimonio_open_investment) * 100
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas utilizadas na carga:
- [bronze].[xp_positivador]: Fonte principal de dados patrimoniais
- [bronze].[xp_rpa_clientes]: Dados de suitability e tipo investidor
- [bronze].[xp_open_investment_habilitacao]: Dados de patrimônio Open Investment
- [silver].[dim_clientes]: Dimensão de clientes (FK)

Pré-requisitos:
- dim_clientes deve existir
- Índices em bronze.xp_positivador: (cod_xp, data_ref)
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ==============================================================================
-- 6. IMPLEMENTAÇÃO DA TABELA
-- ==============================================================================

-- Remover tabela existente se necessário
IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'[silver].[fact_cliente_perfil_historico]'))
    DROP TABLE [silver].[fact_cliente_perfil_historico]
GO

CREATE TABLE [silver].[fact_cliente_perfil_historico](
    [conta_xp_cliente] [int] NOT NULL,
    [data_ref] [date] NOT NULL,
    [patrimonio_declarado] [decimal](18, 2) NULL,
    [patrimonio_xp] [decimal](18, 2) NULL,
    [patrimonio_open_investment] [decimal](18, 2) NULL,
    [share_of_wallet] [decimal](5, 2) NULL,
    [modelo_remuneracao] [varchar](20) NOT NULL DEFAULT 'Commission Based',
    [suitability] [varchar](50) NULL,
    [tipo_investidor] [varchar](100) NULL,
    [segmento_cliente] [varchar](50) NULL,
    [status_cliente] [varchar](20) NOT NULL,
    [faixa_etaria] [varchar](50) NULL,
    [cod_assessor] [varchar](20) NULL,
    [meses_cliente_m7] [int] NULL,
    [safra_cliente_m7] [varchar](7) NULL,
    [data_carga] [datetime] NOT NULL DEFAULT GETDATE(),
 CONSTRAINT [PK_fact_cliente_perfil_historico] PRIMARY KEY CLUSTERED 
(
    [conta_xp_cliente] ASC,
    [data_ref] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- ==============================================================================
-- 7. ÍNDICES E CONSTRAINTS
-- ==============================================================================

-- Índice para consultas por assessor e período
CREATE NONCLUSTERED INDEX [IX_fact_cliente_perfil_historico_assessor_data] 
ON [silver].[fact_cliente_perfil_historico]
(
    [cod_assessor] ASC,
    [data_ref] ASC
)
INCLUDE ([patrimonio_xp], [status_cliente])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

-- Índice para análises de segmentação
CREATE NONCLUSTERED INDEX [IX_fact_cliente_perfil_historico_segmento] 
ON [silver].[fact_cliente_perfil_historico]
(
    [segmento_cliente] ASC,
    [data_ref] ASC,
    [status_cliente] ASC
)
INCLUDE ([patrimonio_xp], [share_of_wallet])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

-- Foreign Key para dim_clientes
ALTER TABLE [silver].[fact_cliente_perfil_historico]  
WITH CHECK ADD CONSTRAINT [FK_fact_cliente_perfil_historico_dim_clientes] 
FOREIGN KEY([conta_xp_cliente])
REFERENCES [silver].[dim_clientes] ([cod_xp])
GO

-- ==============================================================================
-- 8. PROPRIEDADES ESTENDIDAS
-- ==============================================================================

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código único do cliente na XP. FK para dim_clientes.cod_xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'conta_xp_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de referência do perfil (último dia do mês)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio total declarado pelo cliente em outras instituições. Origem: bronze.xp_positivador.aplicacao_financeira_declarada' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'patrimonio_declarado'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio do cliente na XP. Origem: bronze.xp_positivador.net_em_M' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'patrimonio_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio do cliente no Open Investment. Origem: bronze.xp_open_investment_habilitacao.patrimonio' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'patrimonio_open_investment'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Percentual do patrimônio total que está na XP. Calculado: patrimonio_xp / (patrimonio_xp + patrimonio_open_investment) * 100' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'share_of_wallet'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Modelo de remuneração: Commission Based (padrão) ou Fee Based. Origem: bronze.xp_rpa_clientes.fee_based' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'modelo_remuneracao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Perfil de suitability do cliente: Conservador, Moderado, Agressivo, etc. Origem: bronze.xp_rpa_clientes.suitability' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'suitability'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Classificação CVM do investidor: Investidor Regular ou Investidor Qualificado. Origem: bronze.xp_rpa_clientes.tipo_investidor' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'tipo_investidor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Segmento do cliente (Varejo, Private, Corporate, etc). Origem: bronze.xp_rpa_clientes.segmento' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'segmento_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Status do cliente na data: ATIVO, INATIVO ou EVADIU. Baseado na presença no positivador' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'status_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Faixa etária calculada: Menor de 18, 18-25, 26-35, 36-45, 46-55, 56-65, Acima de 65, Pessoa Jurídica. Calculado baseado em data_nascimento' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'faixa_etaria'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do assessor responsável pelo cliente na data. Origem: bronze.xp_positivador.cod_aai' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Quantidade de meses desde que o cliente entrou na M7. Calculado como DATEDIFF entre data de entrada na M7 e data_ref' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'meses_cliente_m7'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Safra (coorte) de entrada do cliente na M7. Formato YYYY-MM representando ano e mês de entrada. Origem: primeira data em que aparece com assessor M7' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'safra_cliente_m7'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data e hora da carga dos dados' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico', @level2type=N'COLUMN',@level2name=N'data_carga'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela fato que armazena o histórico mensal de perfil e patrimônio dos clientes. Permite análise temporal de evolução patrimonial, mudanças de perfil, share of wallet e comportamento de investimento. Granularidade: um registro por cliente por mês.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_cliente_perfil_historico'
GO

-- ==============================================================================
-- 9. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================
/*
-- Verificar evolução patrimonial de um cliente
SELECT 
    data_ref,
    patrimonio_xp,
    patrimonio_open_investment,
    share_of_wallet,
    status_cliente,
    cod_assessor,
    meses_cliente_m7,
    safra_cliente_m7
FROM [silver].[fact_cliente_perfil_historico]
WHERE conta_xp_cliente = 12345
ORDER BY data_ref DESC;

-- Análise de share of wallet por segmento
SELECT 
    segmento_cliente,
    data_ref,
    COUNT(DISTINCT conta_xp_cliente) as qtd_clientes,
    AVG(patrimonio_xp) as patrimonio_xp_medio,
    AVG(share_of_wallet) as share_wallet_medio
FROM [silver].[fact_cliente_perfil_historico]
WHERE status_cliente = 'ATIVO'
  AND data_ref >= DATEADD(MONTH, -12, GETDATE())
GROUP BY segmento_cliente, data_ref
ORDER BY data_ref DESC, segmento_cliente;

-- Análise por safra de clientes
SELECT 
    safra_cliente_m7,
    COUNT(DISTINCT conta_xp_cliente) as qtd_clientes,
    AVG(meses_cliente_m7) as media_meses_relacionamento,
    AVG(patrimonio_xp) as patrimonio_medio,
    AVG(share_of_wallet) as share_wallet_medio
FROM [silver].[fact_cliente_perfil_historico]
WHERE data_ref = EOMONTH(GETDATE(), -1) -- Último mês completo
  AND safra_cliente_m7 IS NOT NULL
GROUP BY safra_cliente_m7
ORDER BY safra_cliente_m7 DESC;

-- Clientes que mudaram de modelo de remuneração
WITH mudancas AS (
    SELECT 
        conta_xp_cliente,
        data_ref,
        modelo_remuneracao,
        LAG(modelo_remuneracao) OVER (PARTITION BY conta_xp_cliente ORDER BY data_ref) as modelo_anterior
    FROM [silver].[fact_cliente_perfil_historico]
)
SELECT 
    conta_xp_cliente,
    data_ref,
    modelo_anterior,
    modelo_remuneracao as modelo_atual
FROM mudancas
WHERE modelo_anterior IS NOT NULL 
  AND modelo_anterior != modelo_remuneracao
ORDER BY data_ref DESC;
*/

-- ==============================================================================
-- 10. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor              | Descrição
--------|------------|--------------------|-----------------------------------------
1.0.0   | 2025-01-06 | Bruno Chiaramonti  | Criação inicial da tabela
*/

-- ==============================================================================
-- 11. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Share of wallet pode ser NULL quando patrimonio_open_investment é NULL
- Modelo de remuneração tem default 'Commission Based'
- Status do cliente é derivado da presença no positivador na data
- Faixa etária é recalculada mensalmente baseada na data_ref
- meses_cliente_m7 e safra_cliente_m7 são NULL para clientes que nunca foram M7
- safra_cliente_m7 é definida na primeira vez que o cliente aparece com assessor M7

Processo de carga:
1. Carregar dados do último dia de cada mês
2. Calcular share_of_wallet quando ambos patrimônios disponíveis
3. Derivar faixa_etaria baseada em idade na data_ref
4. Calcular meses_cliente_m7 baseado na primeira ocorrência com assessor M7
5. Definir safra_cliente_m7 como YYYY-MM da entrada na M7
6. Manter histórico completo (não fazer updates, apenas inserts)

Troubleshooting comum:
1. Duplicatas: Verificar chave (conta_xp_cliente, data_ref)
2. Share of wallet > 100: Verificar dados de patrimônio_open_investment
3. Gaps temporais: Verificar se cliente estava ativo no período

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/