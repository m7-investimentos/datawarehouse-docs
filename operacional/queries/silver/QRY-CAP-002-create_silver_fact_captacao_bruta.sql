-- ==============================================================================
-- QRY-CAP-002-CREATE_SILVER_FACT_CAPTACAO_BRUTA
-- ==============================================================================
-- Tipo: DDL - Create Table
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [ddl, silver, fact_table, captacao, captacao_bruta]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela fato fact_captacao_bruta no schema silver para 
           armazenar dados de captação bruta de clientes, incluindo valores 
           de captação da XP e transferências de entrada.

Casos de uso:
- Armazenar histórico de captações brutas por cliente
- Análise de performance de captação por assessor
- Cálculo de indicadores de captação bruta
- Identificação de origens de captação
- Suporte a relatórios gerenciais e dashboards

Frequência de execução: Única (criação da tabela)
Volume esperado de linhas: ~1.000.000 registros/ano
*/

-- ==============================================================================
-- 2. ESTRUTURA DA TABELA
-- ==============================================================================
/*
Colunas da tabela:

| Coluna                      | Tipo           | Nullable | Descrição                                                    |
|-----------------------------|----------------|----------|--------------------------------------------------------------|
| data_ref                    | DATE           | NOT NULL | Data de referência do registro                               |
| conta_xp_cliente            | INT            | NOT NULL | Código da conta do cliente no sistema XP                     |
| cod_assessor                | VARCHAR(50)    | NOT NULL | Código do assessor no sistema da XP                          |
| origem_captacao             | VARCHAR(100)   | NOT NULL | Origem da captação (TED, PREV, OTA, etc.)                   |
| captacao_bruta_xp           | DECIMAL(18,2)  | NOT NULL | Valor da captação parcial ótica XP                          |
| tipo_transferencia          | VARCHAR(100)   | NOT NULL | Tipo de transferência (nova conta/transferência escritório) |
| captacao_bruta_transferencia| DECIMAL(18,2)  | NOT NULL | Valor da transferência de entrada                           |
| captacao_bruta_total        | DECIMAL(18,2)  | NOT NULL | Captação bruta total (XP + transferência)                   |

Chave primária: Não definida (considerar adicionar)
Índices: A definir baseado em padrões de consulta
*/

-- ==============================================================================
-- 3. DEPENDÊNCIAS
-- ==============================================================================
/*
Schemas necessários:
- silver: Schema onde a tabela será criada
- bronze: Schema de origem dos dados

Tabelas de origem (bronze):
- [bronze].[xp_captacao]: Fonte de dados de captações da XP
  * Campos utilizados: data_ref, cod_xp, cod_aai, tipo_de_captacao, valor_captacao, sinal_captacao
  * Filtro: apenas sinal_captacao = 1 (entradas)
  
- [bronze].[xp_transferencia_clientes]: Fonte de dados de transferências entre assessores
  * Campos utilizados: data_transferencia, cod_xp, cod_aai_origem, cod_aai_destino, status
  * Filtro: apenas status = 'CONCLUIDO'
  
- [bronze].[xp_positivador]: Fonte de dados de patrimônio e cadastro
  * Campos utilizados: cod_xp, data_ref, data_cadastro, net_em_M
  * Usado para classificar tipo de transferência e obter valores

Processo ETL:
- Procedure: [silver].[prc_bronze_to_silver_fact_captacao_bruta]
- Execução: Diária (full load com TRUNCATE/INSERT)

Pré-requisitos:
- Schema silver deve existir
- Usuário deve ter permissão CREATE TABLE no schema silver
- Tabelas bronze devem estar populadas antes da carga
- Database M7Medallion deve ser acessível
*/

-- ==============================================================================
-- 4. SCRIPT DE CRIAÇÃO
-- ==============================================================================

-- Configurações iniciais
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Criação da tabela
CREATE TABLE [silver].[fact_captacao_bruta](
	[data_ref] [date] NOT NULL,
	[conta_xp_cliente] [int] NOT NULL,
	[cod_assessor] [varchar](50) NOT NULL,
	[origem_captacao] [varchar](100) NOT NULL,
	[captacao_bruta_xp] [decimal](18, 2) NOT NULL,
	[tipo_transferencia] [varchar](100) NOT NULL,
	[captacao_bruta_transferencia] [decimal](18, 2) NOT NULL,
	[captacao_bruta_total] [decimal](18, 2) NOT NULL
) ON [PRIMARY]
GO

-- ==============================================================================
-- 5. DOCUMENTAÇÃO DAS COLUNAS (EXTENDED PROPERTIES)
-- ==============================================================================

-- Documentação da coluna data_ref
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'data de referência do registro', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', 
    @level2type=N'COLUMN',@level2name=N'data_ref'
GO

-- Documentação da coluna conta_xp_cliente
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'código da conta do cliente no sistema xp', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', 
    @level2type=N'COLUMN',@level2name=N'conta_xp_cliente'
GO

-- Documentação da coluna cod_assessor
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'código do assessor no sistema da xp', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', 
    @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO

-- Documentação da coluna origem_captacao
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'origem da captação (ted, prev, ota....)', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', 
    @level2type=N'COLUMN',@level2name=N'origem_captacao'
GO

-- Documentação da coluna captacao_bruta_xp
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'valor da captação parcial ótica xp', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', 
    @level2type=N'COLUMN',@level2name=N'captacao_bruta_xp'
GO

-- Documentação da coluna tipo_transferencia
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'tipo de transferência de entrada, se é nova conta ou transferencia de escritorio', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', 
    @level2type=N'COLUMN',@level2name=N'tipo_transferencia'
GO

-- Documentação da coluna captacao_bruta_transferencia
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'valor da transferência de entrada. quanto de patrimonio o cliente tinha quando foi transferido para a M7', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', 
    @level2type=N'COLUMN',@level2name=N'captacao_bruta_transferencia'
GO

-- Documentação da coluna captacao_bruta_total
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'captacao bruta = valor da captação parcial + valor da transferência de entrada', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_captacao_bruta', 
    @level2type=N'COLUMN',@level2name=N'captacao_bruta_total'
GO

-- Documentação da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'tabela silver que contém os dados de captação bruta por cliente com seu respectivo assessor aai. apenas os valores de captação bruta são considerados nessa tabela, os valores de resgate não são considerados. essa tabela é oriunda dos relatórios de operações no hub da xp', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_captacao_bruta'
GO

-- ==============================================================================
-- 6. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-20 | [Nome]         | Criação inicial da tabela

*/

-- ==============================================================================
-- 7. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Tabela não possui chave primária definida - avaliar necessidade
- Considerar criação de índices para otimizar consultas frequentes
- Valores de resgate NÃO são considerados nesta tabela
- Dados originados dos relatórios de operações no Hub da XP
- tipo_transferencia pode ser: 'nova conta', 'transferencia de escritorio' ou 'N/A'

Origens de captação comuns:
- TED: Transferência Eletrônica Disponível
- PREV: Previdência
- OTA: Outras transferências
- Transferência: Transferências entre assessores/escritórios

Recomendações:
1. Adicionar constraint de chave primária composta (data_ref, conta_xp_cliente, origem_captacao)
2. Criar índice não clusterizado em cod_assessor para análises por assessor
3. Criar índice não clusterizado em data_ref para consultas temporais
4. Adicionar constraint CHECK para captacao_bruta_total = captacao_bruta_xp + captacao_bruta_transferencia
5. Adicionar constraint CHECK para valores >= 0 nas colunas de valor

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
