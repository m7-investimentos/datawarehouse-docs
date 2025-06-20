-- ==============================================================================
-- QRY-CLI-002-CREATE_SILVER_DIM_CLIENTES
-- ==============================================================================
-- Tipo: DDL - Create Table
-- Versão: 2.0.0
-- Última atualização: 2025-01-13
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [ddl, silver, dimension_table, clientes, cadastro]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela dimensão dim_clientes no schema silver.
           Contém dados cadastrais básicos e estáticos de TODOS os clientes
           históricos (cod_xp distintos). Dados variáveis no tempo foram
           movidos para fact_cliente_perfil_historico.

Casos de uso:
- Master data de clientes para joins em análises
- Identificação única de clientes
- Dados cadastrais básicos e estáticos
- Base para relatórios e dashboards
- Referência para outras tabelas do DW

Frequência de atualização: Diária
Tempo médio de carga: ~2 minutos
Volume esperado de linhas: ~200.000 registros
*/

-- ==============================================================================
-- 2. ESTRUTURA DA TABELA
-- ==============================================================================
/*
Colunas da tabela:

| Coluna              | Tipo           | Nullable | Descrição                                          |
|---------------------|----------------|----------|----------------------------------------------------|
| cod_xp              | INT            | NOT NULL | Código único do cliente na XP (PK)                |
| cpf                 | VARCHAR(11)    | NULL     | CPF para pessoa física (11 dígitos)               |
| cnpj                | VARCHAR(14)    | NULL     | CNPJ para pessoa jurídica (14 dígitos)           |
| nome_cliente        | VARCHAR(300)   | NULL     | Nome completo ou razão social                     |
| telefone_cliente    | VARCHAR(50)    | NULL     | Telefone principal com DDD                         |
| email_cliente       | VARCHAR(200)   | NULL     | E-mail principal                                   |
| data_nascimento     | DATE           | NULL     | Data de nascimento (PF) ou NULL (PJ)               |
| sexo                | CHAR(1)        | NULL     | Sexo: M/F ou NULL                                  |
| profissao           | VARCHAR(100)   | NULL     | Profissão declarada                               |
| data_cadastro       | DATE           | NULL     | Data de cadastro na XP                            |
| grupo_cliente       | VARCHAR(100)   | NULL     | Grupo econômico quando aplicável                 |
| codigo_cliente_crm  | VARCHAR(20)    | NULL     | Código do cliente no CRM                          |

Chave primária: cod_xp
Índices: PK clustered em cod_xp

Importante: Dados variáveis no tempo (status, patrimônio, assessor, suitability, etc)
           estão em fact_cliente_perfil_historico
*/

-- ==============================================================================
-- 3. DEPENDÊNCIAS
-- ==============================================================================
/*
Schemas necessários:
- silver: Schema onde a tabela será criada
- bronze: Schema de origem dos dados

Tabelas de origem (bronze):
- [bronze].[xp_rpa_clientes]: Dados cadastrais do RPA
  * Campos utilizados: cod_xp, cpf_cnpj, nome_cliente, telefone_cliente, 
                       email_cliente, grupo_economico, codigo_crm
  * Volume: ~50.000 registros ativos
  
- [bronze].[xp_positivador]: Dados demográficos complementares
  * Campos utilizados: cod_xp, data_nascimento, sexo, profissao, data_cadastro
  * Volume: ~7M registros (histórico diário)
  * Usa-se apenas a última data de cada cliente

Processo ETL:
- Procedure: [silver].[prc_load_silver_dim_clientes]
- Execução: Diária (full load com TRUNCATE/INSERT)
- Une todos os cod_xp distintos do positivador com dados do RPA

Pré-requisitos:
- Schema silver deve existir
- Usuário deve ter permissão CREATE TABLE no schema silver
- Tabelas bronze devem estar populadas antes da carga
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
CREATE TABLE [silver].[dim_clientes](
	[cod_xp] [int] NOT NULL,
	[cpf] [varchar](11) NULL,
	[cnpj] [varchar](14) NULL,
	[nome_cliente] [varchar](300) NULL,
	[telefone_cliente] [varchar](50) NULL,
	[email_cliente] [varchar](200) NULL,
	[data_nascimento] [date] NULL,
	[sexo] [char](1) NULL,
	[profissao] [varchar](100) NULL,
	[data_cadastro] [date] NULL,
	[grupo_cliente] [varchar](100) NULL,
	[codigo_cliente_crm] [varchar](20) NULL,
 CONSTRAINT [PK_dim_clientes] PRIMARY KEY CLUSTERED 
(
	[cod_xp] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- ==============================================================================
-- 5. DOCUMENTAÇÃO DAS COLUNAS (EXTENDED PROPERTIES)
-- ==============================================================================

-- Documentação da coluna cod_xp
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código único do cliente na XP. É a chave primária da tabela. Origem: bronze.xp_rpa_clientes.cod_xp', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_clientes', 
    @level2type=N'COLUMN',@level2name=N'cod_xp'
GO

-- Documentação da coluna cpf
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'CPF do cliente pessoa física (11 dígitos). Será NULL para pessoa jurídica. Origem: bronze.xp_rpa_clientes.cpf_cnpj quando LENGTH=11', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_clientes', 
    @level2type=N'COLUMN',@level2name=N'cpf'
GO

-- Documentação da coluna cnpj
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'CNPJ do cliente pessoa jurídica (14 dígitos). Será NULL para pessoa física. Origem: bronze.xp_rpa_clientes.cpf_cnpj quando LENGTH=14', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_clientes', 
    @level2type=N'COLUMN',@level2name=N'cnpj'
GO

-- Documentação da coluna nome_cliente
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Nome completo do cliente ou razão social. Se não existir no RPA, usa o cod_xp como nome. Origem: bronze.xp_rpa_clientes.nome_cliente ou cod_xp', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_clientes', 
    @level2type=N'COLUMN',@level2name=N'nome_cliente'
GO

-- Documentação da coluna telefone_cliente
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Telefone principal do cliente com DDD. Origem: bronze.xp_rpa_clientes.telefone_cliente', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_clientes', 
    @level2type=N'COLUMN',@level2name=N'telefone_cliente'
GO

-- Documentação da coluna email_cliente
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'E-mail principal do cliente. Origem: bronze.xp_rpa_clientes.email_cliente', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_clientes', 
    @level2type=N'COLUMN',@level2name=N'email_cliente'
GO

-- Documentação da coluna data_nascimento
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de nascimento do cliente pessoa física. NULL para PJ. Origem: bronze.xp_positivador.data_nascimento da última data', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_clientes', 
    @level2type=N'COLUMN',@level2name=N'data_nascimento'
GO

-- Documentação da coluna sexo
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Sexo do cliente: M (masculino), F (feminino) ou NULL. Origem: bronze.xp_positivador.sexo da última data', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_clientes', 
    @level2type=N'COLUMN',@level2name=N'sexo'
GO

-- Documentação da coluna profissao
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Profissão declarada pelo cliente. Origem: bronze.xp_positivador.profissao da última data', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_clientes', 
    @level2type=N'COLUMN',@level2name=N'profissao'
GO

-- Documentação da coluna data_cadastro
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de cadastro do cliente na XP. Origem: bronze.xp_positivador.data_cadastro da última data', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_clientes', 
    @level2type=N'COLUMN',@level2name=N'data_cadastro'
GO

-- Documentação da coluna grupo_cliente
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Grupo econômico do cliente quando aplicável. Origem: bronze.xp_rpa_clientes.grupo_economico', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_clientes', 
    @level2type=N'COLUMN',@level2name=N'grupo_cliente'
GO

-- Documentação da coluna codigo_cliente_crm
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do cliente no CRM. Origem: bronze.xp_rpa_clientes.codigo_crm ou silver.dim_pessoas.crm_id', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_clientes', 
    @level2type=N'COLUMN',@level2name=N'codigo_cliente_crm'
GO

-- Documentação da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Dimensão de clientes com dados cadastrais básicos. Contém TODOS os clientes históricos (cod_xp distintos). Dados variáveis no tempo (perfil, patrimônio, etc) foram movidos para fact_cliente_perfil_historico.', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'dim_clientes'
GO

-- ==============================================================================
-- 6. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                      | Descrição
--------|------------|----------------------------|--------------------------------------------
1.0.0   | 2024-11-15 | bruno.chiaramonti         | Criação inicial da tabela
2.0.0   | 2025-01-13 | bruno.chiaramonti         | Simplificação - dados variáveis movidos
                                                  | para fact_cliente_perfil_historico

*/

-- ==============================================================================
-- 7. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Esta tabela contém apenas dados cadastrais básicos e estáticos
- Dados que variam no tempo estão em fact_cliente_perfil_historico
- CPF e CNPJ são mutuamente exclusivos (um ou outro, nunca ambos)
- Nome_cliente nunca é NULL (usa cod_xp como fallback)
- Tabela com compressão de página para otimizar armazenamento
- Considera TODOS os clientes históricos do positivador

Separação de responsabilidades:
- dim_clientes: Dados cadastrais estáticos
- fact_cliente_perfil_historico: Dados que variam no tempo

Recomendações:
1. Criar índice em cpf para consultas por documento
2. Criar índice em cnpj para consultas de PJ
3. Criar índice em nome_cliente para buscas textuais
4. Considerar índice em codigo_cliente_crm se usado frequentemente
5. Implementar compressão de página para economia de espaço

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/

