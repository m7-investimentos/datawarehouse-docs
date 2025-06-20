-- ==============================================================================
-- QRY-CLI-001-create_silver_dim_clientes
-- ==============================================================================
-- Tipo: Create Table
-- Versão: 2.0.0
-- Última atualização: 2025-01-13
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [cliente, dimensão, cadastro, silver]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Dimensão de clientes com dados cadastrais básicos. Contém TODOS 
           os clientes históricos (cod_xp distintos). Dados variáveis no tempo 
           (perfil, patrimônio, status, etc) foram movidos para 
           fact_cliente_perfil_historico.

Casos de uso:
- Master data de clientes para joins em análises
- Identificação única de clientes
- Dados cadastrais básicos e estáticos

Frequência de atualização: Diária
Tempo médio de carga: ~2 minutos
Volume esperado de linhas: ~200.000 registros
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Não aplicável - tabela de dimensão
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Importante: Esta tabela contém apenas dados cadastrais básicos e estáticos.
Dados que variam no tempo (status, patrimônio, assessor, suitability, etc) 
estão em fact_cliente_perfil_historico.
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas utilizadas:
- [bronze].[xp_rpa_clientes]: Dados cadastrais do RPA
- [bronze].[xp_positivador]: Dados demográficos complementares

Pré-requisitos:
- Dados atualizados nas tabelas bronze
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
-- Tabela com compressão de página para otimizar armazenamento
-- Índice clustered na chave primária (cod_xp)

-- ==============================================================================
-- 6. IMPLEMENTAÇÃO DA TABELA
-- ==============================================================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
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
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código único do cliente na XP. É a chave primária da tabela. Origem: bronze.xp_rpa_clientes.cod_xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'cod_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'CPF do cliente pessoa física (11 dígitos). Será NULL para pessoa jurídica. Origem: bronze.xp_rpa_clientes.cpf_cnpj quando LENGTH=11' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'cpf'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'CNPJ do cliente pessoa jurídica (14 dígitos). Será NULL para pessoa física. Origem: bronze.xp_rpa_clientes.cpf_cnpj quando LENGTH=14' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'cnpj'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nome completo do cliente ou razão social. Se não existir no RPA, usa o cod_xp como nome. Origem: bronze.xp_rpa_clientes.nome_cliente ou cod_xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'nome_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Telefone principal do cliente com DDD. Origem: bronze.xp_rpa_clientes.telefone_cliente' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'telefone_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'E-mail principal do cliente. Origem: bronze.xp_rpa_clientes.email_cliente' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'email_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de nascimento do cliente pessoa física. NULL para PJ. Origem: bronze.xp_positivador.data_nascimento da última data' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'data_nascimento'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Sexo do cliente: M (masculino), F (feminino) ou NULL. Origem: bronze.xp_positivador.sexo da última data' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'sexo'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Profissão declarada pelo cliente. Origem: bronze.xp_positivador.profissao da última data' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'profissao'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de cadastro do cliente na XP. Origem: bronze.xp_positivador.data_cadastro da última data' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'data_cadastro'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Grupo econômico do cliente quando aplicável. Origem: bronze.xp_rpa_clientes.grupo_economico' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'grupo_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do cliente no CRM. Origem: bronze.xp_rpa_clientes.codigo_crm ou silver.dim_pessoas.crm_id' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'codigo_cliente_crm'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Dimensão de clientes com dados cadastrais básicos. Contém TODOS os clientes históricos (cod_xp distintos). Dados variáveis no tempo (perfil, patrimônio, etc) foram movidos para fact_cliente_perfil_historico.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes'
GO

