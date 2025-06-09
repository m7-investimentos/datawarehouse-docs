-- ==============================================================================
-- QRY-CLI-001-create_silver_dim_clientes
-- ==============================================================================
-- Tipo: Create Table
-- Versão: 1.0.0
-- Última atualização: 2025-01-06
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
Descrição: Dimensão de clientes com dados cadastrais e patrimoniais. Contém TODOS 
           os clientes históricos (cod_xp distintos do Positivador). Para cada 
           cliente, usa os dados mais recentes do positivador e complementa com 
           RPA quando disponível.

Casos de uso:
- Master data de clientes para joins em análises
- Segmentação de clientes por perfil e patrimônio
- Análise de evolução da base de clientes
- Identificação de clientes ativos/inativos/evadidos

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
Status do cliente:
- ATIVO: presente na última data geral com status=1
- INATIVO: presente na última data geral com status=0  
- EVADIU: não aparece na última data geral

Última data geral de referência: 2025-05-28
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas utilizadas:
- [bronze].[xp_rpa_clientes]: Dados cadastrais do RPA
- [bronze].[xp_positivador]: Dados patrimoniais e demográficos

Pré-requisitos:
- Dados atualizados nas tabelas bronze
- Índices em xp_positivador: (cod_xp, data_ref)
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

-- Remover tabela existente se necessário
IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'[silver].[dim_clientes]'))
    DROP TABLE [silver].[dim_clientes]
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
    [suitability] [varchar](50) NULL,
    [tipo_investidor] [varchar](100) NULL,
    [segmento_cliente] [varchar](50) NULL,
    [grupo_cliente] [varchar](100) NULL,
    [faixa_etaria] [varchar](50) NULL,
    [codigo_cliente_crm] [varchar](20) NULL,
    [patrimonio_declarado] [decimal](18, 2) NULL,
    [patrimonio_xp] [decimal](18, 2) NULL,
    [cod_assessor] [varchar](20) NULL,
    [fee_based] [varchar](25) NULL,
    [status_cliente] [varchar](20) NOT NULL,
    [data_ref_positivador] [date] NULL,
 CONSTRAINT [PK_dim_clientes] PRIMARY KEY CLUSTERED 
(
    [cod_xp] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- ==============================================================================
-- 7. PROPRIEDADES ESTENDIDAS
-- ==============================================================================

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
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Perfil de suitability do cliente: Conservador, Moderado, Agressivo, etc. Origem: bronze.xp_rpa_clientes.suitability' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'suitability'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Classificação CVM do investidor: Investidor Regular ou Investidor Qualificado. Origem: bronze.xp_rpa_clientes.tipo_investidor' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'tipo_investidor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Segmento do cliente (Varejo, Private, Corporate, etc). Origem: bronze.xp_rpa_clientes.segmento' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'segmento_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Grupo econômico do cliente quando aplicável. Origem: bronze.xp_rpa_clientes.grupo_economico' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'grupo_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Faixa etária calculada: Menor de 18, 18-25, 26-35, 36-45, 46-55, 56-65, Acima de 65, Pessoa Jurídica. Calculado baseado em data_nascimento' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'faixa_etaria'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do cliente no CRM. Origem: bronze.xp_rpa_clientes.codigo_crm ou silver.dim_pessoas.crm_id' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'codigo_cliente_crm'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio total declarado pelo cliente em outras instituições. Origem: bronze.xp_positivador.aplicacao_financeira_declarada da última data em que o cliente apareceu' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'patrimonio_declarado'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Patrimônio do cliente na XP. Origem: bronze.xp_positivador.net_em_M da última data em que o cliente apareceu' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'patrimonio_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Código do assessor responsável pelo cliente. Origem: bronze.xp_positivador.cod_aai da última data em que o cliente apareceu' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Indica se o cliente está em modelo fee-based: Sim ou Não. Origem: bronze.xp_rpa_clientes.fee_based' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'fee_based'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Status atual do cliente: ATIVO (presente na última data geral com status=1), INATIVO (presente na última data geral com status=0) ou EVADIU (não aparece na última data geral). Baseado na presença em 2025-05-28.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'status_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Data de referência do positivador - última vez que o cliente apareceu. Para ATIVO/INATIVO será 2025-05-28. Para EVADIU será a última data em que apareceu antes de sair.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes', @level2type=N'COLUMN',@level2name=N'data_ref_positivador'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Dimensão de clientes com dados cadastrais e patrimoniais. Contém TODOS os clientes históricos (cod_xp distintos do Positivador). Para cada cliente, usa os dados mais recentes do positivador e complementa com RPA quando disponível. Status: ATIVO (última data geral com status=1), INATIVO (última data geral com status=0) ou EVADIU (não aparece na última data geral). Última data geral: 2025-05-28.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_clientes'
GO

-- ==============================================================================
-- 8. QUERIES AUXILIARES (PARA VALIDAÇÃO)
-- ==============================================================================
/*
-- Verificar distribuição por status
SELECT 
    status_cliente, 
    COUNT(*) as qtd_clientes,
    AVG(patrimonio_xp) as patrimonio_medio
FROM [silver].[dim_clientes]
GROUP BY status_cliente
ORDER BY qtd_clientes DESC;

-- Verificar clientes sem nome
SELECT COUNT(*) as clientes_sem_nome
FROM [silver].[dim_clientes]
WHERE nome_cliente IS NULL OR nome_cliente = CAST(cod_xp AS VARCHAR);

-- Análise de segmentação
SELECT 
    CASE 
        WHEN cpf IS NOT NULL THEN 'PF'
        WHEN cnpj IS NOT NULL THEN 'PJ'
        ELSE 'Não identificado'
    END AS tipo_cliente,
    COUNT(*) as quantidade,
    AVG(patrimonio_xp) as patrimonio_medio
FROM [silver].[dim_clientes]
GROUP BY 
    CASE 
        WHEN cpf IS NOT NULL THEN 'PF'
        WHEN cnpj IS NOT NULL THEN 'PJ'
        ELSE 'Não identificado'
    END;

-- Verificação de grupos de clientes
SELECT 
    grupo_cliente,
    COUNT(*) as qtd_clientes
FROM [silver].[dim_clientes]
WHERE grupo_cliente IS NOT NULL
GROUP BY grupo_cliente
ORDER BY qtd_clientes DESC;
*/

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
- A tabela contém apenas dados cadastrais básicos
- CPF e CNPJ são mutuamente exclusivos
- grupo_cliente pode estar NULL para clientes sem grupo econômico
- Dados de perfil, patrimônio e status estão em fact_cliente_perfil_historico
- Esta tabela é estável e raramente muda após carga inicial

Troubleshooting comum:
1. Clientes duplicados: Verificar integridade do cod_xp nas fontes
2. Código CRM faltante: Verificar mapeamento com dim_pessoas
3. Dados faltantes: Nem todos os clientes têm dados completos em todas as fontes

Contato para dúvidas: bruno.chiaramonti@multisete.com
*/