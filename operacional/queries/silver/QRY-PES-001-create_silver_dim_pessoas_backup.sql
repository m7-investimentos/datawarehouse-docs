SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [silver].[dim_pessoas](
	[crm_id] [varchar](20) NOT NULL,
	[nome_pessoa] [varchar](200) NOT NULL,
	[cod_aai] [varchar](50) NULL,
	[id_avenue] [varchar](50) NULL,
	[id_rd_station] [varchar](50) NULL,
	[data_nascimento] [date] NULL,
	[data_inicio_vigencia] [date] NOT NULL,
	[data_fim_vigencia] [date] NULL,
	[email_multisete] [varchar](200) NULL,
	[email_xp] [varchar](200) NULL,
	[observacoes] [varchar](200) NULL,
	[assessor_nivel] [varchar](50) NULL,
 CONSTRAINT [PK_dim_pessoas] PRIMARY KEY CLUSTERED 
(
	[crm_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador único da pessoa no crm' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'observacoes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Nível hierárquico do assessor: Junior, Pleno ou Senior. NULL para não-assessores' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'assessor_nivel'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Tabela dimensão contendo informações cadastrais de todos os colaboradores, assessores e funcionários da M7. Inclui histórico de pessoas que saíram da empresa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas'
GO

-- ==============================================================================
-- 6. ÍNDICES RECOMENDADOS
-- ==============================================================================
/*
-- Índice para buscar assessores ativos por código AAI
CREATE NONCLUSTERED INDEX [IX_dim_pessoas_cod_aai]
ON [silver].[dim_pessoas] ([cod_aai])
WHERE [cod_aai] IS NOT NULL AND [data_fim_vigencia] IS NULL
GO

-- Índice para filtrar pessoas ativas
CREATE NONCLUSTERED INDEX [IX_dim_pessoas_vigencia]
ON [silver].[dim_pessoas] ([data_fim_vigencia], [data_inicio_vigencia])
INCLUDE ([nome_pessoa], [cod_aai])
GO

-- Índice para buscar por e-mail
CREATE NONCLUSTERED INDEX [IX_dim_pessoas_email]
ON [silver].[dim_pessoas] ([email_multisete], [email_xp])
GO
*/

-- ==============================================================================
-- 7. VALIDAÇÕES E CONSTRAINTS
-- ==============================================================================
/*
-- Constraint para garantir que data fim seja maior que data início
ALTER TABLE [silver].[dim_pessoas]
ADD CONSTRAINT [CK_dim_pessoas_vigencia] 
CHECK ([data_fim_vigencia] IS NULL OR [data_fim_vigencia] >= [data_inicio_vigencia])
GO

-- Constraint para validar formato de e-mail
ALTER TABLE [silver].[dim_pessoas]
ADD CONSTRAINT [CK_dim_pessoas_email_multisete] 
CHECK ([email_multisete] IS NULL OR [email_multisete] LIKE '%@m7investimentos.com.br')
GO

-- Constraint para validar níveis de assessor
ALTER TABLE [silver].[dim_pessoas]
ADD CONSTRAINT [CK_dim_pessoas_nivel] 
CHECK ([assessor_nivel] IS NULL OR [assessor_nivel] IN ('Junior', 'Pleno', 'Senior'))
GO
*/

-- ==============================================================================
-- 8. QUERIES DE VALIDAÇÃO
-- ==============================================================================
/*
-- Query para verificar a criação da tabela
SELECT 
    t.name AS tabela,
    s.name AS schema_name,
    t.create_date,
    t.modify_date
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'silver' AND t.name = 'dim_pessoas';

-- Query para listar pessoas ativas
SELECT 
    crm_id,
    nome_pessoa,
    cod_aai,
    assessor_nivel,
    data_inicio_vigencia,
    DATEDIFF(YEAR, data_inicio_vigencia, GETDATE()) as anos_empresa
FROM silver.dim_pessoas
WHERE data_fim_vigencia IS NULL
ORDER BY data_inicio_vigencia;

-- Query para contar assessores por nível
SELECT 
    assessor_nivel,
    COUNT(*) as quantidade,
    COUNT(CASE WHEN data_fim_vigencia IS NULL THEN 1 END) as ativos,
    COUNT(CASE WHEN data_fim_vigencia IS NOT NULL THEN 1 END) as inativos
FROM silver.dim_pessoas
WHERE cod_aai IS NOT NULL
GROUP BY assessor_nivel;

-- Query para verificar integridade referencial com outras tabelas
SELECT 
    'Pessoas sem cod_aai' as validacao,
    COUNT(*) as quantidade
FROM silver.dim_pessoas
WHERE cod_aai IS NULL 
    AND data_fim_vigencia IS NULL
    AND EXISTS (
        SELECT 1 FROM silver.fact_clientes fc 
        WHERE fc.cod_assessor = dim_pessoas.crm_id
    );
*/

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-20 | [Nome]         | Criação inicial da tabela

*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Tabela dimensão com baixo volume mas alta importância
- crm_id é a chave primária e deve ser única
- Controle de vigência permite manter histórico de ex-colaboradores
- cod_aai é obrigatório apenas para assessores de investimento
- Múltiplos IDs externos para integração com diferentes sistemas
- email_multisete deve seguir padrão @m7investimentos.com.br

Regras de negócio:
- Uma pessoa pode ter múltiplos períodos de vigência (saiu e voltou)
- Assessor deve ter cod_aai preenchido
- data_fim_vigencia NULL indica pessoa ativa
- assessor_nivel só se aplica a pessoas com cod_aai
- Pessoa pode ter emails em ambos domínios (M7 e XP)

Integrações:
- CRM: Sistema principal de cadastro (crm_id)
- XP: Sistema de assessores (cod_aai)
- Avenue: Sistema de comissionamento (id_avenue)
- RD Station: Marketing e CRM (id_rd_station)

Processo de carga:
1. Carga inicial do CRM com todas as pessoas
2. Enriquecimento com dados da XP (cod_aai)
3. Matching com Avenue e RD Station
4. Atualização diária de mudanças
5. Controle de vigência para entrada/saída

Validações importantes:
- Não permitir duplicação de crm_id
- Validar formato de emails
- Garantir que data_fim >= data_inicio
- Verificar se assessores têm cod_aai

Possíveis melhorias:
1. Adicionar campo para departamento/área
2. Incluir campo para gestor direto (auto-relacionamento)
3. Adicionar campos para telefones de contato
4. Implementar auditoria de mudanças
5. Adicionar campo para tipo de pessoa (Assessor/Admin/Gestor)
6. Incluir foto ou avatar da pessoa

Impacto em outras tabelas:
- fact_clientes: Usa cod_aai para vincular assessor
- fact_comissoes: Usa crm_id para cálculos
- dim_estruturas: Referencia pessoas em hierarquia
- fact_metas: Vincula metas a pessoas

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/crm_id'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nome completo da pessoa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'nome_pessoa'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código da pessoa no sistema da xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'cod_aai'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador da pessoa no sistema avenue' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'id_avenue'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'identificador da pessoa no sistema rd station' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'id_rd_station'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de nascimento da pessoa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'data_nascimento'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de início da vigência da pessoa, quando entrou na empresa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'data_inicio_vigencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de fim da vigência do registro, quando saiu da empresa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'data_fim_vigencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'email da pessoa multisete' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'email_multisete'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'email da pessoa no domínio xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'email_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'observações adicionais sobre a pessoa' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'observacoes'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'nível do assessor, se o assessor é junior, pleno ou senior' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas', @level2type=N'COLUMN',@level2name=N'assessor_nivel'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela dimensão que contém as informações cadastrais dos colaboradores, assessores e funcionarios no geral' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'dim_pessoas'
GO
