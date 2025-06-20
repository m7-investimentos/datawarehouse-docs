-- ==============================================================================
-- QRY-ESP-001-CREATE_SILVER_FACT_ESTRUTURA_PESSOAS
-- ==============================================================================
-- Tipo: DDL - Create Table
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [ddl, silver, fact_table, estrutura, pessoas, histórico]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela fato fact_estrutura_pessoas no schema silver.
           Armazena o histórico de movimentações de pessoas entre estruturas
           organizacionais, permitindo rastrear quando uma pessoa entrou ou
           saiu de uma estrutura específica.

Casos de uso:
- Rastreamento de histórico organizacional de assessores
- Análise de rotatividade entre estruturas
- Identificação da estrutura atual de cada pessoa
- Análise de tempo de permanência em estruturas
- Suporte a relatórios gerenciais e dashboards

Frequência de execução: Única (criação da tabela)
Volume esperado de linhas: ~5.000 registros
*/

-- ==============================================================================
-- 2. ESTRUTURA DA TABELA
-- ==============================================================================
/*
Colunas da tabela:

| Coluna         | Tipo        | Nullable | Descrição                                              |
|----------------|-------------|----------|--------------------------------------------------------|
| crm_id         | VARCHAR(20) | NOT NULL | Identificador único da pessoa no CRM (FK)             |
| id_estrutura   | INT         | NOT NULL | Identificador da estrutura organizacional (FK)        |
| data_entrada   | DATE        | NOT NULL | Data em que a pessoa entrou na estrutura (FK)         |
| data_saida     | DATE        | NULL     | Data em que a pessoa saiu (NULL = ainda na estrutura) |

Chave primária: (crm_id, id_estrutura, data_entrada)
Chaves estrangeiras:
- crm_id -> dim_pessoas.crm_id
- id_estrutura -> dim_estruturas.id_estrutura  
- data_entrada -> dim_calendario.data_ref
*/

-- ==============================================================================
-- 3. DEPENDÊNCIAS
-- ==============================================================================
/*
Schemas necessários:
- silver: Schema onde a tabela será criada

Tabelas relacionadas (silver):
- [silver].[dim_pessoas]: Dimensão de pessoas (FK: crm_id)
  * Deve existir antes da criação desta tabela
  * Contém dados cadastrais de assessores e funcionários
  
- [silver].[dim_estruturas]: Dimensão de estruturas organizacionais (FK: id_estrutura)
  * Deve existir antes da criação desta tabela
  * Contém hierarquia organizacional
  
- [silver].[dim_calendario]: Dimensão calendário (FK: data_entrada)
  * Deve existir antes da criação desta tabela
  * Usada para garantir integridade referencial de datas

Processo de carga:
- Dados podem vir de sistemas de RH ou CRM
- Atualizações quando há mudanças organizacionais
- data_saida preenchida quando pessoa muda de estrutura

Pré-requisitos:
- Schema silver deve existir
- Tabelas de dimensão devem estar criadas e populadas
- Usuário deve ter permissão CREATE TABLE no schema silver
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
CREATE TABLE [silver].[fact_estrutura_pessoas](
	[crm_id] [varchar](20) NOT NULL,
	[id_estrutura] [int] NOT NULL,
	[data_entrada] [date] NOT NULL,
	[data_saida] [date] NULL,
 CONSTRAINT [PK_fato_estrutura_pessoas] PRIMARY KEY CLUSTERED 
(
	[crm_id] ASC,
	[id_estrutura] ASC,
	[data_entrada] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- ==============================================================================
-- 5. CONSTRAINTS E RELACIONAMENTOS
-- ==============================================================================

-- Foreign Key para dim_pessoas
ALTER TABLE [silver].[fact_estrutura_pessoas]  WITH NOCHECK ADD  CONSTRAINT [FK_fato_estrutura_pessoa_pessoas] FOREIGN KEY([crm_id])
REFERENCES [silver].[dim_pessoas] ([crm_id])
GO
ALTER TABLE [silver].[fact_estrutura_pessoas] NOCHECK CONSTRAINT [FK_fato_estrutura_pessoa_pessoas]
GO

-- Foreign Key para dim_calendario
ALTER TABLE [silver].[fact_estrutura_pessoas]  WITH NOCHECK ADD  CONSTRAINT [FK_fato_estrutura_pessoas_calendario] FOREIGN KEY([data_entrada])
REFERENCES [silver].[dim_calendario] ([data_ref])
GO
ALTER TABLE [silver].[fact_estrutura_pessoas] NOCHECK CONSTRAINT [FK_fato_estrutura_pessoas_calendario]
GO

-- Foreign Key para dim_estruturas
ALTER TABLE [silver].[fact_estrutura_pessoas]  WITH NOCHECK ADD  CONSTRAINT [FK_fato_estrutura_pessoas_estrutura] FOREIGN KEY([id_estrutura])
REFERENCES [silver].[dim_estruturas] ([id_estrutura])
GO
ALTER TABLE [silver].[fact_estrutura_pessoas] CHECK CONSTRAINT [FK_fato_estrutura_pessoas_estrutura]
GO

-- ==============================================================================
-- 6. DOCUMENTAÇÃO DAS COLUNAS (EXTENDED PROPERTIES)
-- ==============================================================================

-- Documentação da coluna crm_id
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'identificador único da pessoa no CRM. FK para dim_pessoas.crm_id' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_estrutura_pessoas', 
    @level2type=N'COLUMN',@level2name=N'crm_id'
GO

-- Documentação da coluna id_estrutura
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'identificador único da estrutura organizacional. FK para dim_estruturas.id_estrutura' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_estrutura_pessoas', 
    @level2type=N'COLUMN',@level2name=N'id_estrutura'
GO

-- Documentação da coluna data_entrada
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'data em que a pessoa entrou na estrutura. FK para dim_calendario.data_ref' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_estrutura_pessoas', 
    @level2type=N'COLUMN',@level2name=N'data_entrada'
GO

-- Documentação da coluna data_saida
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'data em que a pessoa saiu da estrutura. nulo se ainda estiver na estrutura. permite rastrear período de permanência' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_estrutura_pessoas', 
    @level2type=N'COLUMN',@level2name=N'data_saida'
GO

-- Documentação da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'tabela fato que contém o histórico de estrutura organizacional das pessoas. registra quando uma pessoa entrou e saiu de uma estrutura. não significa que a pessoa saiu da M7, pode ter apenas saído de uma estrutura. permite análise de rotatividade e tempo de permanência em cada estrutura.' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'TABLE',@level1name=N'fact_estrutura_pessoas'
GO

-- ==============================================================================
-- 7. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-20 | [Nome]         | Criação inicial da tabela

*/

-- ==============================================================================
-- 8. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Chave primária composta garante que uma pessoa não pode entrar na mesma estrutura
  na mesma data mais de uma vez
- data_saida NULL indica que a pessoa ainda está na estrutura
- Constraints com NOCHECK para melhor performance na carga inicial
- Uma pessoa pode ter múltiplos registros se mudou de estrutura várias vezes
- Não significa demissão - apenas mudança organizacional

Cenários de uso:
1. Pessoa entra na empresa: Novo registro com data_entrada
2. Pessoa muda de estrutura: data_saida no registro anterior + novo registro
3. Pessoa sai da empresa: data_saida preenchida no último registro
4. Consulta estrutura atual: WHERE data_saida IS NULL

Queries úteis:
-- Estrutura atual de uma pessoa
SELECT * FROM silver.fact_estrutura_pessoas 
WHERE crm_id = 'XXX' AND data_saida IS NULL;

-- Histórico completo de uma pessoa
SELECT * FROM silver.fact_estrutura_pessoas 
WHERE crm_id = 'XXX' 
ORDER BY data_entrada;

-- Pessoas em uma estrutura específica
SELECT * FROM silver.fact_estrutura_pessoas 
WHERE id_estrutura = 123 AND data_saida IS NULL;

Recomendações:
1. Criar índice em (data_saida) para queries de estrutura atual
2. Criar índice em (id_estrutura, data_saida) para análises por estrutura
3. Implementar trigger para garantir que data_saida >= data_entrada
4. Implementar procedure para movimentação entre estruturas
5. Adicionar auditoria de mudanças para rastreabilidade

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
