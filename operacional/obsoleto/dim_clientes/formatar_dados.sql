-- Script para formatar os dados existentes antes das alterações na estrutura

-- Formatar telefone (remover caracteres especiais e garantir tamanho máximo de 20)
UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [telefone_cliente] = REPLACE(REPLACE(REPLACE(REPLACE([telefone_cliente], '(', ''), ')', ''), '-', ''), ' ', '')
WHERE [telefone_cliente] IS NOT NULL;

-- Formatar CPF (remover caracteres especiais e garantir tamanho de 11)
UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [cpf_cliente] = REPLACE(REPLACE([cpf_cliente], '.', ''), '-', '')
WHERE [cpf_cliente] IS NOT NULL;

-- Formatar CNPJ (remover caracteres especiais e garantir tamanho de 14)
UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [cnpj_cliente] = REPLACE(REPLACE(REPLACE([cnpj_cliente], '.', ''), '-', ''), '/', '')
WHERE [cnpj_cliente] IS NOT NULL;

-- Formatar email (remover espaços em branco e garantir tamanho máximo de 250)
UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [email_cliente] = LTRIM(RTRIM([email_cliente]))
WHERE [email_cliente] IS NOT NULL;

-- Formatar datas (garantir que estão no formato correto)
UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [data_cadastro] = CONVERT(DATE, [data_cadastro])
WHERE [data_cadastro] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [data_cadastro_m7] = CONVERT(DATE, [data_cadastro_m7])
WHERE [data_cadastro_m7] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [data_nascimento] = CONVERT(DATE, [data_nascimento])
WHERE [data_nascimento] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [data_ultimo_aporte] = CONVERT(DATE, [data_ultimo_aporte])
WHERE [data_ultimo_aporte] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [data_ultima_ordem] = CONVERT(DATE, [data_ultima_ordem])
WHERE [data_ultima_ordem] IS NOT NULL;

-- Formatar valores monetários (garantir precisão correta)
UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [patrimonio_xp_atual] = ROUND([patrimonio_xp_atual], 2)
WHERE [patrimonio_xp_atual] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [patrimonio_xp_max] = ROUND([patrimonio_xp_max], 2)
WHERE [patrimonio_xp_max] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [patrimonio_declarado] = ROUND([patrimonio_declarado], 2)
WHERE [patrimonio_declarado] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [saldo_em_conta] = ROUND([saldo_em_conta], 2)
WHERE [saldo_em_conta] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [saldo_open_investment] = ROUND([saldo_open_investment], 2)
WHERE [saldo_open_investment] IS NOT NULL;

-- Formatar campos de texto (remover espaços extras)
UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [nome_cliente] = LTRIM(RTRIM([nome_cliente]))
WHERE [nome_cliente] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [profissao] = LTRIM(RTRIM([profissao]))
WHERE [profissao] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [segmento] = LTRIM(RTRIM([segmento]))
WHERE [segmento] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [segmento_pj] = LTRIM(RTRIM([segmento_pj]))
WHERE [segmento_pj] IS NOT NULL;

-- Formatar campos de código (remover espaços extras)
UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [crm_id_cliente] = LTRIM(RTRIM([crm_id_cliente]))
WHERE [crm_id_cliente] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [crm_id_assessor] = LTRIM(RTRIM([crm_id_assessor]))
WHERE [crm_id_assessor] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [cod_aai_assessor] = LTRIM(RTRIM([cod_aai_assessor]))
WHERE [cod_aai_assessor] IS NOT NULL;

UPDATE [M7InvestimentosOLAP].[dim].[dim_clientes]
SET [cod_aai_primeiro_assessor_m7] = LTRIM(RTRIM([cod_aai_primeiro_assessor_m7]))
WHERE [cod_aai_primeiro_assessor_m7] IS NOT NULL;

GO 