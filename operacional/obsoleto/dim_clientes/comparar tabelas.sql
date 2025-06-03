SELECT 
    s.cod_xp,
    s.nome_cliente,
    CASE WHEN d.telefone_cliente <> s.telefone_cliente THEN CONCAT(d.telefone_cliente, ' / ', s.telefone_cliente) ELSE s.telefone_cliente END AS telefone_cliente,
    s.email_cliente,
    d.genero,
    d.profissao,
    d.crm_id_cliente,
    d.cpf_cliente,
    d.cnpj_cliente,
    d.data_nascimento,
    d.segmento,
    d.suitability,
    d.tipo_investidor,
    d.flag_grupo_familiar,
    d.[grupo familiar],
    s.elegibilidade_cartao,
    s.produto, -- mudar nome para produto_cartao
    s.status_conta_digital,
    d.status_cliente,
    d.tipo_conta,
    d.segmento_pj,
    d.patrimonio_xp_atual,
    d.patrimonio_xp_max,
    d.patrimonio_declarado, -- remover essa coluna
    s.patrimonio, -- mudar nome para patrimonio_declarado
    d.saldo_em_conta,
    d.cod_aai_assessor,
    d.crm_id_assessor,
    d.cod_aai_primeiro_assessor_m7,
    d.qtd_assessores_m7,
    d.data_ultimo_aporte,
    d.data_ultima_ordem,
    d.fee_based,
    d.flag_open_investment,
    d.saldo_open_investment,
    d.data_cadastro,
    d.data_cadastro_m7,
    d.safra_ano_mes


FROM [M7InvestimentosOLAP].[DS].[dim_clientes] d
INNER JOIN [M7InvestimentosOLAP].[DS].[stage_clientes_rpa] s
    ON d.cod_xp = s.cod_xp
WHERE d.cod_xp IS NOT NULL AND s.cod_xp IS NOT NULL;