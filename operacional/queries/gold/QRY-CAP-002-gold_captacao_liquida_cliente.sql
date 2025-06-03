-- =========================================================================
-- TABELA: gold_captacao_liquida_cliente
-- =========================================================================
-- Grain: Cliente × Mês
-- Propósito: Tabela principal com visão 360° do cliente para IA, Dashboards e ML
-- Autor: Data Science Team - M7 Investimentos
-- Data Criação: 2025-01-02
-- =========================================================================

CREATE TABLE IF NOT EXISTS gold.gold_captacao_liquida_cliente (
    -- =======================================
    -- CHAVES E DIMENSÕES TEMPORAIS
    -- =======================================
    data_referencia DATE NOT NULL COMMENT 'Último dia do mês',
    ano INT NOT NULL COMMENT 'Ano de referência',
    mes INT NOT NULL COMMENT 'Mês de referência',
    trimestre INT NOT NULL COMMENT 'Trimestre de referência',
    semestre INT NOT NULL COMMENT 'Semestre de referência',

    -- =======================================
    -- DIMENSÕES DO CLIENTE
    -- =======================================
    cliente_id VARCHAR(50) NOT NULL COMMENT 'ID único do cliente',
    cliente_nome VARCHAR(200) COMMENT 'Nome completo do cliente',
    cliente_cpf_cnpj VARCHAR(20) COMMENT 'CPF ou CNPJ do cliente',
    cliente_tipo VARCHAR(20) COMMENT 'Tipo de cliente: PF/PJ',
    cliente_segmento VARCHAR(50) COMMENT 'Segmento: Varejo/Alta Renda/Private/Corporate',
    cliente_subsegmento VARCHAR(50) COMMENT 'Detalhamento do segmento',
    cliente_data_abertura DATE COMMENT 'Data de abertura da conta',
    cliente_meses_relacionamento INT COMMENT 'Meses desde abertura da conta',
    cliente_status VARCHAR(20) COMMENT 'Status: Ativo/Inativo/Churned',

    -- =======================================
    -- DIMENSÕES DO ASSESSOR/ESTRUTURA
    -- =======================================
    assessor_id VARCHAR(50) COMMENT 'ID do assessor responsável',
    assessor_nome VARCHAR(200) COMMENT 'Nome do assessor',
    assessor_tipo VARCHAR(50) COMMENT 'Tipo: Senior/Pleno/Junior',
    assessor_meses_experiencia INT COMMENT 'Meses de experiência',
    escritorio_id VARCHAR(50) COMMENT 'ID do escritório',
    escritorio_nome VARCHAR(200) COMMENT 'Nome do escritório',
    regional_id VARCHAR(50) COMMENT 'ID da regional',
    regional_nome VARCHAR(200) COMMENT 'Nome da regional',
    diretoria_id VARCHAR(50) COMMENT 'ID da diretoria',
    diretoria_nome VARCHAR(200) COMMENT 'Nome da diretoria',

    -- =======================================
    -- MÉTRICAS DE CAPTAÇÃO BRUTA
    -- =======================================
    captacao_bruta_mes DECIMAL(18,2) COMMENT 'Captação bruta no mês',
    captacao_bruta_trimestre DECIMAL(18,2) COMMENT 'Captação bruta no trimestre',
    captacao_bruta_semestre DECIMAL(18,2) COMMENT 'Captação bruta no semestre',
    captacao_bruta_ano DECIMAL(18,2) COMMENT 'Captação bruta no ano',
    captacao_bruta_12m DECIMAL(18,2) COMMENT 'Captação bruta rolling 12 meses',

    -- Detalhamento por Tipo de Aplicação
    captacao_bruta_rv_mes DECIMAL(18,2) COMMENT 'Captação em renda variável',
    captacao_bruta_rf_mes DECIMAL(18,2) COMMENT 'Captação em renda fixa',
    captacao_bruta_fundos_mes DECIMAL(18,2) COMMENT 'Captação em fundos',
    captacao_bruta_previdencia_mes DECIMAL(18,2) COMMENT 'Captação em previdência',
    captacao_bruta_coe_mes DECIMAL(18,2) COMMENT 'Captação em COE',

    -- =======================================
    -- MÉTRICAS DE RESGATES
    -- =======================================
    resgate_mes DECIMAL(18,2) COMMENT 'Resgates no mês',
    resgate_trimestre DECIMAL(18,2) COMMENT 'Resgates no trimestre',
    resgate_semestre DECIMAL(18,2) COMMENT 'Resgates no semestre',
    resgate_ano DECIMAL(18,2) COMMENT 'Resgates no ano',
    resgate_12m DECIMAL(18,2) COMMENT 'Resgates rolling 12 meses',

    -- Detalhamento por Tipo
    resgate_rv_mes DECIMAL(18,2) COMMENT 'Resgates de renda variável',
    resgate_rf_mes DECIMAL(18,2) COMMENT 'Resgates de renda fixa',
    resgate_fundos_mes DECIMAL(18,2) COMMENT 'Resgates de fundos',
    resgate_previdencia_mes DECIMAL(18,2) COMMENT 'Resgates de previdência',
    resgate_coe_mes DECIMAL(18,2) COMMENT 'Resgates de COE',

    -- =======================================
    -- CAPTAÇÃO LÍQUIDA (MÉTRICA PRINCIPAL)
    -- =======================================
    captacao_liquida_mes DECIMAL(18,2) COMMENT 'Captação líquida no mês',
    captacao_liquida_trimestre DECIMAL(18,2) COMMENT 'Captação líquida no trimestre',
    captacao_liquida_semestre DECIMAL(18,2) COMMENT 'Captação líquida no semestre',
    captacao_liquida_ano DECIMAL(18,2) COMMENT 'Captação líquida no ano',
    captacao_liquida_12m DECIMAL(18,2) COMMENT 'Captação líquida rolling 12 meses',
    captacao_liquida_acumulada DECIMAL(18,2) COMMENT 'Captação líquida desde início',

    -- =======================================
    -- MÉTRICAS DE PATRIMÔNIO
    -- =======================================
    patrimonio_atual DECIMAL(18,2) COMMENT 'Patrimônio atual do cliente',
    patrimonio_mes_anterior DECIMAL(18,2) COMMENT 'Patrimônio mês anterior',
    patrimonio_medio_mes DECIMAL(18,2) COMMENT 'Patrimônio médio no mês',
    patrimonio_medio_trimestre DECIMAL(18,2) COMMENT 'Patrimônio médio no trimestre',
    patrimonio_medio_12m DECIMAL(18,2) COMMENT 'Patrimônio médio 12 meses',

    -- Variações de Patrimônio
    variacao_patrimonio_mes DECIMAL(18,2) COMMENT 'Variação absoluta no mês',
    variacao_patrimonio_mes_pct DECIMAL(8,4) COMMENT 'Variação percentual no mês',
    variacao_patrimonio_ano_pct DECIMAL(8,4) COMMENT 'Variação percentual no ano',

    -- =======================================
    -- MÉTRICAS DE RENTABILIDADE
    -- =======================================
    rentabilidade_mes_pct DECIMAL(8,4) COMMENT 'Rentabilidade % no mês',
    rentabilidade_ano_pct DECIMAL(8,4) COMMENT 'Rentabilidade % no ano',
    rentabilidade_12m_pct DECIMAL(8,4) COMMENT 'Rentabilidade % 12 meses',
    rentabilidade_acumulada_pct DECIMAL(8,4) COMMENT 'Rentabilidade % acumulada',
    rentabilidade_vs_cdi_mes DECIMAL(8,4) COMMENT 'Spread sobre CDI no mês',
    rentabilidade_vs_ibov_mes DECIMAL(8,4) COMMENT 'Spread sobre Ibovespa no mês',
    share_of_wallet_estimado DECIMAL(8,4) COMMENT 'Share of wallet estimado',

    -- =======================================
    -- MÉTRICAS DE COMPORTAMENTO E ENGAJAMENTO
    -- =======================================
    -- Transações e Atividade
    qtd_aplicacoes_mes INT COMMENT 'Quantidade de aplicações no mês',
    qtd_resgates_mes INT COMMENT 'Quantidade de resgates no mês',
    qtd_transacoes_mes INT COMMENT 'Total de transações no mês',
    ticket_medio_aplicacao DECIMAL(18,2) COMMENT 'Ticket médio das aplicações',
    ticket_medio_resgate DECIMAL(18,2) COMMENT 'Ticket médio dos resgates',

    -- Diversificação
    qtd_produtos_ativos INT COMMENT 'Quantidade de produtos diferentes',
    indice_diversificacao DECIMAL(8,4) COMMENT 'Índice HHI de diversificação',

    -- Digital/Engajamento
    qtd_acessos_app_mes INT COMMENT 'Acessos ao app no mês',
    qtd_acessos_homebroker_mes INT COMMENT 'Acessos ao homebroker no mês',
    tempo_medio_sessao_minutos DECIMAL(10,2) COMMENT 'Tempo médio de sessão',
    flag_usa_canal_digital BOOLEAN COMMENT 'Usa canais digitais',

    -- =======================================
    -- MÉTRICAS DE CRM E OPORTUNIDADES
    -- =======================================
    -- Fluxo de Oportunidades
    qtd_oportunidades_criadas_mes INT COMMENT 'Oportunidades criadas no mês',
    valor_oportunidades_criadas_mes DECIMAL(18,2) COMMENT 'Valor das oportunidades criadas',
    qtd_oportunidades_ganhas_mes INT COMMENT 'Oportunidades ganhas no mês',
    valor_oportunidades_ganhas_mes DECIMAL(18,2) COMMENT 'Valor das oportunidades ganhas',
    qtd_oportunidades_perdidas_mes INT COMMENT 'Oportunidades perdidas no mês',
    valor_oportunidades_perdidas_mes DECIMAL(18,2) COMMENT 'Valor das oportunidades perdidas',

    -- Pipeline Ativo
    qtd_oportunidades_ativas INT COMMENT 'Oportunidades em aberto',
    valor_pipeline_total DECIMAL(18,2) COMMENT 'Valor total do pipeline',
    valor_pipeline_ponderado DECIMAL(18,2) COMMENT 'Pipeline × probabilidade',

    -- Conversão e Eficiência
    taxa_conversao_geral_mes DECIMAL(8,4) COMMENT 'Taxa de conversão no mês',
    tempo_ciclo_medio_dias DECIMAL(10,2) COMMENT 'Tempo médio até conversão',

    -- =======================================
    -- MÉTRICAS DE SATISFAÇÃO E RISCO
    -- =======================================
    -- NPS
    nps_score_atual INT COMMENT 'Score NPS atual',
    nps_categoria VARCHAR(20) COMMENT 'Promotor/Neutro/Detrator',
    data_ultimo_nps DATE COMMENT 'Data da última pesquisa NPS',
    meses_desde_ultimo_nps INT COMMENT 'Meses desde último NPS',

    -- Indicadores de Risco/Churn
    score_risco_churn DECIMAL(8,4) COMMENT 'Score de risco de churn (0-1)',
    probabilidade_churn_30d DECIMAL(8,4) COMMENT 'Probabilidade churn 30 dias',
    probabilidade_churn_90d DECIMAL(8,4) COMMENT 'Probabilidade churn 90 dias',
    flag_reducao_patrimonio BOOLEAN COMMENT 'Reduziu patrimônio >20% em 3m',
    flag_reducao_transacoes BOOLEAN COMMENT 'Reduziu transações >50%',
    flag_cliente_inativo BOOLEAN COMMENT 'Sem transações há 90+ dias',

    -- Indicadores de Propensão a Resgate
    score_propensao_resgate_30d DECIMAL(8,4) COMMENT 'Propensão resgate 30d (0-1)',
    score_propensao_resgate_90d DECIMAL(8,4) COMMENT 'Propensão resgate 90d (0-1)',
    valor_estimado_resgate_30d DECIMAL(18,2) COMMENT 'Valor estimado resgate 30d',
    valor_estimado_resgate_90d DECIMAL(18,2) COMMENT 'Valor estimado resgate 90d',

    -- =======================================
    -- MÉTRICAS COMPARATIVAS E BENCHMARKS
    -- =======================================
    percentil_captacao_liquida_segmento DECIMAL(8,4) COMMENT 'Percentil no segmento',
    percentil_patrimonio_segmento DECIMAL(8,4) COMMENT 'Percentil patrimônio',
    percentil_rentabilidade_segmento DECIMAL(8,4) COMMENT 'Percentil rentabilidade',

    -- Comparação com Metas
    meta_captacao_liquida_mes DECIMAL(18,2) COMMENT 'Meta de captação líquida',
    atingimento_meta_mes_pct DECIMAL(8,4) COMMENT 'Atingimento da meta %',
    meta_captacao_liquida_ano DECIMAL(18,2) COMMENT 'Meta anual',
    atingimento_meta_ano_pct DECIMAL(8,4) COMMENT 'Atingimento meta anual %',

    -- =======================================
    -- FEATURES PARA ML E ANÁLISE PREDITIVA
    -- =======================================
    -- Tendências e Momentum
    tendencia_captacao_3m VARCHAR(20) COMMENT 'Crescente/Estável/Decrescente',
    momentum_score DECIMAL(8,4) COMMENT 'Score de momentum',
    volatilidade_patrimonio_6m DECIMAL(8,4) COMMENT 'Volatilidade do patrimônio',

    -- Life Time Value
    ltv_estimado DECIMAL(18,2) COMMENT 'LTV estimado do cliente',
    ltv_realizado DECIMAL(18,2) COMMENT 'LTV realizado até o momento',
    roi_cliente DECIMAL(8,4) COMMENT 'ROI do cliente',

    -- Clustering e Segmentação
    cluster_comportamental VARCHAR(50) COMMENT 'Cluster comportamental',
    cluster_rentabilidade VARCHAR(50) COMMENT 'Cluster por rentabilidade',
    cluster_produto VARCHAR(50) COMMENT 'Cluster por produto',

    -- Flags de Eventos
    flag_primeiro_aporte_mes BOOLEAN COMMENT 'Primeiro aporte no mês',
    flag_aniversario_conta_mes BOOLEAN COMMENT 'Aniversário da conta no mês',
    flag_atingiu_private BOOLEAN COMMENT 'Migrou para Private',
    flag_evento_vida_importante BOOLEAN COMMENT 'Evento de vida detectado',

    -- =======================================
    -- METADADOS E CONTROLE
    -- =======================================
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data/hora da carga',
    data_ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Última atualização',
    versao_modelo VARCHAR(20) DEFAULT 'v1.0' COMMENT 'Versão do modelo de dados',
    score_qualidade_dados DECIMAL(8,4) COMMENT 'Score de qualidade (0-1)',
    flag_dados_completos BOOLEAN DEFAULT TRUE COMMENT 'Dados completos',
    observacoes_qualidade TEXT COMMENT 'Observações sobre qualidade',

    -- =======================================
    -- CONSTRAINTS
    -- =======================================
    PRIMARY KEY (cliente_id, data_referencia),
    INDEX idx_data_referencia (data_referencia),
    INDEX idx_segmento_data (cliente_segmento, data_referencia),
    INDEX idx_assessor_data (assessor_id, data_referencia),
    INDEX idx_captacao_liquida (captacao_liquida_mes DESC),
    INDEX idx_score_churn (score_risco_churn DESC),
    INDEX idx_score_resgate (score_propensao_resgate_30d DESC),
    
    -- Check constraints
    CONSTRAINT ck_data_referencia CHECK (data_referencia = LAST_DAY(data_referencia)),
    CONSTRAINT ck_scores CHECK (
        score_risco_churn BETWEEN 0 AND 1 AND
        probabilidade_churn_30d BETWEEN 0 AND 1 AND
        probabilidade_churn_90d BETWEEN 0 AND 1 AND
        score_propensao_resgate_30d BETWEEN 0 AND 1 AND
        score_propensao_resgate_90d BETWEEN 0 AND 1
    )
) 
COMMENT = 'Tabela Gold - Visão 360° do cliente com métricas de captação líquida, comportamento e predições'
PARTITION BY RANGE (YEAR(data_referencia) * 100 + MONTH(data_referencia)) (
    PARTITION p202501 VALUES LESS THAN (202502),
    PARTITION p202502 VALUES LESS THAN (202503),
    PARTITION p202503 VALUES LESS THAN (202504),
    PARTITION p202504 VALUES LESS THAN (202505),
    PARTITION p202505 VALUES LESS THAN (202506),
    PARTITION p202506 VALUES LESS THAN (202507),
    PARTITION p202507 VALUES LESS THAN (202508),
    PARTITION p202508 VALUES LESS THAN (202509),
    PARTITION p202509 VALUES LESS THAN (202510),
    PARTITION p202510 VALUES LESS THAN (202511),
    PARTITION p202511 VALUES LESS THAN (202512),
    PARTITION p202512 VALUES LESS THAN (202601)
);