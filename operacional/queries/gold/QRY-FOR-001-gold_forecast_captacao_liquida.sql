-- =========================================================================
-- TABELA: gold_forecast_captacao_liquida
-- =========================================================================
-- Grain: Cliente × Período de Forecast
-- Propósito: Consolidar previsões integradas de captação e resgate com cenários
-- Autor: Data Science Team - M7 Investimentos
-- Data Criação: 2025-01-02
-- =========================================================================

CREATE TABLE IF NOT EXISTS gold.gold_forecast_captacao_liquida (
    -- =======================================
    -- IDENTIFICAÇÃO PRIMÁRIA
    -- =======================================
    forecast_id VARCHAR(50) NOT NULL PRIMARY KEY COMMENT 'ID único do forecast',
    cliente_id VARCHAR(50) NOT NULL COMMENT 'ID do cliente',
    data_referencia DATE NOT NULL COMMENT 'Data base do forecast',
    periodo_forecast VARCHAR(20) NOT NULL COMMENT 'Período: 30d/60d/90d/6m/12m',
    data_inicio_periodo DATE NOT NULL COMMENT 'Início do período previsto',
    data_fim_periodo DATE NOT NULL COMMENT 'Fim do período previsto',
    
    -- =======================================
    -- INFORMAÇÕES DO CLIENTE
    -- =======================================
    cliente_nome VARCHAR(200) COMMENT 'Nome do cliente',
    cliente_segmento VARCHAR(50) COMMENT 'Segmento do cliente',
    assessor_id VARCHAR(50) COMMENT 'ID do assessor responsável',
    assessor_nome VARCHAR(200) COMMENT 'Nome do assessor',
    escritorio_id VARCHAR(50) COMMENT 'ID do escritório',
    patrimonio_base DECIMAL(18,2) COMMENT 'Patrimônio na data base',
    
    -- =======================================
    -- FORECAST DE CAPTAÇÃO (ORIGEM: CRM)
    -- =======================================
    captacao_prevista_crm DECIMAL(18,2) DEFAULT 0 COMMENT 'Captação prevista via CRM',
    captacao_pipeline_qualificado DECIMAL(18,2) DEFAULT 0 COMMENT 'Pipeline qualificado',
    captacao_pipeline_provavel DECIMAL(18,2) DEFAULT 0 COMMENT 'Pipeline com alta probabilidade',
    
    probabilidade_captacao_crm DECIMAL(8,4) COMMENT 'Probabilidade de captação (CRM)',
    confianca_forecast_crm DECIMAL(8,4) COMMENT 'Nível de confiança do forecast CRM',
    
    qtd_oportunidades_pipeline INT DEFAULT 0 COMMENT 'Oportunidades no pipeline',
    valor_ticket_medio_esperado DECIMAL(18,2) COMMENT 'Ticket médio esperado',
    
    -- Detalhamento por produto
    captacao_prevista_rf DECIMAL(18,2) DEFAULT 0 COMMENT 'Captação prevista renda fixa',
    captacao_prevista_rv DECIMAL(18,2) DEFAULT 0 COMMENT 'Captação prevista renda variável',
    captacao_prevista_fundos DECIMAL(18,2) DEFAULT 0 COMMENT 'Captação prevista fundos',
    captacao_prevista_previdencia DECIMAL(18,2) DEFAULT 0 COMMENT 'Captação prevista previdência',
    
    -- =======================================
    -- FORECAST DE RESGATE (ORIGEM: ML)
    -- =======================================
    resgate_previsto_ml DECIMAL(18,2) DEFAULT 0 COMMENT 'Resgate previsto via ML',
    resgate_cenario_otimista DECIMAL(18,2) DEFAULT 0 COMMENT 'Cenário otimista de resgate',
    resgate_cenario_pessimista DECIMAL(18,2) DEFAULT 0 COMMENT 'Cenário pessimista de resgate',
    
    probabilidade_resgate_ml DECIMAL(8,4) COMMENT 'Probabilidade de resgate (ML)',
    confianca_forecast_ml DECIMAL(8,4) COMMENT 'Nível de confiança do modelo ML',
    
    score_propensao_resgate DECIMAL(8,4) COMMENT 'Score de propensão a resgate',
    categoria_risco VARCHAR(20) COMMENT 'BAIXO/MEDIO/ALTO/CRITICO',
    
    -- Drivers do resgate
    drivers_resgate JSON COMMENT 'Principais fatores de risco identificados',
    peso_mercado DECIMAL(8,4) COMMENT 'Peso dos fatores de mercado',
    peso_comportamental DECIMAL(8,4) COMMENT 'Peso dos fatores comportamentais',
    peso_relacionamento DECIMAL(8,4) COMMENT 'Peso dos fatores de relacionamento',
    
    -- =======================================
    -- CAPTAÇÃO LÍQUIDA CONSOLIDADA
    -- =======================================
    captacao_liquida_prevista DECIMAL(18,2) COMMENT 'Captação líquida prevista (base)',
    captacao_liquida_otimista DECIMAL(18,2) COMMENT 'Cenário otimista',
    captacao_liquida_realista DECIMAL(18,2) COMMENT 'Cenário realista',
    captacao_liquida_pessimista DECIMAL(18,2) COMMENT 'Cenário pessimista',
    
    patrimonio_previsto DECIMAL(18,2) COMMENT 'Patrimônio previsto ao final',
    patrimonio_otimista DECIMAL(18,2) COMMENT 'Patrimônio cenário otimista',
    patrimonio_pessimista DECIMAL(18,2) COMMENT 'Patrimônio cenário pessimista',
    
    variacao_patrimonio_esperada DECIMAL(18,2) COMMENT 'Variação esperada do patrimônio',
    variacao_patrimonio_esperada_pct DECIMAL(8,4) COMMENT 'Variação esperada %',
    
    -- =======================================
    -- CENÁRIOS E SIMULAÇÕES
    -- =======================================
    cenario_mercado VARCHAR(50) COMMENT 'Cenário de mercado assumido',
    cdi_esperado_periodo DECIMAL(8,4) COMMENT 'CDI esperado para o período',
    ibov_esperado_periodo DECIMAL(8,4) COMMENT 'Ibovespa esperado para o período',
    
    ajuste_sazonalidade DECIMAL(8,4) COMMENT 'Ajuste por sazonalidade',
    ajuste_eventos_calendario JSON COMMENT 'Ajustes por eventos especiais',
    
    -- Análise de sensibilidade
    sensibilidade_mercado_baixa DECIMAL(18,2) COMMENT 'Impacto se mercado cair 10%',
    sensibilidade_mercado_alta DECIMAL(18,2) COMMENT 'Impacto se mercado subir 10%',
    sensibilidade_taxa_juros DECIMAL(18,2) COMMENT 'Impacto de mudança na Selic',
    
    -- =======================================
    -- VALIDAÇÃO E ACOMPANHAMENTO
    -- =======================================
    forecast_anterior_id VARCHAR(50) COMMENT 'ID do forecast anterior para comparação',
    revisao_numero INT DEFAULT 1 COMMENT 'Número da revisão',
    motivo_revisao VARCHAR(500) COMMENT 'Motivo da revisão, se aplicável',
    
    -- Comparação com forecast anterior
    variacao_vs_forecast_anterior DECIMAL(18,2) COMMENT 'Variação vs forecast anterior',
    variacao_vs_forecast_anterior_pct DECIMAL(8,4) COMMENT 'Variação % vs anterior',
    
    -- Tracking de acurácia (preenchido após período)
    captacao_liquida_realizada_30d DECIMAL(18,2) COMMENT 'Realizado em 30 dias',
    captacao_liquida_realizada_60d DECIMAL(18,2) COMMENT 'Realizado em 60 dias',
    captacao_liquida_realizada_90d DECIMAL(18,2) COMMENT 'Realizado em 90 dias',
    
    erro_absoluto_30d DECIMAL(18,2) COMMENT 'Erro absoluto 30d',
    erro_percentual_30d DECIMAL(8,4) COMMENT 'Erro percentual 30d',
    erro_absoluto_90d DECIMAL(18,2) COMMENT 'Erro absoluto 90d',
    erro_percentual_90d DECIMAL(8,4) COMMENT 'Erro percentual 90d',
    
    flag_forecast_acurado_30d BOOLEAN COMMENT 'Se forecast foi acurado em 30d',
    flag_forecast_acurado_90d BOOLEAN COMMENT 'Se forecast foi acurado em 90d',
    
    -- =======================================
    -- AÇÕES E RECOMENDAÇÕES
    -- =======================================
    acoes_recomendadas JSON COMMENT 'Ações recomendadas para o período',
    prioridade_contato INT COMMENT 'Prioridade de contato (1-5)',
    
    -- Para clientes com risco de resgate
    estrategia_retencao VARCHAR(500) COMMENT 'Estratégia de retenção sugerida',
    produtos_alternativos JSON COMMENT 'Produtos para oferecer',
    argumentos_retencao JSON COMMENT 'Argumentos de retenção personalizados',
    
    -- Para clientes com potencial de captação
    estrategia_captacao VARCHAR(500) COMMENT 'Estratégia de captação sugerida',
    produtos_recomendados JSON COMMENT 'Produtos recomendados',
    momento_ideal_abordagem VARCHAR(100) COMMENT 'Melhor momento para abordagem',
    
    -- =======================================
    -- FEATURES E EXPLICABILIDADE
    -- =======================================
    features_importantes JSON COMMENT 'Top 10 features que influenciaram o forecast',
    explicacao_modelo TEXT COMMENT 'Explicação do resultado do modelo',
    fatores_risco JSON COMMENT 'Principais fatores de risco identificados',
    fatores_oportunidade JSON COMMENT 'Principais fatores de oportunidade',
    
    -- Intervalos de confiança
    intervalo_confianca_inferior DECIMAL(18,2) COMMENT 'Limite inferior (95%)',
    intervalo_confianca_superior DECIMAL(18,2) COMMENT 'Limite superior (95%)',
    desvio_padrao_forecast DECIMAL(18,2) COMMENT 'Desvio padrão da previsão',
    
    -- =======================================
    -- METADADOS E CONTROLE
    -- =======================================
    modelo_captacao_versao VARCHAR(50) COMMENT 'Versão do modelo de captação',
    modelo_resgate_versao VARCHAR(50) COMMENT 'Versão do modelo de resgate',
    algoritmo_utilizado VARCHAR(100) COMMENT 'Algoritmo utilizado (XGBoost, etc)',
    
    data_processamento TIMESTAMP NOT NULL COMMENT 'Data/hora do processamento',
    tempo_processamento_segundos DECIMAL(8,2) COMMENT 'Tempo de processamento',
    
    status_forecast VARCHAR(50) DEFAULT 'ATIVO' COMMENT 'ATIVO/EXPIRADO/REVISADO',
    data_expiracao DATE COMMENT 'Data de expiração do forecast',
    
    origem_dados VARCHAR(100) COMMENT 'Origem dos dados (pipeline, batch, etc)',
    qualidade_dados_score DECIMAL(8,4) COMMENT 'Score de qualidade dos dados',
    
    observacoes TEXT COMMENT 'Observações adicionais',
    
    -- =======================================
    -- ÍNDICES E CONSTRAINTS
    -- =======================================
    INDEX idx_cliente_periodo (cliente_id, periodo_forecast, data_referencia DESC),
    INDEX idx_data_referencia (data_referencia DESC),
    INDEX idx_risco_resgate (categoria_risco, probabilidade_resgate_ml DESC),
    INDEX idx_captacao_prevista (captacao_liquida_prevista DESC),
    INDEX idx_assessor (assessor_id, data_referencia DESC),
    INDEX idx_status (status_forecast, data_referencia DESC),
    
    -- Índices para análise de acurácia
    INDEX idx_acuracia_30d (flag_forecast_acurado_30d, erro_percentual_30d),
    INDEX idx_acuracia_90d (flag_forecast_acurado_90d, erro_percentual_90d),
    
    -- Check constraints
    CONSTRAINT ck_periodo_forecast CHECK (
        periodo_forecast IN ('30d', '60d', '90d', '6m', '12m')
    ),
    CONSTRAINT ck_probabilidades CHECK (
        probabilidade_captacao_crm BETWEEN 0 AND 1 AND
        probabilidade_resgate_ml BETWEEN 0 AND 1 AND
        score_propensao_resgate BETWEEN 0 AND 1 AND
        confianca_forecast_crm BETWEEN 0 AND 1 AND
        confianca_forecast_ml BETWEEN 0 AND 1
    ),
    CONSTRAINT ck_categoria_risco CHECK (
        categoria_risco IN ('BAIXO', 'MEDIO', 'ALTO', 'CRITICO')
    ),
    CONSTRAINT ck_status_forecast CHECK (
        status_forecast IN ('ATIVO', 'EXPIRADO', 'REVISADO')
    ),
    CONSTRAINT ck_datas CHECK (
        data_fim_periodo > data_inicio_periodo AND
        data_inicio_periodo >= data_referencia
    )
) 
COMMENT = 'Tabela Gold - Forecasts integrados de captação líquida com cenários e validação'
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

-- =========================================================================
-- VIEWS PARA ANÁLISE DE FORECASTS
-- =========================================================================

-- View de acurácia dos modelos
CREATE OR REPLACE VIEW vw_acuracia_forecasts AS
SELECT 
    periodo_forecast,
    modelo_captacao_versao,
    modelo_resgate_versao,
    COUNT(*) as qtd_forecasts,
    AVG(ABS(erro_percentual_30d)) as mape_30d,
    AVG(ABS(erro_percentual_90d)) as mape_90d,
    SUM(CASE WHEN flag_forecast_acurado_30d = TRUE THEN 1 ELSE 0 END) / COUNT(*) as taxa_acerto_30d,
    SUM(CASE WHEN flag_forecast_acurado_90d = TRUE THEN 1 ELSE 0 END) / COUNT(*) as taxa_acerto_90d,
    AVG(confianca_forecast_crm) as confianca_media_crm,
    AVG(confianca_forecast_ml) as confianca_media_ml
FROM gold_forecast_captacao_liquida
WHERE captacao_liquida_realizada_30d IS NOT NULL
GROUP BY periodo_forecast, modelo_captacao_versao, modelo_resgate_versao;

-- View de forecasts ativos por risco
CREATE OR REPLACE VIEW vw_forecasts_ativos_risco AS
SELECT 
    categoria_risco,
    COUNT(*) as qtd_clientes,
    SUM(resgate_previsto_ml) as valor_total_risco,
    AVG(probabilidade_resgate_ml) as prob_media_resgate,
    SUM(captacao_liquida_prevista) as captacao_liquida_total_prevista
FROM gold_forecast_captacao_liquida
WHERE status_forecast = 'ATIVO'
    AND data_referencia = CURRENT_DATE
GROUP BY categoria_risco
ORDER BY FIELD(categoria_risco, 'CRITICO', 'ALTO', 'MEDIO', 'BAIXO');