-- ============================================================================
-- TABELA: gold_forecast_captacao_liquida
-- ============================================================================
-- Grain: Cliente × Período de Forecast
-- Propósito: Consolidar previsões de captação e resgate para forecast integrado
-- Autor: Data Science Team - M7 Investimentos
-- Data Criação: 2025-01-02
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.gold_forecast_captacao_liquida (
    -- ========================================
    -- IDENTIFICAÇÃO
    -- ========================================
    cliente_id VARCHAR(50) NOT NULL COMMENT 'ID único do cliente',
    data_forecast DATE NOT NULL COMMENT 'Data em que o forecast foi gerado',
    periodo_forecast VARCHAR(20) NOT NULL COMMENT 'Período: 30D/90D/180D/1Y',
    
    -- ========================================
    -- INFORMAÇÕES DO CLIENTE
    -- ========================================
    cliente_nome VARCHAR(200) COMMENT 'Nome do cliente',
    cliente_segmento VARCHAR(50) COMMENT 'Segmento do cliente',
    assessor_id VARCHAR(50) COMMENT 'ID do assessor responsável',
    assessor_nome VARCHAR(200) COMMENT 'Nome do assessor',
    patrimonio_base DECIMAL(18,2) COMMENT 'Patrimônio base para o forecast',
    
    -- ========================================
    -- FORECAST DE CAPTAÇÃO BRUTA
    -- ========================================
    -- 30 dias
    forecast_captacao_bruta_30d DECIMAL(18,2) COMMENT 'Previsão captação bruta 30d',
    intervalo_confianca_captacao_30d_inferior DECIMAL(18,2) COMMENT 'IC inferior 95%',
    intervalo_confianca_captacao_30d_superior DECIMAL(18,2) COMMENT 'IC superior 95%',
    probabilidade_captacao_30d DECIMAL(8,4) COMMENT 'Probabilidade de haver captação',
    
    -- 90 dias
    forecast_captacao_bruta_90d DECIMAL(18,2) COMMENT 'Previsão captação bruta 90d',
    intervalo_confianca_captacao_90d_inferior DECIMAL(18,2) COMMENT 'IC inferior 95%',
    intervalo_confianca_captacao_90d_superior DECIMAL(18,2) COMMENT 'IC superior 95%',
    probabilidade_captacao_90d DECIMAL(8,4) COMMENT 'Probabilidade de haver captação',
    
    -- Origem do Forecast de Captação
    origem_forecast_captacao VARCHAR(100) COMMENT 'CRM/Histórico/Modelo Híbrido',
    qtd_oportunidades_base INT COMMENT 'Quantidade de oportunidades consideradas',
    valor_pipeline_considerado DECIMAL(18,2) COMMENT 'Valor do pipeline considerado',
    
    -- ========================================
    -- FORECAST DE RESGATES
    -- ========================================
    -- 30 dias
    forecast_resgate_30d DECIMAL(18,2) COMMENT 'Previsão de resgates 30d',
    intervalo_confianca_resgate_30d_inferior DECIMAL(18,2) COMMENT 'IC inferior 95%',
    intervalo_confianca_resgate_30d_superior DECIMAL(18,2) COMMENT 'IC superior 95%',
    probabilidade_resgate_30d DECIMAL(8,4) COMMENT 'Probabilidade de haver resgate',
    
    -- 90 dias
    forecast_resgate_90d DECIMAL(18,2) COMMENT 'Previsão de resgates 90d',
    intervalo_confianca_resgate_90d_inferior DECIMAL(18,2) COMMENT 'IC inferior 95%',
    intervalo_confianca_resgate_90d_superior DECIMAL(18,2) COMMENT 'IC superior 95%',
    probabilidade_resgate_90d DECIMAL(8,4) COMMENT 'Probabilidade de haver resgate',
    
    -- Origem do Forecast de Resgate
    origem_forecast_resgate VARCHAR(100) COMMENT 'ML/Survival/Série Temporal',
    score_risco_resgate DECIMAL(8,4) COMMENT 'Score de risco consolidado',
    
    -- ========================================
    -- CAPTAÇÃO LÍQUIDA PREVISTA
    -- ========================================
    -- 30 dias
    forecast_captacao_liquida_30d DECIMAL(18,2) COMMENT 'Captação líquida prevista 30d',
    intervalo_confianca_liquida_30d_inferior DECIMAL(18,2) COMMENT 'IC inferior 95%',
    intervalo_confianca_liquida_30d_superior DECIMAL(18,2) COMMENT 'IC superior 95%',
    probabilidade_captacao_liquida_positiva_30d DECIMAL(8,4) COMMENT 'Prob. captação > resgate',
    
    -- 90 dias
    forecast_captacao_liquida_90d DECIMAL(18,2) COMMENT 'Captação líquida prevista 90d',
    intervalo_confianca_liquida_90d_inferior DECIMAL(18,2) COMMENT 'IC inferior 95%',
    intervalo_confianca_liquida_90d_superior DECIMAL(18,2) COMMENT 'IC superior 95%',
    probabilidade_captacao_liquida_positiva_90d DECIMAL(8,4) COMMENT 'Prob. captação > resgate',
    
    -- ========================================
    -- CENÁRIOS DE ANÁLISE
    -- ========================================
    -- 30 dias
    cenario_otimista_30d DECIMAL(18,2) COMMENT 'Cenário otimista P90',
    cenario_realista_30d DECIMAL(18,2) COMMENT 'Cenário realista P50',
    cenario_pessimista_30d DECIMAL(18,2) COMMENT 'Cenário pessimista P10',
    
    -- 90 dias
    cenario_otimista_90d DECIMAL(18,2) COMMENT 'Cenário otimista P90',
    cenario_realista_90d DECIMAL(18,2) COMMENT 'Cenário realista P50',
    cenario_pessimista_90d DECIMAL(18,2) COMMENT 'Cenário pessimista P10',
    
    -- ========================================
    -- COMPONENTES DO FORECAST
    -- ========================================
    componente_tendencia DECIMAL(18,2) COMMENT 'Componente de tendência',
    componente_sazonalidade DECIMAL(18,2) COMMENT 'Componente sazonal',
    componente_eventos DECIMAL(18,2) COMMENT 'Impacto de eventos especiais',
    componente_mercado DECIMAL(18,2) COMMENT 'Impacto de condições de mercado',
    
    -- ========================================
    -- FATORES DE AJUSTE
    -- ========================================
    fator_ajuste_manual DECIMAL(8,4) DEFAULT 1.0 COMMENT 'Ajuste manual aplicado',
    justificativa_ajuste TEXT COMMENT 'Justificativa para ajuste manual',
    fator_confianca_modelo DECIMAL(8,4) COMMENT 'Confiança no modelo (0-1)',
    
    -- ========================================
    -- ACCURACY HISTÓRICA
    -- ========================================
    mae_forecast_captacao_3m DECIMAL(18,2) COMMENT 'Erro absoluto médio captação 3m',
    mae_forecast_resgate_3m DECIMAL(18,2) COMMENT 'Erro absoluto médio resgate 3m',
    mape_forecast_total_3m DECIMAL(8,4) COMMENT 'Erro percentual médio total 3m',
    
    -- Métricas de Performance do Modelo
    r2_score_captacao DECIMAL(8,4) COMMENT 'R² do modelo de captação',
    r2_score_resgate DECIMAL(8,4) COMMENT 'R² do modelo de resgate',
    dias_desde_ultimo_retrain INT COMMENT 'Dias desde último retreinamento',
    
    -- ========================================
    -- VALIDAÇÃO E REALIZAÇÃO
    -- ========================================
    -- Valores realizados (preenchidos após o período)
    captacao_bruta_realizada_30d DECIMAL(18,2) COMMENT 'Captação bruta real após 30d',
    resgate_realizado_30d DECIMAL(18,2) COMMENT 'Resgate real após 30d',
    captacao_liquida_realizada_30d DECIMAL(18,2) COMMENT 'Captação líquida real após 30d',
    
    captacao_bruta_realizada_90d DECIMAL(18,2) COMMENT 'Captação bruta real após 90d',
    resgate_realizado_90d DECIMAL(18,2) COMMENT 'Resgate real após 90d',
    captacao_liquida_realizada_90d DECIMAL(18,2) COMMENT 'Captação líquida real após 90d',
    
    -- Métricas de Erro
    erro_absoluto_30d DECIMAL(18,2) COMMENT 'Erro absoluto após 30d',
    erro_percentual_30d DECIMAL(8,4) COMMENT 'Erro percentual após 30d',
    erro_absoluto_90d DECIMAL(18,2) COMMENT 'Erro absoluto após 90d',
    erro_percentual_90d DECIMAL(8,4) COMMENT 'Erro percentual após 90d',
    
    -- ========================================
    -- INSIGHTS E RECOMENDAÇÕES
    -- ========================================
    principais_drivers JSON COMMENT 'Principais drivers do forecast',
    riscos_identificados JSON COMMENT 'Riscos identificados',
    oportunidades_identificadas JSON COMMENT 'Oportunidades identificadas',
    acoes_recomendadas JSON COMMENT 'Ações recomendadas baseadas no forecast',
    
    -- ========================================
    -- METADADOS
    -- ========================================
    modelo_versao VARCHAR(50) COMMENT 'Versão do modelo utilizado',
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data de criação',
    data_ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Última atualização',
    flag_forecast_ativo BOOLEAN DEFAULT TRUE COMMENT 'Se o forecast está ativo',
    
    -- ========================================
    -- CONSTRAINTS
    -- ========================================
    PRIMARY KEY (cliente_id, data_forecast, periodo_forecast),
    INDEX idx_data_forecast (data_forecast),
    INDEX idx_assessor_forecast (assessor_id, data_forecast),
    INDEX idx_accuracy (mape_forecast_total_3m),
    INDEX idx_valor_forecast (forecast_captacao_liquida_30d DESC),
    
    -- Check constraints
    CONSTRAINT ck_probabilidades_forecast CHECK (
        probabilidade_captacao_30d BETWEEN 0 AND 1 AND
        probabilidade_captacao_90d BETWEEN 0 AND 1 AND
        probabilidade_resgate_30d BETWEEN 0 AND 1 AND
        probabilidade_resgate_90d BETWEEN 0 AND 1 AND
        probabilidade_captacao_liquida_positiva_30d BETWEEN 0 AND 1 AND
        probabilidade_captacao_liquida_positiva_90d BETWEEN 0 AND 1
    ),
    CONSTRAINT ck_periodo_forecast CHECK (
        periodo_forecast IN ('30D', '90D', '180D', '1Y')
    )
) 
COMMENT = 'Tabela Gold - Forecast integrado de captação líquida com cenários e validação de accuracy'
PARTITION BY RANGE (YEAR(data_forecast) * 100 + MONTH(data_forecast)) (
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

-- ============================================================================
-- VIEW PARA ANÁLISE DE ACCURACY
-- ============================================================================

CREATE OR REPLACE VIEW vw_forecast_accuracy AS
SELECT 
    cliente_id,
    cliente_segmento,
    assessor_id,
    periodo_forecast,
    
    -- Accuracy 30 dias
    AVG(ABS(erro_percentual_30d)) as mape_30d,
    STDDEV(erro_percentual_30d) as desvio_erro_30d,
    SUM(CASE WHEN ABS(erro_percentual_30d) <= 0.1 THEN 1 ELSE 0 END) / COUNT(*) as pct_dentro_10pct_30d,
    
    -- Accuracy 90 dias
    AVG(ABS(erro_percentual_90d)) as mape_90d,
    STDDEV(erro_percentual_90d) as desvio_erro_90d,
    SUM(CASE WHEN ABS(erro_percentual_90d) <= 0.15 THEN 1 ELSE 0 END) / COUNT(*) as pct_dentro_15pct_90d,
    
    -- Viés do modelo
    AVG(erro_percentual_30d) as vies_medio_30d,
    AVG(erro_percentual_90d) as vies_medio_90d,
    
    COUNT(*) as qtd_forecasts
FROM gold_forecast_captacao_liquida
WHERE captacao_liquida_realizada_30d IS NOT NULL
GROUP BY cliente_id, cliente_segmento, assessor_id, periodo_forecast;
