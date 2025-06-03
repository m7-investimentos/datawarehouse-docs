-- =========================================================================
-- TABELA: gold_pipeline_resgates_previstos
-- =========================================================================
-- Grain: Cliente × Data de Previsão
-- Propósito: "CRM Reverso" para prever e prevenir resgates antes que aconteçam
-- Autor: Data Science Team - M7 Investimentos
-- Data Criação: 2025-01-02
-- =========================================================================

CREATE TABLE IF NOT EXISTS gold.gold_pipeline_resgates_previstos (
    -- =======================================
    -- IDENTIFICAÇÃO PRIMÁRIA
    -- =======================================
    pipeline_id VARCHAR(50) NOT NULL PRIMARY KEY COMMENT 'ID único da previsão',
    cliente_id VARCHAR(50) NOT NULL COMMENT 'ID do cliente',
    data_previsao DATE NOT NULL COMMENT 'Data da previsão',
    data_resgate_estimada DATE NOT NULL COMMENT 'Data estimada do resgate',
    
    -- =======================================
    -- INFORMAÇÕES DO CLIENTE
    -- =======================================
    cliente_nome VARCHAR(200) COMMENT 'Nome do cliente',
    cliente_segmento VARCHAR(50) COMMENT 'Segmento do cliente',
    assessor_id VARCHAR(50) COMMENT 'ID do assessor responsável',
    assessor_nome VARCHAR(200) COMMENT 'Nome do assessor',
    escritorio_id VARCHAR(50) COMMENT 'ID do escritório',
    patrimonio_atual DECIMAL(18,2) COMMENT 'Patrimônio atual do cliente',
    
    -- =======================================
    -- CLASSIFICAÇÃO DE RISCO
    -- =======================================
    categoria_risco VARCHAR(20) NOT NULL COMMENT 'IMINENTE/ALTO/MEDIO/BAIXO',
    score_risco_geral DECIMAL(8,4) NOT NULL COMMENT 'Score geral de risco (0-1)',
    probabilidade_resgate DECIMAL(8,4) NOT NULL COMMENT 'Probabilidade de resgate',
    
    dias_para_resgate_estimado INT COMMENT 'Dias estimados até o resgate',
    janela_tempo_acao VARCHAR(50) COMMENT 'Janela para ação: URGENTE/NORMAL/PREVENTIVA',
    
    -- =======================================
    -- VALORES EM RISCO
    -- =======================================
    valor_provavel_resgate DECIMAL(18,2) NOT NULL COMMENT 'Valor provável de resgate',
    valor_minimo_resgate DECIMAL(18,2) COMMENT 'Valor mínimo estimado',
    valor_maximo_resgate DECIMAL(18,2) COMMENT 'Valor máximo estimado',
    
    percentual_patrimonio_risco DECIMAL(8,4) COMMENT '% do patrimônio em risco',
    
    -- Breakdown por produto
    valor_risco_renda_fixa DECIMAL(18,2) DEFAULT 0 COMMENT 'Valor em risco - RF',
    valor_risco_renda_variavel DECIMAL(18,2) DEFAULT 0 COMMENT 'Valor em risco - RV',
    valor_risco_fundos DECIMAL(18,2) DEFAULT 0 COMMENT 'Valor em risco - Fundos',
    valor_risco_previdencia DECIMAL(18,2) DEFAULT 0 COMMENT 'Valor em risco - Previdência',
    
    -- =======================================
    -- DRIVERS DE RISCO (FEATURES PRINCIPAIS)
    -- =======================================
    
    -- Comportamentais
    score_comportamental DECIMAL(8,4) COMMENT 'Score de fatores comportamentais',
    reducao_transacoes_pct DECIMAL(8,4) COMMENT 'Redução % nas transações',
    dias_sem_acesso_app INT COMMENT 'Dias sem acessar app',
    reducao_patrimonio_3m_pct DECIMAL(8,4) COMMENT 'Redução patrimônio 3m %',
    
    -- Relacionamento
    score_relacionamento DECIMAL(8,4) COMMENT 'Score de relacionamento',
    dias_sem_contato_assessor INT COMMENT 'Dias sem contato com assessor',
    qtd_reclamacoes_6m INT COMMENT 'Reclamações últimos 6 meses',
    nps_score_atual INT COMMENT 'Score NPS atual',
    
    -- Performance e Mercado
    score_performance DECIMAL(8,4) COMMENT 'Score de performance da carteira',
    rentabilidade_vs_benchmark DECIMAL(8,4) COMMENT 'Performance vs benchmark',
    volatilidade_carteira DECIMAL(8,4) COMMENT 'Volatilidade da carteira',
    drawdown_maximo_6m DECIMAL(8,4) COMMENT 'Maior queda nos últimos 6m',
    
    -- Eventos de Vida
    score_eventos_vida DECIMAL(8,4) COMMENT 'Score de eventos de vida',
    flag_mudanca_emprego BOOLEAN DEFAULT FALSE COMMENT 'Mudança de emprego detectada',
    flag_evento_familiar BOOLEAN DEFAULT FALSE COMMENT 'Evento familiar detectado',
    flag_necessidade_liquidez BOOLEAN DEFAULT FALSE COMMENT 'Necessidade de liquidez',
    
    -- =======================================
    -- DRIVERS DETALHADOS (JSON)
    -- =======================================
    drivers_principais JSON COMMENT 'Top 5 drivers que influenciam o risco',
    sinais_comportamentais JSON COMMENT 'Sinais comportamentais detectados',
    alertas_sistema JSON COMMENT 'Alertas automáticos gerados',
    historico_interacoes JSON COMMENT 'Histórico de interações recentes',
    
    -- =======================================
    -- ESTRATÉGIA E AÇÕES RECOMENDADAS
    -- =======================================
    estrategia_retencao VARCHAR(20) COMMENT 'PROATIVA/REATIVA/PREVENTIVA',
    acao_principal VARCHAR(500) NOT NULL COMMENT 'Ação principal recomendada',
    acoes_secundarias JSON COMMENT 'Lista de ações secundárias',
    
    -- Priorização
    prioridade_acao INT COMMENT 'Prioridade: 1 (máxima) a 5 (mínima)',
    prazo_acao_dias INT COMMENT 'Prazo recomendado para ação (dias)',
    
    -- Abordagem personalizada
    script_abordagem TEXT COMMENT 'Script sugerido para abordagem',
    produtos_alternativos JSON COMMENT 'Produtos alternativos a oferecer',
    argumentos_retencao JSON COMMENT 'Argumentos personalizados de retenção',
    
    melhor_canal_contato VARCHAR(50) COMMENT 'Canal preferencial do cliente',
    melhor_horario_contato VARCHAR(50) COMMENT 'Melhor horário para contato',
    
    -- =======================================
    -- ACOMPANHAMENTO E RESULTADOS
    -- =======================================
    status_previsao VARCHAR(50) DEFAULT 'ATIVA' COMMENT 'ATIVA/EXPIRADA/CONVERTIDA/FALSO_POSITIVO',
    
    -- Ações tomadas
    flag_acao_tomada BOOLEAN DEFAULT FALSE COMMENT 'Se alguma ação foi executada',
    data_primeira_acao DATE COMMENT 'Data da primeira ação',
    tipo_primeira_acao VARCHAR(100) COMMENT 'Tipo da primeira ação tomada',
    responsavel_acao VARCHAR(200) COMMENT 'Responsável pela ação',
    
    qtd_tentativas_contato INT DEFAULT 0 COMMENT 'Tentativas de contato',
    qtd_reunioes_realizadas INT DEFAULT 0 COMMENT 'Reuniões realizadas',
    
    -- Resultado final
    resultado_final VARCHAR(50) COMMENT 'RETIDO/PERDA_PARCIAL/PERDA_TOTAL/EM_ANDAMENTO',
    data_resultado_final DATE COMMENT 'Data do resultado final',
    
    valor_efetivamente_resgatado DECIMAL(18,2) COMMENT 'Valor que foi resgatado',
    valor_retido DECIMAL(18,2) COMMENT 'Valor efetivamente retido',
    percentual_retencao DECIMAL(8,4) COMMENT '% do valor em risco que foi retido',
    
    -- =======================================
    -- LEARNING E FEEDBACK
    -- =======================================
    eficacia_acao_principal DECIMAL(8,4) COMMENT 'Eficácia da ação principal (0-1)',
    motivo_insucesso VARCHAR(500) COMMENT 'Motivo do insucesso, se aplicável',
    feedback_assessor TEXT COMMENT 'Feedback do assessor sobre a abordagem',
    licoes_aprendidas TEXT COMMENT 'Lições aprendidas para casos similares',
    
    -- Satisfação pós-ação
    nps_pos_acao INT COMMENT 'NPS do cliente após as ações',
    variacao_nps INT COMMENT 'Variação do NPS pós-ação',
    
    -- =======================================
    -- MODELO E EXPLICABILIDADE
    -- =======================================
    modelo_versao VARCHAR(50) COMMENT 'Versão do modelo preditivo',
    algoritmo_utilizado VARCHAR(100) COMMENT 'Algoritmo usado (XGBoost, etc)',
    confianca_modelo DECIMAL(8,4) COMMENT 'Nível de confiança do modelo',
    
    features_importantes JSON COMMENT 'Features mais importantes para a predição',
    explicacao_previsao TEXT COMMENT 'Explicação da predição para o assessor',
    
    shap_values JSON COMMENT 'SHAP values para explicabilidade',
    intervalo_confianca_superior DECIMAL(18,2) COMMENT 'Limite superior IC 95%',
    intervalo_confianca_inferior DECIMAL(18,2) COMMENT 'Limite inferior IC 95%',
    
    -- =======================================
    -- MONITORAMENTO E ALERTAS
    -- =======================================
    flag_alerta_critico BOOLEAN DEFAULT FALSE COMMENT 'Se gerou alerta crítico',
    flag_alerta_enviado BOOLEAN DEFAULT FALSE COMMENT 'Se alerta foi enviado',
    data_envio_alerta TIMESTAMP COMMENT 'Data/hora do envio do alerta',
    
    canais_notificacao JSON COMMENT 'Canais onde foi enviada notificação',
    
    -- Escalação
    flag_escalado_gerencia BOOLEAN DEFAULT FALSE COMMENT 'Se foi escalado para gerência',
    motivo_escalacao VARCHAR(500) COMMENT 'Motivo da escalação',
    data_escalacao TIMESTAMP COMMENT 'Data/hora da escalação',
    
    -- =======================================
    -- COMPARAÇÃO TEMPORAL
    -- =======================================
    previsao_anterior_id VARCHAR(50) COMMENT 'ID da previsão anterior',
    variacao_score_risco DECIMAL(8,4) COMMENT 'Variação do score desde previsão anterior',
    variacao_valor_risco DECIMAL(18,2) COMMENT 'Variação do valor em risco',
    
    tendencia_risco VARCHAR(20) COMMENT 'CRESCENTE/ESTAVEL/DECRESCENTE',
    velocidade_deterioracao DECIMAL(8,4) COMMENT 'Taxa de deterioração do relacionamento',
    
    -- =======================================
    -- METADADOS E CONTROLE
    -- =======================================
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data de criação da previsão',
    data_ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Última atualização',
    data_expiracao DATE COMMENT 'Data de expiração da previsão',
    
    origem_dados VARCHAR(100) COMMENT 'Origem dos dados (batch, real-time, etc)',
    qualidade_dados_score DECIMAL(8,4) COMMENT 'Score de qualidade dos dados',
    
    observacoes TEXT COMMENT 'Observações adicionais',
    
    -- =======================================
    -- ÍNDICES E CONSTRAINTS
    -- =======================================
    INDEX idx_cliente_data (cliente_id, data_previsao DESC),
    INDEX idx_categoria_risco (categoria_risco, score_risco_geral DESC),
    INDEX idx_valor_risco (valor_provavel_resgate DESC),
    INDEX idx_assessor (assessor_id, data_previsao DESC),
    INDEX idx_status (status_previsao, data_previsao DESC),
    INDEX idx_acao (flag_acao_tomada, prioridade_acao),
    INDEX idx_prazo_acao (prazo_acao_dias, data_previsao),
    INDEX idx_alertas (flag_alerta_critico, flag_alerta_enviado),
    
    -- Check constraints
    CONSTRAINT ck_categoria_risco CHECK (
        categoria_risco IN ('IMINENTE', 'ALTO', 'MEDIO', 'BAIXO')
    ),
    CONSTRAINT ck_scores CHECK (
        score_risco_geral BETWEEN 0 AND 1 AND
        probabilidade_resgate BETWEEN 0 AND 1 AND
        confianca_modelo BETWEEN 0 AND 1
    ),
    CONSTRAINT ck_percentuais CHECK (
        percentual_patrimonio_risco BETWEEN 0 AND 1 AND
        percentual_retencao BETWEEN 0 AND 1
    ),
    CONSTRAINT ck_prioridade CHECK (
        prioridade_acao BETWEEN 1 AND 5
    ),
    CONSTRAINT ck_status_previsao CHECK (
        status_previsao IN ('ATIVA', 'EXPIRADA', 'CONVERTIDA', 'FALSO_POSITIVO')
    ),
    CONSTRAINT ck_janela_tempo CHECK (
        janela_tempo_acao IN ('URGENTE', 'NORMAL', 'PREVENTIVA')
    ),
    CONSTRAINT ck_resultado_final CHECK (
        resultado_final IN ('RETIDO', 'PERDA_PARCIAL', 'PERDA_TOTAL', 'EM_ANDAMENTO')
    )
) 
COMMENT = 'Tabela Gold - Pipeline de resgates previstos com ações e acompanhamento'
PARTITION BY RANGE (YEAR(data_previsao) * 100 + MONTH(data_previsao)) (
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
-- VIEWS PARA ANÁLISE DO PIPELINE
-- =========================================================================

-- View de pipeline ativo por assessor
CREATE OR REPLACE VIEW vw_pipeline_ativo_assessor AS
SELECT 
    assessor_id,
    assessor_nome,
    COUNT(*) as qtd_clientes_risco,
    SUM(valor_provavel_resgate) as valor_total_risco,
    AVG(score_risco_geral) as score_medio_risco,
    SUM(CASE WHEN categoria_risco = 'IMINENTE' THEN valor_provavel_resgate ELSE 0 END) as valor_risco_iminente,
    SUM(CASE WHEN categoria_risco = 'ALTO' THEN valor_provavel_resgate ELSE 0 END) as valor_risco_alto,
    COUNT(CASE WHEN flag_acao_tomada = FALSE AND prazo_acao_dias <= 7 THEN 1 END) as urgentes_sem_acao
FROM gold_pipeline_resgates_previstos
WHERE status_previsao = 'ATIVA'
    AND data_previsao >= CURRENT_DATE - INTERVAL 30 DAY
GROUP BY assessor_id, assessor_nome
ORDER BY valor_total_risco DESC;

-- View de eficácia das ações de retenção
CREATE OR REPLACE VIEW vw_eficacia_retencao AS
SELECT 
    categoria_risco,
    acao_principal,
    COUNT(*) as qtd_casos,
    SUM(CASE WHEN resultado_final = 'RETIDO' THEN 1 ELSE 0 END) as qtd_retidos,
    AVG(percentual_retencao) as percentual_retencao_medio,
    SUM(valor_retido) as valor_total_retido,
    AVG(eficacia_acao_principal) as eficacia_media
FROM gold_pipeline_resgates_previstos
WHERE resultado_final IS NOT NULL
    AND data_resultado_final >= CURRENT_DATE - INTERVAL 90 DAY
GROUP BY categoria_risco, acao_principal
ORDER BY eficacia_media DESC;

-- View de alertas críticos pendentes
CREATE OR REPLACE VIEW vw_alertas_criticos_pendentes AS
SELECT 
    p.pipeline_id,
    p.cliente_nome,
    p.assessor_nome,
    p.valor_provavel_resgate,
    p.dias_para_resgate_estimado,
    p.acao_principal,
    p.prioridade_acao,
    DATEDIFF(CURRENT_DATE, p.data_previsao) as dias_desde_previsao
FROM gold_pipeline_resgates_previstos p
WHERE p.status_previsao = 'ATIVA'
    AND p.flag_alerta_critico = TRUE
    AND p.flag_acao_tomada = FALSE
    AND p.prazo_acao_dias <= 7
ORDER BY p.prioridade_acao, p.valor_provavel_resgate DESC;