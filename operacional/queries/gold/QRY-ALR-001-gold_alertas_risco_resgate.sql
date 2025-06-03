-- =========================================================================
-- TABELA: gold_alertas_risco_resgate
-- =========================================================================
-- Grain: Cliente × Alerta
-- Propósito: Sistema de alertas proativos para prevenção de resgates
-- Autor: Data Science Team - M7 Investimentos
-- Data Criação: 2025-01-02
-- =========================================================================

CREATE TABLE IF NOT EXISTS gold.gold_alertas_risco_resgate (
    -- =======================================
    -- IDENTIFICAÇÃO DO ALERTA
    -- =======================================
    alerta_id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID único do alerta',
    cliente_id VARCHAR(50) NOT NULL COMMENT 'ID do cliente',
    data_alerta DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Data/hora do alerta',
    
    -- =======================================
    -- INFORMAÇÕES DO CLIENTE
    -- =======================================
    cliente_nome VARCHAR(200) COMMENT 'Nome do cliente',
    cliente_segmento VARCHAR(50) COMMENT 'Segmento do cliente',
    assessor_id VARCHAR(50) COMMENT 'ID do assessor responsável',
    assessor_nome VARCHAR(200) COMMENT 'Nome do assessor',
    escritorio_id VARCHAR(50) COMMENT 'ID do escritório',
    patrimonio_atual DECIMAL(18,2) COMMENT 'Patrimônio atual',
    
    -- =======================================
    -- CLASSIFICAÇÃO DO ALERTA
    -- =======================================
    nivel_alerta VARCHAR(20) NOT NULL COMMENT 'Nível: CRITICO/ALTO/MEDIO/BAIXO',
    score_urgencia DECIMAL(8,4) COMMENT 'Score de urgência (0-1)',
    categoria_principal VARCHAR(100) COMMENT 'Categoria principal do risco',
    tempo_estimado_acao_dias INT COMMENT 'Dias estimados até o resgate',
    
    -- =======================================
    -- GATILHOS ATIVADOS
    -- =======================================
    gatilhos_comportamentais JSON COMMENT 'Lista de gatilhos comportamentais',
    qtd_gatilhos_comportamentais INT DEFAULT 0 COMMENT 'Quantidade de gatilhos comportamentais',
    
    gatilhos_mercado JSON COMMENT 'Lista de gatilhos de mercado',
    qtd_gatilhos_mercado INT DEFAULT 0 COMMENT 'Quantidade de gatilhos de mercado',
    
    gatilhos_vida JSON COMMENT 'Lista de eventos de vida',
    qtd_gatilhos_vida INT DEFAULT 0 COMMENT 'Quantidade de eventos de vida',
    
    gatilhos_operacionais JSON COMMENT 'Lista de gatilhos operacionais',
    qtd_gatilhos_operacionais INT DEFAULT 0 COMMENT 'Quantidade de gatilhos operacionais',
    
    qtd_total_gatilhos INT GENERATED ALWAYS AS (
        qtd_gatilhos_comportamentais + qtd_gatilhos_mercado + 
        qtd_gatilhos_vida + qtd_gatilhos_operacionais
    ) STORED COMMENT 'Total de gatilhos ativados',
    
    -- =======================================
    -- VALORES EM RISCO
    -- =======================================
    valor_risco_estimado DECIMAL(18,2) COMMENT 'Valor estimado em risco',
    valor_risco_minimo DECIMAL(18,2) COMMENT 'Valor mínimo em risco',
    valor_risco_maximo DECIMAL(18,2) COMMENT 'Valor máximo em risco',
    percentual_patrimonio_risco DECIMAL(8,4) COMMENT '% do patrimônio em risco',
    
    -- =======================================
    -- RECOMENDAÇÕES DE AÇÃO
    -- =======================================
    acao_principal VARCHAR(500) NOT NULL COMMENT 'Ação principal recomendada',
    acoes_secundarias JSON COMMENT 'Lista de ações secundárias',
    script_abordagem TEXT COMMENT 'Script sugerido para abordagem',
    produtos_alternativos JSON COMMENT 'Produtos alternativos sugeridos',
    argumentos_retencao JSON COMMENT 'Argumentos para retenção',
    
    -- =======================================
    -- PRIORIZAÇÃO
    -- =======================================
    prioridade_contato INT COMMENT 'Prioridade: 1 (máxima) a 5',
    prazo_contato_horas INT COMMENT 'Prazo recomendado em horas',
    melhor_horario_contato VARCHAR(50) COMMENT 'Melhor horário para contato',
    canal_preferencial VARCHAR(50) COMMENT 'Canal preferencial do cliente',
    
    -- =======================================
    -- GESTÃO DO ALERTA
    -- =======================================
    status_alerta VARCHAR(50) DEFAULT 'ABERTO' COMMENT 'ABERTO/EM_ANDAMENTO/RESOLVIDO/EXPIRADO',
    responsavel_designado VARCHAR(200) COMMENT 'Pessoa designada para o alerta',
    data_designacao DATETIME COMMENT 'Data/hora da designação',
    
    -- =======================================
    -- TRACKING DE AÇÕES
    -- =======================================
    flag_acao_tomada BOOLEAN DEFAULT FALSE COMMENT 'Se alguma ação foi tomada',
    data_primeira_acao DATETIME COMMENT 'Data/hora da primeira ação',
    tipo_primeira_acao VARCHAR(100) COMMENT 'Tipo da primeira ação',
    
    qtd_tentativas_contato INT DEFAULT 0 COMMENT 'Tentativas de contato',
    data_ultimo_contato DATETIME COMMENT 'Data/hora do último contato',
    
    -- =======================================
    -- RESULTADO DO ALERTA
    -- =======================================
    resultado VARCHAR(200) COMMENT 'Resultado: RETIDO/PARCIAL/NAO_RETIDO/EXPIRADO',
    detalhes_resultado TEXT COMMENT 'Detalhes do resultado',
    
    valor_retido DECIMAL(18,2) COMMENT 'Valor efetivamente retido',
    percentual_retido DECIMAL(8,4) COMMENT '% do valor em risco retido',
    
    flag_sucesso BOOLEAN COMMENT 'Se a ação foi bem sucedida',
    motivo_insucesso VARCHAR(500) COMMENT 'Motivo do insucesso, se aplicável',
    
    -- =======================================
    -- APRENDIZADO
    -- =======================================
    eficacia_acao_principal DECIMAL(8,4) COMMENT 'Eficácia da ação principal (0-1)',
    feedback_assessor TEXT COMMENT 'Feedback do assessor',
    licoes_aprendidas TEXT COMMENT 'Lições aprendidas',
    
    -- =======================================
    -- INTEGRAÇÃO COM OUTRAS TABELAS
    -- =======================================
    pipeline_resgate_id VARCHAR(50) COMMENT 'ID relacionado em gold_pipeline_resgates_previstos',
    forecast_id VARCHAR(50) COMMENT 'ID relacionado em gold_forecast_captacao_liquida',
    
    -- =======================================
    -- METADADOS
    -- =======================================
    origem_alerta VARCHAR(100) COMMENT 'Sistema/modelo que gerou o alerta',
    versao_modelo VARCHAR(50) COMMENT 'Versão do modelo',
    confidence_score DECIMAL(8,4) COMMENT 'Confiança do alerta (0-1)',
    
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data de criação',
    data_ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Última atualização',
    data_expiracao DATETIME COMMENT 'Data de expiração do alerta',
    
    -- =======================================
    -- CONSTRAINTS
    -- =======================================
    INDEX idx_cliente_data (cliente_id, data_alerta DESC),
    INDEX idx_nivel_status (nivel_alerta, status_alerta),
    INDEX idx_assessor_prioridade (assessor_id, prioridade_contato),
    INDEX idx_valor_risco (valor_risco_estimado DESC),
    INDEX idx_data_expiracao (data_expiracao, status_alerta),
    INDEX idx_resultado (resultado, flag_sucesso),
    
    -- Check constraints
    CONSTRAINT ck_nivel_alerta CHECK (
        nivel_alerta IN ('CRITICO', 'ALTO', 'MEDIO', 'BAIXO')
    ),
    CONSTRAINT ck_status_alerta CHECK (
        status_alerta IN ('ABERTO', 'EM_ANDAMENTO', 'RESOLVIDO', 'EXPIRADO')
    ),
    CONSTRAINT ck_scores_alerta CHECK (
        score_urgencia BETWEEN 0 AND 1 AND
        confidence_score BETWEEN 0 AND 1 AND
        eficacia_acao_principal BETWEEN 0 AND 1
    )
) 
COMMENT = 'Tabela Gold - Sistema de alertas proativos para prevenção de resgates com tracking completo'
PARTITION BY RANGE (YEAR(data_alerta) * 100 + MONTH(data_alerta)) (
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
-- PROCEDURES PARA GESTÃO DE ALERTAS
-- =========================================================================

DELIMITER $$

-- Procedure para criar alertas automáticos
CREATE PROCEDURE sp_criar_alerta_risco_resgate(
    IN p_cliente_id VARCHAR(50),
    IN p_nivel_alerta VARCHAR(20),
    IN p_valor_risco DECIMAL(18,2),
    IN p_acao_principal VARCHAR(500),
    IN p_gatilhos_json JSON
)
BEGIN
    DECLARE v_patrimonio DECIMAL(18,2);
    DECLARE v_assessor_id VARCHAR(50);
    
    -- Buscar informações do cliente
    SELECT patrimonio_atual, assessor_id 
    INTO v_patrimonio, v_assessor_id
    FROM gold_captacao_liquida_cliente
    WHERE cliente_id = p_cliente_id
    ORDER BY data_referencia DESC
    LIMIT 1;
    
    -- Inserir alerta
    INSERT INTO gold_alertas_risco_resgate (
        cliente_id,
        nivel_alerta,
        valor_risco_estimado,
        percentual_patrimonio_risco,
        acao_principal,
        gatilhos_comportamentais,
        prioridade_contato,
        prazo_contato_horas,
        assessor_id,
        patrimonio_atual
    ) VALUES (
        p_cliente_id,
        p_nivel_alerta,
        p_valor_risco,
        p_valor_risco / NULLIF(v_patrimonio, 0),
        p_acao_principal,
        p_gatilhos_json,
        CASE p_nivel_alerta
            WHEN 'CRITICO' THEN 1
            WHEN 'ALTO' THEN 2
            WHEN 'MEDIO' THEN 3
            ELSE 4
        END,
        CASE p_nivel_alerta
            WHEN 'CRITICO' THEN 4
            WHEN 'ALTO' THEN 24
            WHEN 'MEDIO' THEN 72
            ELSE 168
        END,
        v_assessor_id,
        v_patrimonio
    );
    
    -- Notificar assessor (integração com sistema de notificações)
    -- CALL sp_notificar_assessor(v_assessor_id, LAST_INSERT_ID());
    
END$$

-- Procedure para atualizar status do alerta
CREATE PROCEDURE sp_atualizar_status_alerta(
    IN p_alerta_id BIGINT,
    IN p_status VARCHAR(50),
    IN p_resultado VARCHAR(200),
    IN p_valor_retido DECIMAL(18,2)
)
BEGIN
    UPDATE gold_alertas_risco_resgate
    SET 
        status_alerta = p_status,
        resultado = p_resultado,
        valor_retido = p_valor_retido,
        percentual_retido = valor_retido / NULLIF(valor_risco_estimado, 0),
        flag_sucesso = CASE WHEN p_valor_retido > 0 THEN TRUE ELSE FALSE END,
        data_ultima_atualizacao = NOW()
    WHERE alerta_id = p_alerta_id;
END$$

DELIMITER ;

-- =========================================================================
-- VIEWS PARA ANÁLISE
-- =========================================================================

-- View de alertas pendentes por assessor
CREATE OR REPLACE VIEW vw_alertas_pendentes_assessor AS
SELECT 
    assessor_id,
    assessor_nome,
    COUNT(DISTINCT CASE WHEN nivel_alerta = 'CRITICO' THEN alerta_id END) as qtd_alertas_criticos,
    COUNT(DISTINCT CASE WHEN nivel_alerta = 'ALTO' THEN alerta_id END) as qtd_alertas_alto,
    COUNT(DISTINCT CASE WHEN nivel_alerta = 'MEDIO' THEN alerta_id END) as qtd_alertas_medio,
    COUNT(DISTINCT alerta_id) as total_alertas_abertos,
    SUM(valor_risco_estimado) as valor_total_em_risco,
    MIN(prazo_contato_horas) as menor_prazo_horas
FROM gold_alertas_risco_resgate
WHERE status_alerta IN ('ABERTO', 'EM_ANDAMENTO')
    AND (data_expiracao IS NULL OR data_expiracao > NOW())
GROUP BY assessor_id, assessor_nome
ORDER BY qtd_alertas_criticos DESC, valor_total_em_risco DESC;

-- View de eficácia das ações
CREATE OR REPLACE VIEW vw_eficacia_acoes_retencao AS
SELECT 
    acao_principal,
    nivel_alerta,
    COUNT(*) as qtd_alertas,
    SUM(CASE WHEN flag_sucesso = TRUE THEN 1 ELSE 0 END) as qtd_sucessos,
    AVG(percentual_retido) as percentual_retido_medio,
    SUM(valor_retido) as valor_total_retido,
    AVG(eficacia_acao_principal) as eficacia_media
FROM gold_alertas_risco_resgate
WHERE status_alerta = 'RESOLVIDO'
    AND resultado IS NOT NULL
GROUP BY acao_principal, nivel_alerta
ORDER BY eficacia_media DESC;
