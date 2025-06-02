-- ============================================================================
-- TABELA: gold_pipeline_resgates_previstos
-- ============================================================================
-- Grain: Cliente × Data de Previsão
-- Propósito: "CRM Reverso" para prever e prevenir resgates
-- Autor: Data Science Team - M7 Investimentos
-- Data Criação: 2025-01-02
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.gold_pipeline_resgates_previstos (
    -- ========================================
    -- IDENTIFICAÇÃO
    -- ========================================
    cliente_id VARCHAR(50) NOT NULL COMMENT 'ID único do cliente',
    data_previsao DATE NOT NULL COMMENT 'Data da previsão',
    data_referencia_analise DATE NOT NULL COMMENT 'Data base para análise',
    
    -- ========================================
    -- INFORMAÇÕES DO CLIENTE
    -- ========================================
    cliente_nome VARCHAR(200) COMMENT 'Nome do cliente',
    cliente_segmento VARCHAR(50) COMMENT 'Segmento do cliente',
    assessor_id VARCHAR(50) COMMENT 'ID do assessor responsável',
    assessor_nome VARCHAR(200) COMMENT 'Nome do assessor',
    escritorio_id VARCHAR(50) COMMENT 'ID do escritório',
    patrimonio_atual DECIMAL(18,2) COMMENT 'Patrimônio atual do cliente',
    
    -- ========================================
    -- CLASSIFICAÇÃO DO RISCO
    -- ========================================
    categoria_risco VARCHAR(50) NOT NULL COMMENT 'Classificação: Iminente/Alto/Médio/Baixo',
    nivel_prioridade INT COMMENT 'Nível de prioridade: 1 (máxima) a 5',
    motivo_principal VARCHAR(200) COMMENT 'Principal razão para o risco',
    motivos_secundarios JSON COMMENT 'Lista de motivos adicionais',
    
    -- ========================================
    -- PREVISÕES DE RESGATE
    -- ========================================
    -- 30 dias
    probabilidade_resgate_30d DECIMAL(8,4) COMMENT 'Probabilidade de resgate em 30 dias (0-1)',
    valor_provavel_resgate_30d DECIMAL(18,2) COMMENT 'Valor mais provável de resgate em 30d',
    valor_minimo_resgate_30d DECIMAL(18,2) COMMENT 'Valor mínimo estimado (IC 95%)',
    valor_maximo_resgate_30d DECIMAL(18,2) COMMENT 'Valor máximo estimado (IC 95%)',
    
    -- 90 dias
    probabilidade_resgate_90d DECIMAL(8,4) COMMENT 'Probabilidade de resgate em 90 dias (0-1)',
    valor_provavel_resgate_90d DECIMAL(18,2) COMMENT 'Valor mais provável de resgate em 90d',
    valor_minimo_resgate_90d DECIMAL(18,2) COMMENT 'Valor mínimo estimado 90d (IC 95%)',
    valor_maximo_resgate_90d DECIMAL(18,2) COMMENT 'Valor máximo estimado 90d (IC 95%)',
    
    -- ========================================
    -- GATILHOS IDENTIFICADOS
    -- ========================================
    gatilhos_comportamentais JSON COMMENT 'Gatilhos comportamentais detectados',
    gatilhos_mercado JSON COMMENT 'Gatilhos de mercado detectados',
    gatilhos_vida JSON COMMENT 'Eventos de vida detectados',
    qtd_sinais_alerta INT COMMENT 'Quantidade total de sinais de alerta',
    score_urgencia DECIMAL(8,4) COMMENT 'Score de urgência (0-1)',
    
    -- ========================================
    -- ANÁLISE DE PADRÕES
    -- ========================================
    padrao_resgate_historico VARCHAR(100) COMMENT 'Padrão histórico: Esporádico/Regular/Sazonal',
    ultimo_resgate_dias INT COMMENT 'Dias desde o último resgate',
    valor_medio_resgate_historico DECIMAL(18,2) COMMENT 'Valor médio dos resgates anteriores',
    frequencia_resgate_anual DECIMAL(8,2) COMMENT 'Frequência média de resgates por ano',
    
    -- ========================================
    -- AÇÕES RECOMENDADAS
    -- ========================================
    acao_recomendada VARCHAR(500) COMMENT 'Principal ação recomendada',
    tipo_abordagem_sugerida VARCHAR(200) COMMENT 'Tipo de abordagem: Consultiva/Educativa/Retenção',
    canal_contato_preferencial VARCHAR(50) COMMENT 'Canal preferencial: Telefone/WhatsApp/Presencial',
    produtos_alternativos_sugeridos JSON COMMENT 'Produtos alternativos para oferecer',
    argumentos_retencao JSON COMMENT 'Principais argumentos para retenção',
    
    -- ========================================
    -- TRACKING DE AÇÕES
    -- ========================================
    flag_contactado BOOLEAN DEFAULT FALSE COMMENT 'Cliente foi contactado',
    data_contato DATE COMMENT 'Data do contato',
    hora_contato TIME COMMENT 'Hora do contato',
    responsavel_contato VARCHAR(200) COMMENT 'Quem realizou o contato',
    canal_contato_utilizado VARCHAR(50) COMMENT 'Canal utilizado no contato',
    duracao_contato_minutos INT COMMENT 'Duração do contato em minutos',
    
    -- ========================================
    -- RESULTADO DO CONTATO
    -- ========================================
    resultado_contato VARCHAR(200) COMMENT 'Resultado: Retido/Parcial/Não Retido/Sem Sucesso',
    detalhes_conversa TEXT COMMENT 'Detalhes da conversa',
    flag_resgate_evitado BOOLEAN COMMENT 'Resgate foi evitado',
    flag_resgate_reduzido BOOLEAN COMMENT 'Resgate foi reduzido',
    valor_resgate_evitado DECIMAL(18,2) COMMENT 'Valor de resgate evitado',
    valor_resgate_reduzido DECIMAL(18,2) COMMENT 'Redução no valor do resgate',
    
    -- ========================================
    -- ACOMPANHAMENTO
    -- ========================================
    proxima_acao_data DATE COMMENT 'Data da próxima ação',
    proxima_acao_tipo VARCHAR(200) COMMENT 'Tipo da próxima ação',
    flag_requer_acompanhamento BOOLEAN DEFAULT TRUE COMMENT 'Requer acompanhamento',
    observacoes_acompanhamento TEXT COMMENT 'Observações para acompanhamento',
    
    -- ========================================
    -- MÉTRICAS DO MODELO
    -- ========================================
    modelo_utilizado VARCHAR(100) COMMENT 'Nome/versão do modelo preditivo',
    confidence_score DECIMAL(8,4) COMMENT 'Score de confiança da previsão (0-1)',
    features_mais_importantes JSON COMMENT 'Features mais importantes para a previsão',
    
    -- ========================================
    -- METADADOS
    -- ========================================
    data_criacao_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data de criação',
    data_ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Última atualização',
    status_previsao VARCHAR(50) DEFAULT 'ATIVA' COMMENT 'Status: ATIVA/CONCLUIDA/CANCELADA',
    
    -- ========================================
    -- CONSTRAINTS
    -- ========================================
    PRIMARY KEY (cliente_id, data_previsao),
    INDEX idx_categoria_risco (categoria_risco, nivel_prioridade),
    INDEX idx_probabilidade (probabilidade_resgate_30d DESC),
    INDEX idx_valor_provavel (valor_provavel_resgate_30d DESC),
    INDEX idx_data_previsao (data_previsao),
    INDEX idx_assessor (assessor_id, categoria_risco),
    INDEX idx_status (status_previsao, flag_contactado),
    
    -- Check constraints
    CONSTRAINT ck_probabilidades CHECK (
        probabilidade_resgate_30d BETWEEN 0 AND 1 AND
        probabilidade_resgate_90d BETWEEN 0 AND 1
    ),
    CONSTRAINT ck_categoria_risco CHECK (
        categoria_risco IN ('IMINENTE', 'ALTO', 'MEDIO', 'BAIXO')
    ),
    CONSTRAINT ck_valores_resgate CHECK (
        valor_minimo_resgate_30d <= valor_provavel_resgate_30d AND
        valor_provavel_resgate_30d <= valor_maximo_resgate_30d AND
        valor_minimo_resgate_90d <= valor_provavel_resgate_90d AND
        valor_provavel_resgate_90d <= valor_maximo_resgate_90d
    )
) 
COMMENT = 'Tabela Gold - Pipeline de resgates previstos para ações preventivas e retenção de clientes'
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

-- ============================================================================
-- TRIGGERS PARA AUTOMAÇÃO
-- ============================================================================

DELIMITER $$

CREATE TRIGGER trg_pipeline_resgates_alerta_critico
AFTER INSERT ON gold_pipeline_resgates_previstos
FOR EACH ROW
BEGIN
    -- Criar alerta automático para casos críticos
    IF NEW.categoria_risco = 'IMINENTE' AND NEW.valor_provavel_resgate_30d > 1000000 THEN
        INSERT INTO gold_alertas_risco_resgate (
            cliente_id,
            data_alerta,
            nivel_alerta,
            acao_principal,
            gatilhos_comportamentais,
            gatilhos_mercado,
            gatilhos_vida
        ) VALUES (
            NEW.cliente_id,
            NOW(),
            'CRITICO',
            CONCAT('URGENTE: Cliente com ', FORMAT(NEW.probabilidade_resgate_30d * 100, 0), 
                   '% de chance de resgatar R$ ', FORMAT(NEW.valor_provavel_resgate_30d, 2)),
            NEW.gatilhos_comportamentais,
            NEW.gatilhos_mercado,
            NEW.gatilhos_vida
        );
    END IF;
END$$

DELIMITER ;
