-- ============================================================================
-- TABELA: gold_captacao_liquida_assessor
-- ============================================================================
-- Grain: Assessor × Mês
-- Propósito: Visão consolidada de performance comercial para gestão, 
--            comissionamento, rankings e identificação de best practices
-- Autor: Data Science Team - M7 Investimentos
-- Data Criação: 2025-01-02
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.gold_captacao_liquida_assessor (
    -- ========================================
    -- CHAVES E DIMENSÕES TEMPORAIS
    -- ========================================
    data_referencia DATE NOT NULL COMMENT 'Último dia do mês',
    ano INT NOT NULL COMMENT 'Ano de referência',
    mes INT NOT NULL COMMENT 'Mês de referência',
    trimestre INT NOT NULL COMMENT 'Trimestre de referência',
    semestre INT NOT NULL COMMENT 'Semestre de referência',

    -- ========================================
    -- DIMENSÕES DO ASSESSOR
    -- ========================================
    assessor_id VARCHAR(50) NOT NULL COMMENT 'ID único do assessor',
    assessor_nome VARCHAR(200) COMMENT 'Nome completo do assessor',
    assessor_cpf VARCHAR(20) COMMENT 'CPF do assessor',
    assessor_tipo VARCHAR(50) COMMENT 'Tipo: AAI/Agente/Gerente',
    assessor_nivel VARCHAR(50) COMMENT 'Nível: Junior/Pleno/Senior/Master',
    assessor_certificacoes VARCHAR(500) COMMENT 'Certificações: CPA-10, CPA-20, CEA, CFP',
    data_admissao DATE COMMENT 'Data de admissão',
    meses_experiencia_empresa INT COMMENT 'Meses na empresa',
    meses_experiencia_mercado INT COMMENT 'Meses no mercado financeiro',
    assessor_status VARCHAR(20) COMMENT 'Status: Ativo/Inativo/Licença',

    -- ========================================
    -- ESTRUTURA ORGANIZACIONAL
    -- ========================================
    escritorio_id VARCHAR(50) COMMENT 'ID do escritório',
    escritorio_nome VARCHAR(200) COMMENT 'Nome do escritório',
    escritorio_tipo VARCHAR(50) COMMENT 'Tipo: Próprio/Parceiro/Digital',
    regional_id VARCHAR(50) COMMENT 'ID da regional',
    regional_nome VARCHAR(200) COMMENT 'Nome da regional',
    diretoria_id VARCHAR(50) COMMENT 'ID da diretoria',
    diretoria_nome VARCHAR(200) COMMENT 'Nome da diretoria',
    gerente_direto_id VARCHAR(50) COMMENT 'ID do gerente direto',
    gerente_direto_nome VARCHAR(200) COMMENT 'Nome do gerente direto',

    -- ========================================
    -- CARACTERÍSTICAS DO ASSESSOR
    -- ========================================
    modelo_atendimento VARCHAR(50) COMMENT 'Presencial/Remoto/Híbrido',
    especializacao VARCHAR(200) COMMENT 'Private/Varejo/Corporate/Institucional',
    qtd_clientes_ativos INT COMMENT 'Total de clientes ativos',
    qtd_clientes_exclusivos INT COMMENT 'Clientes apenas com este assessor',
    qtd_clientes_compartilhados INT COMMENT 'Clientes com outros assessores',

    -- ========================================
    -- MÉTRICAS DE CAPTAÇÃO BRUTA
    -- ========================================
    captacao_bruta_mes DECIMAL(18,2) COMMENT 'Captação bruta no mês',
    captacao_bruta_trimestre DECIMAL(18,2) COMMENT 'Captação bruta no trimestre',
    captacao_bruta_semestre DECIMAL(18,2) COMMENT 'Captação bruta no semestre',
    captacao_bruta_ano DECIMAL(18,2) COMMENT 'Captação bruta no ano',
    captacao_bruta_12m DECIMAL(18,2) COMMENT 'Captação bruta rolling 12 meses',

    -- Detalhamento por Produto
    captacao_bruta_rv_mes DECIMAL(18,2) COMMENT 'Captação em renda variável',
    captacao_bruta_rf_mes DECIMAL(18,2) COMMENT 'Captação em renda fixa',
    captacao_bruta_fundos_mes DECIMAL(18,2) COMMENT 'Captação em fundos',
    captacao_bruta_previdencia_mes DECIMAL(18,2) COMMENT 'Captação em previdência',
    captacao_bruta_coe_mes DECIMAL(18,2) COMMENT 'Captação em COE',
    captacao_bruta_cambio_mes DECIMAL(18,2) COMMENT 'Captação em câmbio',

    -- Por Tipo de Cliente
    captacao_bruta_pf_mes DECIMAL(18,2) COMMENT 'Captação pessoa física',
    captacao_bruta_pj_mes DECIMAL(18,2) COMMENT 'Captação pessoa jurídica',
    captacao_bruta_novos_clientes_mes DECIMAL(18,2) COMMENT 'Captação de novos clientes',
    captacao_bruta_clientes_existentes_mes DECIMAL(18,2) COMMENT 'Captação de clientes existentes',

    -- ========================================
    -- MÉTRICAS DE RESGATES
    -- ========================================
    resgate_total_mes DECIMAL(18,2) COMMENT 'Resgates totais no mês',
    resgate_total_trimestre DECIMAL(18,2) COMMENT 'Resgates totais no trimestre',
    resgate_total_ano DECIMAL(18,2) COMMENT 'Resgates totais no ano',

    -- ========================================
    -- CAPTAÇÃO LÍQUIDA
    -- ========================================
    captacao_liquida_mes DECIMAL(18,2) COMMENT 'Captação líquida no mês',
    captacao_liquida_trimestre DECIMAL(18,2) COMMENT 'Captação líquida no trimestre',
    captacao_liquida_semestre DECIMAL(18,2) COMMENT 'Captação líquida no semestre',
    captacao_liquida_ano DECIMAL(18,2) COMMENT 'Captação líquida no ano',
    captacao_liquida_12m DECIMAL(18,2) COMMENT 'Captação líquida rolling 12 meses',

    -- ========================================
    -- PATRIMÔNIO (AUM)
    -- ========================================
    aum_total DECIMAL(18,2) COMMENT 'Assets Under Management total',
    aum_medio_mes DECIMAL(18,2) COMMENT 'AUM médio no mês',
    aum_medio_trimestre DECIMAL(18,2) COMMENT 'AUM médio no trimestre',
    aum_medio_12m DECIMAL(18,2) COMMENT 'AUM médio 12 meses',
    variacao_aum_mes_pct DECIMAL(8,4) COMMENT 'Variação % AUM no mês',
    variacao_aum_ano_pct DECIMAL(8,4) COMMENT 'Variação % AUM no ano',

    -- Por Segmento
    aum_varejo DECIMAL(18,2) COMMENT 'AUM segmento varejo',
    aum_alta_renda DECIMAL(18,2) COMMENT 'AUM segmento alta renda',
    aum_private DECIMAL(18,2) COMMENT 'AUM segmento private',
    aum_corporate DECIMAL(18,2) COMMENT 'AUM segmento corporate',

    -- ========================================
    -- PRODUTIVIDADE E EFICIÊNCIA
    -- ========================================
    -- Atividade com Clientes
    qtd_clientes_novos_mes INT COMMENT 'Novos clientes no mês',
    qtd_clientes_reativados_mes INT COMMENT 'Clientes reativados',
    qtd_clientes_perdidos_mes INT COMMENT 'Clientes perdidos',
    net_clientes_mes INT COMMENT 'Saldo líquido de clientes',

    -- Produtividade
    captacao_por_cliente_ativo DECIMAL(18,2) COMMENT 'Captação média por cliente',
    aum_por_cliente DECIMAL(18,2) COMMENT 'AUM médio por cliente',
    ticket_medio_captacao DECIMAL(18,2) COMMENT 'Ticket médio de captação',
    ticket_medio_cliente DECIMAL(18,2) COMMENT 'Ticket médio por cliente',

    -- Atividades
    qtd_reunioes_realizadas_mes INT COMMENT 'Reuniões realizadas',
    qtd_ligacoes_realizadas_mes INT COMMENT 'Ligações realizadas',
    qtd_emails_enviados_mes INT COMMENT 'E-mails enviados',
    qtd_propostas_enviadas_mes INT COMMENT 'Propostas enviadas',
    qtd_visitas_presenciais_mes INT COMMENT 'Visitas presenciais',
    qtd_eventos_realizados_mes INT COMMENT 'Eventos realizados',

    -- Conversão
    taxa_conversao_reuniao_captacao DECIMAL(8,4) COMMENT 'Conversão reunião para captação',
    taxa_conversao_proposta_captacao DECIMAL(8,4) COMMENT 'Conversão proposta para captação',
    tempo_medio_conversao_dias DECIMAL(10,2) COMMENT 'Tempo médio de conversão',

    -- ========================================
    -- MÉTRICAS DE CRM E PIPELINE
    -- ========================================
    -- Pipeline de Oportunidades
    qtd_oportunidades_abertas INT COMMENT 'Oportunidades em aberto',
    valor_pipeline_total DECIMAL(18,2) COMMENT 'Valor total do pipeline',
    valor_pipeline_ponderado DECIMAL(18,2) COMMENT 'Pipeline × probabilidade',

    -- Fluxo de Oportunidades
    qtd_oportunidades_criadas_mes INT COMMENT 'Oportunidades criadas',
    valor_oportunidades_criadas_mes DECIMAL(18,2) COMMENT 'Valor criado',
    qtd_oportunidades_ganhas_mes INT COMMENT 'Oportunidades ganhas',
    valor_oportunidades_ganhas_mes DECIMAL(18,2) COMMENT 'Valor ganho',
    qtd_oportunidades_perdidas_mes INT COMMENT 'Oportunidades perdidas',
    valor_oportunidades_perdidas_mes DECIMAL(18,2) COMMENT 'Valor perdido',

    -- Performance
    taxa_conversao_oportunidades DECIMAL(8,4) COMMENT 'Taxa de conversão geral',
    win_rate_valor DECIMAL(8,4) COMMENT 'Taxa de ganho por valor',
    tempo_ciclo_venda_medio_dias DECIMAL(10,2) COMMENT 'Ciclo médio de venda',
    velocity_vendas DECIMAL(10,2) COMMENT 'Velocidade de vendas',

    -- Qualidade do Pipeline
    pipeline_coverage_ratio DECIMAL(8,4) COMMENT 'Cobertura pipeline/meta',
    idade_media_oportunidades_dias DECIMAL(10,2) COMMENT 'Idade média das oportunidades',
    qtd_oportunidades_paradas INT COMMENT 'Oportunidades sem movimento 30+ dias',
    health_score_pipeline DECIMAL(8,4) COMMENT 'Score de saúde do pipeline',

    -- ========================================
    -- QUALIDADE E SATISFAÇÃO
    -- ========================================
    -- NPS
    nps_medio_carteira DECIMAL(8,2) COMMENT 'NPS médio da carteira',
    qtd_clientes_promotores INT COMMENT 'Quantidade de promotores',
    qtd_clientes_neutros INT COMMENT 'Quantidade de neutros',
    qtd_clientes_detratores INT COMMENT 'Quantidade de detratores',
    pct_clientes_promotores DECIMAL(8,4) COMMENT 'Percentual de promotores',

    -- Atendimento
    qtd_reclamacoes_mes INT COMMENT 'Reclamações no mês',
    qtd_reclamacoes_procedentes INT COMMENT 'Reclamações procedentes',
    tempo_medio_resolucao_horas DECIMAL(10,2) COMMENT 'Tempo médio de resolução',
    taxa_resolucao_primeiro_contato DECIMAL(8,4) COMMENT 'Resolução no primeiro contato',

    -- Compliance
    qtd_operacoes_irregulares INT COMMENT 'Operações irregulares',
    qtd_advertencias_compliance INT COMMENT 'Advertências de compliance',
    score_aderencia_processos DECIMAL(8,4) COMMENT 'Aderência aos processos',
    flag_certificacao_vencida BOOLEAN COMMENT 'Certificação vencida',

    -- Retenção
    taxa_retencao_clientes DECIMAL(8,4) COMMENT 'Taxa de retenção',
    taxa_churn_clientes DECIMAL(8,4) COMMENT 'Taxa de churn',
    lifetime_value_medio_carteira DECIMAL(18,2) COMMENT 'LTV médio da carteira',
    antiguidade_media_clientes_meses DECIMAL(10,2) COMMENT 'Antiguidade média dos clientes',

    -- ========================================
    -- MÉTRICAS FINANCEIRAS E COMISSIONAMENTO
    -- ========================================
    receita_bruta_gerada_mes DECIMAL(18,2) COMMENT 'Receita bruta gerada',
    receita_liquida_gerada_mes DECIMAL(18,2) COMMENT 'Receita líquida gerada',
    comissao_captacao_mes DECIMAL(18,2) COMMENT 'Comissão sobre captação',
    comissao_recorrente_mes DECIMAL(18,2) COMMENT 'Comissão recorrente',
    comissao_total_mes DECIMAL(18,2) COMMENT 'Comissão total',
    bonus_performance_mes DECIMAL(18,2) COMMENT 'Bônus de performance',

    -- Rentabilidade
    margem_contribuicao_mes DECIMAL(18,2) COMMENT 'Margem de contribuição',
    roi_assessor DECIMAL(8,4) COMMENT 'ROI do assessor',
    custo_por_cliente DECIMAL(18,2) COMMENT 'Custo por cliente',
    receita_por_cliente DECIMAL(18,2) COMMENT 'Receita por cliente',

    -- Performance Financeira
    receita_media_por_aum DECIMAL(8,4) COMMENT 'Receita média por AUM (bps)',
    share_of_wallet_medio DECIMAL(8,4) COMMENT 'Share of wallet médio',
    penetracao_produtos DECIMAL(8,4) COMMENT '% clientes com 2+ produtos',
    cross_sell_ratio DECIMAL(8,4) COMMENT 'Índice de cross-sell',

    -- ========================================
    -- COMPARATIVOS E RANKINGS
    -- ========================================
    -- Rankings Internos
    ranking_captacao_liquida_mes INT COMMENT 'Ranking captação no mês',
    ranking_captacao_liquida_regional INT COMMENT 'Ranking regional',
    ranking_captacao_liquida_nacional INT COMMENT 'Ranking nacional',
    ranking_aum_total INT COMMENT 'Ranking por AUM',
    ranking_novos_clientes INT COMMENT 'Ranking novos clientes',
    ranking_nps INT COMMENT 'Ranking por NPS',

    -- Percentis
    percentil_captacao_liquida DECIMAL(8,4) COMMENT 'Percentil captação',
    percentil_produtividade DECIMAL(8,4) COMMENT 'Percentil produtividade',
    percentil_conversao DECIMAL(8,4) COMMENT 'Percentil conversão',
    percentil_retencao DECIMAL(8,4) COMMENT 'Percentil retenção',

    -- Metas
    meta_captacao_liquida_mes DECIMAL(18,2) COMMENT 'Meta captação líquida',
    atingimento_meta_mes_pct DECIMAL(8,4) COMMENT 'Atingimento meta mês',
    meta_captacao_liquida_ano DECIMAL(18,2) COMMENT 'Meta anual',
    atingimento_meta_ano_pct DECIMAL(8,4) COMMENT 'Atingimento meta ano',
    meta_novos_clientes_mes INT COMMENT 'Meta novos clientes',
    atingimento_meta_clientes_pct DECIMAL(8,4) COMMENT 'Atingimento meta clientes',

    -- Benchmarks
    captacao_media_peer_group DECIMAL(18,2) COMMENT 'Captação média dos pares',
    aum_medio_peer_group DECIMAL(18,2) COMMENT 'AUM médio dos pares',
    produtividade_vs_peer_group DECIMAL(8,4) COMMENT 'Produtividade vs pares',

    -- ========================================
    -- ANÁLISE PREDITIVA E TENDÊNCIAS
    -- ========================================
    -- Tendências
    tendencia_captacao_3m VARCHAR(20) COMMENT 'Crescente/Estável/Decrescente',
    tendencia_produtividade_3m VARCHAR(20) COMMENT 'Tendência produtividade',
    volatilidade_captacao_6m DECIMAL(8,4) COMMENT 'Volatilidade captação',
    consistencia_performance DECIMAL(8,4) COMMENT '% meses com meta atingida',

    -- Scores Preditivos
    probabilidade_atingir_meta_mes DECIMAL(8,4) COMMENT 'Prob. atingir meta mês',
    probabilidade_atingir_meta_ano DECIMAL(8,4) COMMENT 'Prob. atingir meta ano',
    score_potencial_crescimento DECIMAL(8,4) COMMENT 'Potencial de crescimento',
    score_risco_turnover DECIMAL(8,4) COMMENT 'Risco de saída',

    -- Machine Learning
    cluster_performance VARCHAR(50) COMMENT 'Cluster de performance',
    perfil_comercial VARCHAR(50) COMMENT 'Hunter/Farmer/Híbrido',
    maturidade_carteira VARCHAR(50) COMMENT 'Nova/Em Crescimento/Madura',

    -- ========================================
    -- DESENVOLVIMENTO E CAPACITAÇÃO
    -- ========================================
    qtd_horas_treinamento_mes DECIMAL(10,2) COMMENT 'Horas de treinamento',
    qtd_certificacoes_obtidas_ano INT COMMENT 'Certificações obtidas',
    score_conhecimento_produtos DECIMAL(8,4) COMMENT 'Conhecimento produtos',
    score_conhecimento_mercado DECIMAL(8,4) COMMENT 'Conhecimento mercado',

    -- Mentoria
    flag_eh_mentor BOOLEAN COMMENT 'É mentor',
    qtd_mentorados INT COMMENT 'Quantidade de mentorados',
    score_colaboracao_equipe DECIMAL(8,4) COMMENT 'Score colaboração',
    qtd_best_practices_compartilhadas INT COMMENT 'Best practices compartilhadas',

    -- ========================================
    -- MÉTRICAS POR CANAL
    -- ========================================
    pct_clientes_digitais DECIMAL(8,4) COMMENT '% clientes digitais',
    pct_captacao_via_app DECIMAL(8,4) COMMENT '% captação via app',
    taxa_adocao_ferramentas_digitais DECIMAL(8,4) COMMENT 'Adoção ferramentas digitais',

    -- Parcerias
    qtd_indicacoes_recebidas_mes INT COMMENT 'Indicações recebidas',
    qtd_indicacoes_convertidas_mes INT COMMENT 'Indicações convertidas',
    taxa_conversao_indicacoes DECIMAL(8,4) COMMENT 'Taxa conversão indicações',
    receita_via_parcerias DECIMAL(18,2) COMMENT 'Receita via parcerias',

    -- ========================================
    -- METADADOS E CONTROLE
    -- ========================================
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data/hora da carga',
    data_ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Última atualização',
    versao_modelo VARCHAR(20) DEFAULT 'v1.0' COMMENT 'Versão do modelo',

    -- ========================================
    -- CONSTRAINTS
    -- ========================================
    PRIMARY KEY (assessor_id, data_referencia),
    INDEX idx_data_referencia (data_referencia),
    INDEX idx_escritorio_data (escritorio_id, data_referencia),
    INDEX idx_ranking_captacao (ranking_captacao_liquida_mes),
    INDEX idx_atingimento_meta (atingimento_meta_mes_pct DESC),
    INDEX idx_aum_total (aum_total DESC),
    
    -- Check constraints
    CONSTRAINT ck_data_referencia CHECK (data_referencia = LAST_DAY(data_referencia)),
    CONSTRAINT ck_scores_assessor CHECK (
        score_aderencia_processos BETWEEN 0 AND 1 AND
        probabilidade_atingir_meta_mes BETWEEN 0 AND 1 AND
        probabilidade_atingir_meta_ano BETWEEN 0 AND 1 AND
        score_potencial_crescimento BETWEEN 0 AND 1 AND
        score_risco_turnover BETWEEN 0 AND 1
    )
) 
COMMENT = 'Tabela Gold - Performance comercial dos assessores com métricas de produtividade, rankings e análises preditivas'
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
