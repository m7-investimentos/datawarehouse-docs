CREATE TABLE M7InvestimentosOLAP.bronze.bronze_xp_open_investment_extrato (
    segmento                       VARCHAR(20)     NULL,
    cód_assessor                   VARCHAR(20)     NULL,
    cód_conta                      INT             NULL,
    cód_matriz                     SMALLINT        NULL,
    instituição_bancária           VARCHAR(500)    NULL,
    produtos                       VARCHAR(100)    NULL,
    sub_produtos                   VARCHAR(100)    NULL,
    ativo                          VARCHAR(500)    NULL,
    valor_bruto                    DECIMAL(18,6)   NULL,
    valor_líquido                  DECIMAL(18,6)   NULL,
    data_carga DATE NOT NULL DEFAULT (CAST(GETDATE() AS DATE))
);