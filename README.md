# datawarehouse-docs
🗄️ Documentação técnica completa do Data Warehouse da M7. Arquitetura, modelagem de dados, ETL, catálogos de queries e guias de uso para analistas e desenvolvedores.

## 📂 Estrutura do Repositório

```
datawarehouse-docs/
│
├── sql/
│   └── gold/
│       └── captacao_liquida/
│           ├── README.md
│           ├── gold_captacao_liquida_cliente.sql
│           ├── gold_captacao_liquida_assessor.sql
│           ├── gold_pipeline_resgates_previstos.sql
│           ├── gold_forecast_captacao_liquida.sql
│           └── gold_alertas_risco_resgate.sql
│
└── README.md (este arquivo)
```

## 🎯 Tabelas Gold - Captação Líquida

### Visão Geral

As tabelas Gold de captação líquida foram projetadas para fornecer uma visão completa e integrada dos fluxos de captação e resgate, permitindo:

- 📊 **Análises Avançadas**: Dashboards executivos e relatórios gerenciais
- 🤖 **Machine Learning**: Features pré-calculadas para modelos preditivos
- 🎯 **Ações Proativas**: Sistema de alertas e prevenção de resgates
- 📈 **Forecasting**: Previsões integradas de captação líquida

### Tabelas Disponíveis

| Tabela | Propósito | Grain |
|--------|-----------|-------|
| `gold_captacao_liquida_cliente` | Visão 360° do cliente com métricas completas | Cliente × Mês |
| `gold_captacao_liquida_assessor` | Performance comercial dos assessores | Assessor × Mês |
| `gold_pipeline_resgates_previstos` | Previsão e prevenção de resgates | Cliente × Data Previsão |
| `gold_forecast_captacao_liquida` | Forecast integrado de captação líquida | Cliente × Período |
| `gold_alertas_risco_resgate` | Sistema operacional de alertas | Cliente × Alerta |

## 🚀 Quick Start

### 1. Criação das Tabelas

Execute os scripts SQL na seguinte ordem:

```bash
# 1. Tabela principal de clientes
mysql -u user -p database < sql/gold/captacao_liquida/gold_captacao_liquida_cliente.sql

# 2. Tabela de assessores
mysql -u user -p database < sql/gold/captacao_liquida/gold_captacao_liquida_assessor.sql

# 3. Pipeline de resgates
mysql -u user -p database < sql/gold/captacao_liquida/gold_pipeline_resgates_previstos.sql

# 4. Forecast integrado
mysql -u user -p database < sql/gold/captacao_liquida/gold_forecast_captacao_liquida.sql

# 5. Sistema de alertas
mysql -u user -p database < sql/gold/captacao_liquida/gold_alertas_risco_resgate.sql
```

### 2. Query de Exemplo

```sql
-- Top 10 clientes por captação líquida no mês
SELECT 
    cliente_nome,
    assessor_nome,
    captacao_liquida_mes,
    patrimonio_atual,
    score_risco_churn
FROM gold.gold_captacao_liquida_cliente
WHERE data_referencia = LAST_DAY(CURRENT_DATE - INTERVAL 1 MONTH)
ORDER BY captacao_liquida_mes DESC
LIMIT 10;
```

## 📚 Documentação Detalhada

Para informações completas sobre cada tabela, incluindo:
- Descrição detalhada de campos
- Casos de uso
- Queries de exemplo
- Processo de ETL

Consulte: [sql/gold/captacao_liquida/README.md](sql/gold/captacao_liquida/README.md)

## 🔧 Manutenção e Suporte

### Time Responsável
**Data & Analytics Team**
- Email: data@m7investimentos.com.br
- Slack: #data-warehouse

### Contribuindo
1. Crie uma branch: `git checkout -b feature/nova-metrica`
2. Faça suas alterações
3. Commit: `git commit -m 'Adiciona nova métrica X'`
4. Push: `git push origin feature/nova-metrica`
5. Abra um Pull Request

## 📝 Changelog

### [2025.01.02] - Inicial
- Criação das 5 tabelas Gold de captação líquida
- Documentação completa
- Scripts DDL com particionamento e índices

---

*M7 Investimentos - Transformando dados em inteligência para investimentos*
