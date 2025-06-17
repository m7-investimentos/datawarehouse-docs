# ETL Scripts - Performance Indicators & Assignments

Este diretório contém os scripts ETL para extração de dados de performance do Google Sheets para a camada Bronze do Data Warehouse M7.

## Scripts Disponíveis

### ETL-001: Performance Indicators
- **Script**: `etl_001_indicators.py`
- **Fonte**: Google Sheets - m7_performance_indicators
- **Destino**: bronze.performance_indicators
- **Configuração**: `config/etl_001_config.json`
- **Frequência**: Sob demanda (mudanças raras)

### ETL-002: Performance Assignments
- **Script**: `etl_002_assignments.py`
- **Fonte**: Google Sheets - m7_performance_assignments
- **Destino**: bronze.performance_assignments
- **Configuração**: `config/etl_002_config.json`
- **Frequência**: Diária ou sob demanda

## Estrutura do Projeto

```
etl_metas_google/
├── config/                    # Arquivos de configuração
│   ├── etl_001_config.json   # Config ETL-001
│   └── etl_002_config.json   # Config ETL-002
├── credentials/              # Credenciais do Google (não versionadas)
│   └── google_sheets_api.json
├── logs/                     # Logs de execução
├── data/                     # Dados temporários
├── etl_001_indicators.py     # ETL de indicadores
├── etl_002_assignments.py    # ETL de atribuições
├── run_all_etls.py          # Orquestrador de ETLs
├── run_full_pipeline.py     # Pipeline completo (ETLs + Procedures)
├── run_etl.sh               # Menu interativo para execução
├── test_connection.py        # Script para testar conexão
├── verify_data.py            # Script para verificar dados
├── requirements.txt          # Dependências Python
├── .env                      # Variáveis de ambiente
├── CHANGES.md               # Histórico de alterações
└── README.md                 # Este arquivo
```

## Configuração

1. **Instalar dependências:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configurar credenciais do Google:**
   - Obter arquivo JSON de Service Account com acesso às planilhas
   - Salvar como `credentials/google_sheets_api.json`

3. **Configurar variáveis de ambiente (.env):**
   ```bash
   DB_SERVER=172.17.0.10
   DB_DATABASE=M7Medallion
   DB_USERNAME=m7invest
   DB_PASSWORD=!@Multi19732846
   DB_DRIVER=ODBC Driver 17 for SQL Server
   ```

## Execução

### Método 1: Menu Interativo (Recomendado)
```bash
# Executar menu interativo
./run_etl.sh

# Ou se não tiver permissão de execução
bash run_etl.sh
```

### Método 2: Execução Individual

#### ETL-001 - Indicadores de Performance
```bash
# Execução padrão
python etl_001_indicators.py

# Modo debug
python etl_001_indicators.py --debug

# Dry run (sem carga no banco)
python etl_001_indicators.py --dry-run
```

#### ETL-002 - Atribuições de Performance
```bash
# Execução padrão
python etl_002_assignments.py

# Validação apenas
python etl_002_assignments.py --validate-only

# Modo debug com dry run
python etl_002_assignments.py --debug --dry-run
```

### Método 3: Execução em Batch

#### Executar todos os ETLs
```bash
# Todos os ETLs em sequência
python run_all_etls.py

# Apenas ETLs específicos
python run_all_etls.py --only-etl 001 002

# Continuar mesmo se houver erro
python run_all_etls.py --continue-on-error
```

#### Pipeline Completo (ETLs + Procedures)
```bash
# Pipeline completo
python run_full_pipeline.py

# Apenas ETLs
python run_full_pipeline.py --only-etls

# Apenas procedures
python run_full_pipeline.py --only-procedures

# Modo debug
python run_full_pipeline.py --debug
```

## Fluxo de Processamento

1. **ETL-001** deve ser executado primeiro para garantir que os indicadores existam
2. **ETL-002** valida os códigos de indicadores contra a tabela de indicadores
3. Após carga no Bronze, executar procedures de processamento:
   ```sql
   -- Processar indicadores
   EXEC metadata.prc_bronze_to_metadata_indicators;
   
   -- Processar atribuições
   EXEC metadata.prc_bronze_to_metadata_assignments;
   ```

## Validações

### ETL-001 - Indicadores
- Campos obrigatórios: indicator_code, indicator_name
- Categorias válidas: FINANCEIRO, QUALIDADE, VOLUME, COMPORTAMENTAL, PROCESSO, GATILHO
- Unidades válidas: R$, %, QTD, SCORE, HORAS, DIAS, RATIO

### ETL-002 - Atribuições
- Campos obrigatórios: cod_assessor, indicator_code, indicator_type
- Tipos válidos: CARD, GATILHO, KPI, PPI, METRICA
- Soma de pesos CARD deve ser 100% por assessor/período
- Todos os indicator_codes devem existir em performance_indicators

## Monitoramento

### Logs
- Logs detalhados em `logs/ETL-IND-XXX_YYYYMMDD_HHMMSS.log`
- Níveis: INFO, WARNING, ERROR
- Modo debug disponível via flag --debug

### Validações Pós-Carga
- Verificação de códigos únicos
- Validação de somas de pesos
- Verificação de relacionamentos

### Tabelas de Controle
```sql
-- Verificar cargas pendentes
SELECT * FROM bronze.performance_indicators WHERE is_processed = 0;
SELECT * FROM bronze.performance_assignments WHERE is_processed = 0;

-- Verificar erros de validação
SELECT * FROM bronze.performance_assignments 
WHERE validation_errors IS NOT NULL;
```

## Troubleshooting

### Problema: Conexão com Google Sheets falha
- Verificar credenciais em `credentials/google_sheets_api.json`
- Confirmar que a Service Account tem acesso às planilhas
- Verificar IDs das planilhas nas configurações

### Problema: Conexão com banco falha
- Executar `python test_connection.py` para diagnóstico
- Verificar driver ODBC instalado
- Confirmar credenciais no arquivo .env

### Problema: Dados não aparecem após ETL
- Verificar se a transação foi commitada
- Executar `python verify_data.py` para confirmar dados
- Verificar logs para erros de validação

## Suporte

Para dúvidas ou problemas:
- Email: arquitetura.dados@m7investimentos.com.br
- Documentação: `/datawarehouse-docs/tatico/processos-etl/`