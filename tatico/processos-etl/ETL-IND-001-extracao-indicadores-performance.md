---
título: Extração de Indicadores de Performance - Google Sheets para Bronze
tipo: ETL
código: ETL-IND-001
versão: 2.0.0
data_criação: 2025-01-17
última_atualização: 2025-01-18
próxima_revisão: 2025-04-18
responsável: bruno.chiaramonti@multisete.com
aprovador: diretoria.ti@m7investimentos.com.br
tags: [etl, performance, indicadores, google-sheets, bronze, silver]
status: aprovado
confidencialidade: interno
---

# ETL-IND-001 - Extração de Indicadores de Performance

## 1. Objetivo

Extrair dados de configuração de indicadores de performance da planilha Google Sheets `m7_performance_indicators` para a camada Bronze do Data Warehouse, preservando todos os dados originais sem transformações complexas. Este ETL é responsável apenas pela extração e carga inicial dos dados brutos, preparando-os para posterior processamento pela procedure de transformação Bronze → Silver.

### Dependências
- **Modelo de dados**: [MOD-IND-002 - Performance Indicators Silver](../modelos-dados/MOD-IND-002-performance-indicators-silver.md)
- **Procedure**: [QRY-IND-003 - Bronze to Silver Indicators](../../operacional/queries/bronze/QRY-IND-003-prc_bronze_to_silver_indicators.sql)

## 2. Escopo e Aplicabilidade

### 2.1 Escopo
- **Fonte de dados**: Google Sheets - m7_performance_indicators
- **Destino**: M7Medallion.bronze.performance_indicators
- **Volume esperado**: 10-50 registros por execução
- **Frequência**: Sob demanda (mudanças são raras)

### 2.2 Fora de Escopo
- Validação complexa de fórmulas SQL (responsabilidade da procedure Bronze → Silver)
- Processamento de outras planilhas (coberto por ETL-IND-002 e ETL-IND-003)
- Transformações de tipo de dados (mantém VARCHAR na Bronze)
- Execução das fórmulas de cálculo dos indicadores

## 3. Pré-requisitos e Dependências

### 3.1 Técnicos
- **Conectividade**: 
  - Google Sheets API v4 habilitada
  - Service Account com permissões de leitura
  - Arquivo credentials/google_sheets_api.json
  - Conexão SQL Server via ODBC Driver 17
- **Recursos computacionais**: Mínimos (< 1MB de dados)
- **Software/Ferramentas**: 
  - Python 3.8+
  - google-api-python-client==2.95.0
  - pandas==2.0.3
  - pyodbc==4.0.39
  - sqlalchemy==2.0.19
  - tenacity==8.2.2 (retry logic)

### 3.2 Negócio
- **Aprovações necessárias**: Gestão de Performance deve aprovar mudanças
- **Janelas de execução**: Qualquer horário (baixo impacto)
- **SLAs dependentes**: Nenhum crítico

## 4. Arquitetura do Pipeline

### 4.1 Diagrama de Fluxo
```
[Google Sheets] ─→ [API Extract] ─→ [Validation] ─→ [Transform] ─→ [Bronze Load] ─→ [Audit Log]
                                           ↓
                                    [Quarantine]
```

### 4.2 Componentes
| Componente | Tecnologia | Função | Configuração |
|------------|------------|--------|--------------|
| Extractor | Google Sheets API v4 | Ler dados da planilha | credentials.json |
| Validator | Python/Pandas | Validar estrutura e tipos | validation_rules.py |
| Loader | PyODBC/SQLAlchemy | Inserir em Bronze | connection_string |

## 5. Processo de Extração

### 5.1 Fontes de Dados

#### Fonte: Google Sheets - m7_performance_indicators
- **Tipo**: Google Sheets via API v4
- **ID da Planilha**: `1h3jC5EpXOv-O1oyL2tBlt9Q16pLHpsoWCHaeNiRHmeY`
- **Range**: `'Página1!A:K'`
- **Colunas esperadas**: 
  - indicator_code (Código único do indicador)
  - indicator_name (Nome descritivo)
  - category (Categoria: FINANCEIRO, QUALIDADE, etc.)
  - subcategory (Subcategoria opcional)
  - indicator_type (Tipo: CARD, RANKING)
  - unit (Unidade: R$, %, QTD, etc.)
  - aggregation (Método: SUM, AVG, etc.)
  - formula (Fórmula SQL)
  - is_inverted (1 = menor é melhor)
  - is_active (1 = ativo)
  - description (Descrição detalhada)
  - notes (Observações)
- **Formato de dados**: Valores em texto (headers na linha 1)

### 5.2 Estratégia de Extração
- **Tipo**: Full Load (substitui todos os dados a cada execução)
- **Controle de watermark**: load_timestamp no Bronze
- **Método**: TRUNCATE + INSERT
- **Retry**: 3 tentativas com backoff exponencial (4-60 segundos)

## 6. Processo de Transformação

### 6.1 Validações durante Extração
| Validação | Regra | Ação se Falha |
|-----------|-------|---------------|
| Campos obrigatórios | indicator_code, indicator_name NOT NULL | Rejeitar registro |
| Código duplicado | indicator_code único na carga | Log warning |
| Planilha vazia | Mínimo 1 registro | Abortar execução |
| Colunas esperadas | Verificar headers | Abortar se estrutura mudou |

### 6.2 Transformações Aplicadas

#### T1: Padronização de Códigos
**Descrição**: Padroniza códigos para formato consistente
**Lógica**:
```python
def transform(self) -> pd.DataFrame:
    df = self.data.copy()
    
    # Padronização de códigos
    df['indicator_code'] = df['indicator_code'].str.upper().str.replace(' ', '_').str.strip()
    
    # Converter created_date se existir (não faz parte do Bronze)
    if 'created_date' in df.columns:
        df = df.drop(columns=['created_date'])
    
    return df
```
**Campos afetados**: indicator_code

#### T2: Tratamento de Valores Especiais
**Descrição**: Converte valores especiais preservando formato VARCHAR
**Lógica**:
```python
# Booleanos convertidos para string '0' ou '1'
bool_map = {'TRUE': '1', 'FALSE': '0', 'true': '1', 'false': '0', '1': '1', '0': '0', '': '0'}
df['is_inverted'] = df.get('is_inverted', '').map(bool_map).fillna('0')
df['is_active'] = df.get('is_active', '').map(bool_map).fillna('1')

# Textos vazios e NaN
text_columns = ['formula', 'notes', 'description']
for col in text_columns:
    if col in df.columns:
        df[col] = df[col].fillna('')
        
# Aggregation com default
if 'aggregation' in df.columns:
    df['aggregation'] = df['aggregation'].fillna('CUSTOM')
```
**Campos afetados**: is_inverted, is_active, formula, notes, description, aggregation

#### T3: Enriquecimento de Metadados
**Descrição**: Adiciona metadados para rastreabilidade e detecção de mudanças
**Lógica**:
```python
# Número da linha original na planilha
df['row_number'] = range(2, len(df) + 2)  # Começa em 2 (pula header)

# Hash MD5 para detectar mudanças
hash_columns = ['indicator_code', 'indicator_name', 'category', 'unit', 
               'aggregation', 'formula', 'is_inverted', 'is_active']
existing_columns = [col for col in hash_columns if col in df.columns]

df['row_hash'] = df[existing_columns].apply(
    lambda x: hashlib.md5(''.join(str(x[col]) for col in existing_columns).encode()).hexdigest(),
    axis=1
)
```
**Campos afetados**: row_number (novo), row_hash (novo)

## 7. Processo de Carga

### 7.1 Destino
- **Sistema**: SQL Server - M7Medallion
- **Schema.Tabela**: bronze.performance_indicators
- **Método de carga**: TRUNCATE + INSERT (substitui tudo)

### 7.2 Estratégia de Carga
- **Modo**: Batch (carga completa)
- **Transacional**: Sim (TRUNCATE + INSERT em transação)
- **Rollback**: Automático em caso de erro
- **Campos de controle adicionados**:
  - load_id: Auto-incremento
  - load_timestamp: Data/hora da carga
  - load_source: Identificador da fonte
  - is_processed: Flag para Bronze → Silver
  - processing_date: Quando foi processado
  - processing_status: Status do processamento
  - processing_notes: Observações do processamento

## 8. Tratamento de Erros

### 8.1 Tipos de Erro e Ações
| Tipo de Erro | Detecção | Ação | Notificação |
|--------------|----------|------|-------------|
| Google Sheets indisponível | HttpError 404/403/429 | Retry 3x com backoff exponencial | Log error |
| Credenciais inválidas | Authentication error | Parar execução | Email admin |
| Planilha vazia | Zero registros | Abortar carga | Log warning |
| Código duplicado | COUNT(DISTINCT) != COUNT(*) | Continuar com warning | Log detalhado |
| Conexão DB perdida | pyodbc.Error | Retry 1x → Falha | Log error |
| Transação falhou | Rollback automático | Registrar em audit | Log error |

### 8.2 Processo de Retry
- **Tentativas**: 3 para Google Sheets API
- **Intervalo**: Exponential backoff (4, 8, 16... até 60 segundos)
- **Timeout máximo**: 5 minutos total
- **Implementação com tenacity**:
```python
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    retry=retry_if_exception_type(HttpError)
)
def extract(self) -> pd.DataFrame:
    service = build('sheets', 'v4', credentials=self.credentials)
    sheet = service.spreadsheets()
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME
    ).execute()
    # ... resto da implementação
```

## 9. Monitoramento e Auditoria

### 9.1 Métricas de Performance
| Métrica | Threshold | Alerta |
|---------|-----------|--------|
| Tempo total execução | < 1 minuto | > 2 minutos |
| Registros processados | > 0 | = 0 |
| Taxa de erro | < 5% | > 10% |
| Latência API Google | < 5 seg | > 10 seg |

### 9.2 Logs
- **Nível de log**: INFO (DEBUG se --debug flag)
- **Localização**: logs/ETL-IND-001_YYYYMMDD_HHMMSS.log
- **Retenção**: 30 dias
- **Formato**:
  ```
  [2025-01-17 15:10:50] [INFO] [ETL-IND-001] Iniciando extração de 1h3jC5EpXOv-O1oyL2tBlt9Q16pLHpsoWCHaeNiRHmeY
  [2025-01-17 15:10:52] [INFO] [ETL-IND-001] Extraídos 12 indicadores
  [2025-01-17 15:10:53] [WARNING] [ETL-IND-001] Linha 5: categoria inválida 'OPERACIONAL'
  [2025-01-17 15:10:55] [INFO] [ETL-IND-001] Carregados 12 registros no Bronze
  ```

### 9.3 Auditoria
- **Tabela de controle**: audit.etl_executions (se existir)
- **Informações registradas**:
  - etl_name: 'ETL-IND-001-performance-indicators'
  - execution_start/end: Timestamps
  - records_read: Total extraído do Sheets
  - records_written: Total inserido no Bronze
  - records_error: Registros com warning
  - status: SUCCESS/ERROR/WARNING
  - details: JSON com detalhes da execução

## 10. Qualidade de Dados

### 10.1 Validações Pós-Carga
| Validação | Query/Método | Threshold | Ação se Falha |
|-----------|--------------|-----------|---------------|
| Códigos únicos | SELECT COUNT(*) vs COUNT(DISTINCT indicator_code) | = | Warning se duplicados |
| Registros carregados | COUNT(*) WHERE load_timestamp = MAX | > 0 | Investigar falha |
| Categorias conhecidas | % registros com categoria válida | > 95% | Log novos valores |
| Fórmulas preenchidas | % registros com formula != '' | > 90% | Notificar gestão |

### 10.2 Validação Pós-Carga
```python
def run_post_load_validation(self):
    """Executa validações após carga no Bronze"""
    with self.db_engine.connect() as conn:
        # Verificar códigos únicos
        result = conn.execute(text("""
            SELECT COUNT(*) total, COUNT(DISTINCT indicator_code) unicos
            FROM bronze.performance_indicators
            WHERE load_timestamp = (SELECT MAX(load_timestamp) FROM bronze.performance_indicators)
        """)).fetchone()
        
        if result.total != result.unicos:
            self.logger.warning(f"Códigos duplicados detectados: {result.total - result.unicos}")
```

## 11. Agendamento e Triggers

### 11.1 Schedule
- **Ferramenta**: Execução manual ou via orchestrador
- **Expressão**: Não agendado (sob demanda)
- **Timezone**: America/Sao_Paulo
- **Dependências**: Nenhuma

### 11.2 Triggers e Execução
- **Execução direta**:
  ```bash
  python etl_001_indicators.py
  ```
- **Com parâmetros**:
  ```bash
  python etl_001_indicators.py --debug --dry-run
  ```
- **Via orchestrador**:
  ```bash
  ./run_etl.sh  # Menu interativo
  python run_pipeline.py 001  # Pipeline completo
  ```

## 12. Manutenção e Operação

### 12.1 Procedimentos Operacionais
- **Reprocessamento**: Script sempre faz TRUNCATE + INSERT (full reload)
- **Limpeza de logs**: Remover arquivos > 30 dias em logs/
- **Backup da planilha**: Download manual mensal recomendado
- **Verificação de integridade**:
  ```sql
  -- Verificar última carga
  SELECT TOP 10 * FROM bronze.performance_indicators
  ORDER BY load_timestamp DESC;
  ```

### 12.2 Troubleshooting Comum
| Problema | Sintoma | Diagnóstico | Solução |
|----------|---------|-------------|---------|
| ModuleNotFoundError | Import error | pip list | pip install -r requirements.txt |
| ODBC Driver error | Can't connect to DB | odbcinst -q -d | Instalar ODBC Driver 17 |
| Planilha não encontrada | HTTP 404 | Verificar URL no navegador | Atualizar SPREADSHEET_ID |
| Timeout na API | Request timeout | Verificar quota API | Aguardar ou aumentar timeout |
| API limite excedido | HTTP 429 | Check quota Google | Aguardar reset |
| Credenciais expiradas | HTTP 401 | Verificar service account | Renovar credenciais |
| Planilha movida/deletada | HTTP 404 | Verificar ID da planilha | Atualizar configuração |
| Novos indicadores não aparecem | Count não aumenta | Check is_active | Verificar filtros |

## 13. Segurança e Compliance

### 13.1 Classificação de Dados
- **Nível de sensibilidade**: Interno
- **PII/PCI**: Não contém

### 13.2 Controles de Segurança
- **Autenticação**: Service Account com escopo readonly
- **Criptografia em trânsito**: HTTPS/TLS 1.2+
- **Criptografia em repouso**: N/A (dados não sensíveis)
- **Credenciais**: Arquivo JSON fora do controle de versão
- **Acesso DB**: Usuário com permissões mínimas (INSERT em bronze)

## 14. Versionamento e Mudanças

### 14.1 Controle de Versão
- **Repositório**: GitHub ProjetoAlicerce
- **Script**: datawarehouse-docs/operacional/scripts/etl_metas_google/etl_001_indicators.py
- **Branch strategy**: main (produção), develop (desenvolvimento)
- **Versionamento**: Semantic versioning no header do arquivo

### 14.2 Processo de Mudança
1. Testar em ambiente dev com planilha de teste
2. Validar com equipe de Performance
3. Deploy em produção com rollback plan
4. Monitorar primeira execução

## 15. Anexos

### 15.1 Configuração do ETL
```json
// config/etl_001_config.json
{
    "spreadsheet_id": "1h3jC5EpXOv-O1oyL2tBlt9Q16pLHpsoWCHaeNiRHmeY",
    "range_name": "Página1!A:K",
    "google_credentials_path": "credentials/google_sheets_api.json",
    "database": {
        "server": "${DB_SERVER}",
        "database": "${DB_DATABASE}",
        "user": "${DB_USERNAME}",
        "password": "${DB_PASSWORD}",
        "driver": "ODBC Driver 17 for SQL Server"
    },
    "validation": {
        "min_records": 1,
        "max_records": 100,
        "required_fields": ["indicator_code", "indicator_name"],
        "valid_categories": ["FINANCEIRO", "QUALIDADE", "VOLUME", "COMPORTAMENTAL", "PROCESSO", "GATILHO"],
        "valid_units": ["R$", "%", "QTD", "SCORE", "HORAS", "DIAS", "RATIO"],
        "valid_aggregations": ["SUM", "AVG", "COUNT", "MAX", "MIN", "LAST", "CUSTOM"]
    }
}
```

### 15.2 Exemplos de Dados
```json
// Exemplo de registro extraído do Google Sheets
{
  "indicator_code": "CAPTACAO_LIQUIDA",
  "indicator_name": "Captação Líquida",
  "category": "FINANCEIRO",
  "subcategory": "",
  "indicator_type": "CARD",
  "unit": "R$",
  "aggregation": "SUM",
  "formula": "captacao_bruta - resgates",
  "is_inverted": "0",
  "is_active": "1",
  "description": "Diferença entre captação bruta e resgates no período",
  "notes": "Indicador principal de crescimento"
}

// Exemplo após transformação para Bronze
{
  "load_id": 1,
  "load_timestamp": "2025-01-17 15:10:55",
  "load_source": "GoogleSheets:1h3jC5EpXOv-O1oyL2tBlt9Q16pLHpsoWCHaeNiRHmeY",
  "indicator_code": "CAPTACAO_LIQUIDA",
  "indicator_name": "Captação Líquida",
  "category": "FINANCEIRO",
  "unit": "R$",
  "aggregation": "SUM",
  "formula": "captacao_bruta - resgates",
  "is_inverted": "0",
  "is_active": "1",
  "description": "Diferença entre captação bruta e resgates no período",
  "notes": "Indicador principal de crescimento",
  "row_number": 2,
  "row_hash": "a1b2c3d4e5f6...",
  "is_processed": 0
}
```

### 15.3 Referências
- [Google Sheets API v4 Documentation](https://developers.google.com/sheets/api/quickstart/python)
- [MOD-IND-002 - Modelo de Dados Performance Indicators Silver](../modelos-dados/MOD-IND-002-performance-indicators-silver.md)
- [QRY-IND-001 - DDL Bronze Performance Indicators](../../operacional/queries/bronze/QRY-IND-001-create_bronze_performance_indicators.sql)
- [QRY-IND-003 - Procedure Bronze to Silver Indicators](../../operacional/queries/bronze/QRY-IND-003-prc_bronze_to_silver_indicators.sql)
- [ARQ-DWH-001 - Visão Geral do Data Warehouse](../../estrategico/arquiteturas/ARQ-DWH-001-visao-geral-datawarehouse.md)
- [ARQ-IND-001 - Performance Tracking System](../../estrategico/arquiteturas/ARQ-IND-001-performance-tracking-system.md)
- [CLAUDE.md - Diretrizes do Projeto](../../CLAUDE.md)

---

**Documento criado por**: Bruno Chiaramonti  
**Data**: 2025-01-18  
**Revisão**: 2.0.0 - Atualização completa seguindo template ETL
