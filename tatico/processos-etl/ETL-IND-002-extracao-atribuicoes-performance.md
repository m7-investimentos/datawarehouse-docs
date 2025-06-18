---
título: Extração de Atribuições de Performance - Google Sheets para Bronze
tipo: ETL
código: ETL-IND-002
versão: 2.0.0
data_criação: 2025-01-17
última_atualização: 2025-01-18
próxima_revisão: 2025-04-18
responsável: bruno.chiaramonti@multisete.com
aprovador: diretoria.ti@m7investimentos.com.br
tags: [etl, performance, assignments, google-sheets, bronze, silver]
status: aprovado
confidencialidade: interno
---

# ETL-IND-002 - Extração de Atribuições de Performance

## 1. Objetivo

Extrair dados de atribuições de indicadores de performance por assessor da planilha Google Sheets `m7_performance_assignments` para a camada Bronze do Data Warehouse, preservando todos os dados originais sem transformações complexas. Este ETL é responsável apenas pela extração e carga inicial dos dados brutos, incluindo validações básicas de integridade de pesos e relacionamentos, preparando-os para posterior processamento pela procedure de transformação Bronze → Silver.

### Dependências
- **Modelo de dados**: [MOD-IND-003 - Performance Assignments Silver](../modelos-dados/MOD-IND-003-performance-assignments-silver.md)
- **Procedure**: [QRY-ASS-003 - Bronze to Silver Assignments](../../operacional/queries/bronze/QRY-ASS-003-prc_bronze_to_silver_assignments.sql)
- **ETL**: [ETL-IND-001 - Extração de Indicadores](ETL-IND-001-extracao-indicadores-performance.md)

## 2. Escopo e Aplicabilidade

### 2.1 Escopo
- **Fonte de dados**: Google Sheets - m7_performance_assignments
- **Destino**: M7Medallion.bronze.performance_assignments
- **Volume esperado**: 200-500 registros por execução
- **Frequência**: Diária ou sob demanda (mudanças trimestrais)

### 2.2 Fora de Escopo
- Transformação complexa de tipos de dados (responsabilidade da procedure Bronze → Silver)
- Execução de cálculos de performance
- Processamento de metas (coberto por ETL-IND-003)
- Validação de fórmulas SQL dos indicadores
- Inserção direta na camada Silver

## 3. Pré-requisitos e Dependências

### 3.1 Técnicos
- **Conectividade**: 
  - Google Sheets API v4 habilitada
  - Service Account com permissões de leitura
  - Credenciais JSON armazenadas seguramente
- **Recursos computacionais**: Mínimos (< 5MB de dados)
- **Software/Ferramentas**: 
  - Python 3.8+
  - google-api-python-client==2.95.0
  - pandas==2.0.3
  - numpy==1.24.3
  - pyodbc==4.0.39
  - sqlalchemy==2.0.19
  - tenacity==8.2.2 (retry logic)
  - python-dotenv==1.0.0

### 3.2 Negócio
- **Aprovações necessárias**: 
  - Gestão de Performance para alterações de pesos
  - RH para novos assessores
- **Janelas de execução**: Preferencialmente fora do horário comercial
- **SLAs dependentes**: 
  - Processamento de metas mensais
  - Cálculo de remuneração variável

## 4. Arquitetura do Pipeline

### 4.1 Diagrama de Fluxo
```
[Google Sheets] ─→ [API Extract] ─→ [Validation] ─→ [Transform] ─→ [Bronze Load] ─→ [Post-Validation]
                                           ↓                                              ↓
                                    [Quarantine]                                   [Alert if Issues]
```

### 4.2 Componentes
| Componente | Tecnologia | Função | Configuração |
|------------|------------|--------|--------------|
| Extractor | Google Sheets API v4 | Ler dados da planilha | credentials.json |
| Validator | Python/Pandas | Validar pesos e relacionamentos | validation_rules.py |
| Transformer | Python/Pandas | Padronizar e enriquecer | transform_config.json |
| Loader | PyODBC/SQLAlchemy | Inserir em Bronze | connection_string |

## 5. Processo de Extração

### 5.1 Fontes de Dados

#### Fonte: Google Sheets - m7_performance_assignments
- **Tipo**: Google Sheets via API
- **ID da Planilha**: `1k9gM3poSzuwEZbwaRv-AwVqQj4WjFnJE6mT5RWTvUww`
- **Range**: `'Página1!A:J'` (todas as colunas)
- **Conexão**: 
  ```python
  # Configuração
  SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
  SERVICE_ACCOUNT_FILE = 'path/to/credentials.json'
  SPREADSHEET_ID = '1k9gM3poSzuwEZbwaRv-AwVqQj4WjFnJE6mT5RWTvUww'
  RANGE_NAME = 'Página1!A:J'
  ```

### 5.2 Estratégia de Extração
- **Tipo**: Full Load (substitui todos os dados a cada execução)
- **Controle de watermark**: load_timestamp no Bronze
- **Método**: TRUNCATE + INSERT
- **Validação prévia**: Mínimo 50 registros esperados
- **Retry**: 3 tentativas com backoff exponencial (4-60 segundos)

## 6. Processo de Transformação

### 6.1 Limpeza de Dados
| Validação | Regra | Ação se Falha |
|-----------|-------|---------------|
| Campos obrigatórios | crm_id, indicator_code, indicator_type NOT NULL | Rejeitar registro |
| Linhas vazias consecutivas | 5 ou mais linhas sem crm_id | Parar leitura |
| indicator_type válido | IN ('CARD', 'GATILHO', 'KPI', 'PPI', 'METRICA') | Log warning |
| Weight para CARD | Deve ser numérico > 0 | Log warning |
| Weight para não-CARD | Ajustar para 0.00 | Ajuste automático |

### 6.2 Transformações Aplicadas

#### T1: Padronização de Códigos e Tipos
**Descrição**: Padroniza códigos para formato consistente
**Lógica**:
```python
def standardize_assignments(self, df: pd.DataFrame) -> pd.DataFrame:
    # Padronizar crm_id
    df['crm_id'] = df['crm_id'].str.upper().str.strip()
    
    # Padronizar indicator_code
    df['indicator_code'] = df['indicator_code'].str.upper().str.replace(' ', '_').str.strip()
    
    # Padronizar indicator_type
    df['indicator_type'] = df['indicator_type'].str.upper().str.strip()
    
    # Converter weight para numérico
    df['weight'] = pd.to_numeric(df['weight'], errors='coerce').fillna(0.0)
    
    # Para não-CARD, garantir weight = 0
    df.loc[df['indicator_type'] != 'CARD', 'weight'] = 0.0
    
    # Preencher campos vazios
    text_columns = ['nome_assessor', 'created_by', 'approved_by', 'comments']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].fillna('')
    
    return df
```
**Campos afetados**: crm_id, indicator_code, indicator_type, weight

#### T2: Validação de Soma de Pesos
**Descrição**: Valida que soma dos pesos CARD = 100% por assessor/período
**Lógica**:
```python
def validate_weights(self, df: pd.DataFrame) -> List[Dict]:
    validation_errors = []
    
    # Filtrar apenas registros CARD ativos (sem valid_to)
    card_df = df[(df['indicator_type'] == 'CARD') & 
                 (df['valid_to'].isna() | (df['valid_to'] == ''))]
    
    if len(card_df) == 0:
        self.logger.warning("Nenhum indicador CARD ativo encontrado")
        return validation_errors
    
    # Agrupar por assessor e valid_from
    weight_sums = card_df.groupby(['crm_id', 'valid_from'])['weight'].sum()
    
    # Verificar somas diferentes de 100 (com tolerância de 0.01)
    invalid_weights = weight_sums[abs(weight_sums - 100.0) > 0.01]
    
    for (assessor, valid_from), total_weight in invalid_weights.items():
        validation_errors.append({
            'error_type': 'INVALID_WEIGHT_SUM',
            'crm_id': assessor,
            'valid_from': valid_from,
            'total_weight': float(total_weight),
            'expected': 100.0,
            'deviation': abs(float(total_weight) - 100.0)
        })
    
    return validation_errors
```
**Regra**: Soma de pesos CARD deve ser 100% ± 0.01

#### T3: Validação de Relacionamentos
**Descrição**: Valida que todos indicator_codes existem na tabela de indicadores
**Lógica**:
```python
def validate_relationships(self, df: pd.DataFrame, indicators_df: pd.DataFrame) -> List[Dict]:
    validation_errors = []
    
    if indicators_df.empty:
        self.logger.warning("Sem indicadores para validar relacionamentos")
        return validation_errors
    
    # Obter lista de códigos válidos
    valid_codes = set(indicators_df['indicator_code'].unique())
    
    # Verificar códigos inválidos
    invalid_codes = df[~df['indicator_code'].isin(valid_codes)]
    
    for _, row in invalid_codes.iterrows():
        validation_errors.append({
            'error_type': 'INVALID_INDICATOR_CODE',
            'crm_id': row['crm_id'],
            'indicator_code': row['indicator_code'],
            'message': 'Código não existe em performance_indicators'
        })
    
    return validation_errors
```
**Campos afetados**: indicator_exists (flag de validação)

#### T4: Enriquecimento de Metadados
**Descrição**: Adiciona metadados para rastreabilidade e detecção de mudanças
**Lógica**:
```python
# Número da linha original na planilha
df['row_number'] = range(2, len(df) + 2)  # Começa em 2 (pula header)

# Hash MD5 para detectar mudanças
hash_columns = ['crm_id', 'indicator_code', 'valid_from']
df['row_hash'] = df[hash_columns].apply(
    lambda x: hashlib.md5('_'.join(str(x[col]) for col in hash_columns).encode()).hexdigest(),
    axis=1
)

# Adicionar flag de vigência
df['is_current'] = (df['valid_to'].isna() | (df['valid_to'] == '')).astype(int)

# Validação de soma de pesos por assessor/período
df['weight_sum_valid'] = 1  # Default: válido
df['indicator_exists'] = 1  # Default: existe

# Marcar registros com erro de peso
for error in self.validation_errors:
    if error['error_type'] == 'INVALID_WEIGHT_SUM':
        mask = (df['crm_id'] == error['crm_id']) & \
               (df['valid_from'] == error['valid_from']) & \
               (df['indicator_type'] == 'CARD')
        df.loc[mask, 'weight_sum_valid'] = 0
```
**Campos adicionados**: row_number, row_hash, is_current, weight_sum_valid, indicator_exists

## 7. Processo de Carga

### 7.1 Destino
- **Sistema**: SQL Server - M7Medallion
- **Schema.Tabela**: bronze.performance_assignments
- **Método de carga**: TRUNCATE + INSERT (Full Load)

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
  - weight_sum_valid: Indica se soma de pesos é válida
  - indicator_exists: Indica se indicador existe
  - validation_errors: JSON com erros de validação

## 8. Tratamento de Erros

### 8.1 Tipos de Erro e Ações
| Tipo de Erro | Detecção | Ação | Notificação |
|--------------|----------|------|-------------|
| Google Sheets indisponível | HttpError 404/403/429 | Retry 3x com backoff exponencial | Log error |
| Credenciais inválidas | Authentication error | Parar execução | Email admin |
| Planilha com poucos dados | < 50 registros | Abortar carga | Log warning |
| Soma pesos ≠ 100% | Validação de pesos | Carregar com flag weight_sum_valid=0 | Log detalhado |
| Indicator_code inválido | JOIN com indicators | Carregar com flag indicator_exists=0 | Log warning |
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
    # ... processamento dos dados
```

## 9. Monitoramento e Auditoria

### 9.1 Métricas de Performance
| Métrica | Threshold | Alerta |
|---------|-----------|--------|
| Tempo total execução | < 2 minutos | > 5 minutos |
| Registros processados | > 100 | < 50 |
| Taxa de erro validação | < 5% | > 10% |
| Assessores com peso inválido | = 0 | > 0 |

### 9.2 Logs
- **Nível de log**: INFO (DEBUG se --debug flag)
- **Localização**: logs/ETL-IND-002_YYYYMMDD_HHMMSS.log
- **Retenção**: 30 dias
- **Formato**:
  ```
  [2025-01-17 15:10:50] [INFO] [ETL-IND-002] Iniciando extração de 1k9gM3poSzuwEZbwaRv-AwVqQj4WjFnJE6mT5RWTvUww
  [2025-01-17 15:10:52] [INFO] [ETL-IND-002] Extraídos 287 registros válidos de atribuições
  [2025-01-17 15:10:53] [WARNING] [ETL-IND-002] 5 erros de validação de peso encontrados
  [2025-01-17 15:10:53] [WARNING] [ETL-IND-002] 2 códigos de indicador inválidos
  [2025-01-17 15:10:55] [INFO] [ETL-IND-002] Carregados 287 registros no Bronze
  ```

### 9.3 Auditoria
- **Tabela de controle**: audit.etl_executions (se existir)
- **Informações registradas**:
  - etl_name: 'ETL-IND-002-performance-assignments'
  - execution_start/end: Timestamps
  - records_read: Total extraído do Sheets
  - records_written: Total inserido no Bronze
  - records_error: Total de erros de validação
  - status: SUCCESS/ERROR/WARNING
  - details: JSON com detalhes da execução incluindo:
    - weight_errors: Quantidade de erros de soma de peso
    - relationship_errors: Quantidade de códigos inválidos

## 10. Qualidade de Dados

### 10.1 Validações Pós-Carga
| Validação | Query/Método | Threshold | Ação se Falha |
|-----------|--------------|-----------|---------------|
| Assessores únicos carregados | SELECT COUNT(DISTINCT crm_id) | > 30 | Verificar filtros |
| Assessores com peso inválido | % com soma ≠ 100 | = 0 | Notificar gestão |
| Indicadores órfãos | % sem correspondência | < 5% | Executar ETL-IND-001 |
| Registros processados | COUNT(*) | > 50 | Investigar fonte |

### 10.2 Validação Pós-Carga
```python
def post_load_validations(self):
    """Executa validações após carga no Bronze"""
    with self.db_engine.connect() as conn:
        # Verificar assessores únicos
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT crm_id) as assessores_unicos
            FROM bronze.performance_assignments
            WHERE load_timestamp = (SELECT MAX(load_timestamp) FROM bronze.performance_assignments)
        """)).fetchone()
        
        self.logger.info(f"Assessores únicos carregados: {result.assessores_unicos}")
        
        # Verificar soma de pesos inválidos
        result = conn.execute(text("""
            WITH weight_check AS (
                SELECT 
                    crm_id,
                    valid_from,
                    SUM(CAST(weight AS DECIMAL(5,2))) as total_weight
                FROM bronze.performance_assignments
                WHERE indicator_type = 'CARD'
                  AND (valid_to IS NULL OR valid_to = '')
                  AND load_timestamp = (SELECT MAX(load_timestamp) FROM bronze.performance_assignments)
                GROUP BY crm_id, valid_from
            )
            SELECT COUNT(*) as assessores_com_erro
            FROM weight_check
            WHERE ABS(total_weight - 100.0) >= 0.01
        """)).fetchone()
        
        if result.assessores_com_erro > 0:
            self.logger.warning(f"Assessores com soma de pesos inválida: {result.assessores_com_erro}")
```

## 11. Agendamento e Triggers

### 11.1 Schedule
- **Ferramenta**: Execução manual ou via orchestrador
- **Expressão**: Não agendado (sob demanda)
- **Timezone**: America/Sao_Paulo
- **Dependências**: ETL-IND-001 deve executar antes

### 11.2 Triggers e Execução
- **Execução direta**:
  ```bash
  python etl_002_assignments.py
  ```
- **Com parâmetros**:
  ```bash
  python etl_002_assignments.py --debug --dry-run
  python etl_002_assignments.py --validate-only
  ```
- **Via orchestrador**:
  ```bash
  ./run_etl.sh  # Menu interativo
  python run_pipeline.py 002  # Pipeline completo
  ```

### 11.3 Integração com Pipeline
```bash
# Executar todos os ETLs em sequência
python run_all_etls.py

# Executar pipeline completo (ETL + Procedures)
python run_full_pipeline.py
```

## 12. Manutenção e Operação

### 12.1 Procedimentos Operacionais
- **Reprocessamento**: Script sempre faz TRUNCATE + INSERT (full reload)
- **Limpeza de logs**: Remover arquivos > 30 dias em logs/
- **Backup da planilha**: Download manual trimestral recomendado
- **Verificação de integridade**:
  ```sql
  -- Verificar última carga
  SELECT TOP 10 * FROM bronze.performance_assignments
  ORDER BY load_timestamp DESC;
  
  -- Verificar pesos por assessor
  SELECT crm_id, SUM(CAST(weight AS DECIMAL(5,2))) as total
  FROM bronze.performance_assignments
  WHERE indicator_type = 'CARD' AND is_processed = 0
  GROUP BY crm_id;
  ```

### 12.2 Troubleshooting Comum
| Problema | Sintoma | Diagnóstico | Solução |
|----------|---------|-------------|---------|
| ModuleNotFoundError | Import error | pip list | pip install -r requirements.txt |
| Planilha não encontrada | HTTP 404 | Verificar URL no navegador | Atualizar SPREADSHEET_ID |
| Poucos registros | < 50 carregados | Verificar filtros na planilha | Revisar dados fonte |
| Pesos não somam 100% | Erro validação | Query weight_check | Ajustar na planilha |
| Assessor sem indicadores | Missing data | Check assignments | Verificar com RH |
| Performance lenta | > 5 min execução | Check índices | Rebuild índices |
| Indicador novo não aparece | Validation fail | Check indicators ETL | Rodar ETL-001 primeiro |


## 13. Segurança e Compliance

### 13.1 Classificação de Dados
- **Nível de sensibilidade**: Confidencial (dados de RH)
- **PII/PCI**: Contém nomes de funcionários

### 13.2 Controles de Segurança
- **Autenticação**: Service Account Google (read-only)
- **Criptografia em trânsito**: HTTPS/TLS 1.2+
- **Acesso banco**: Apenas role ETL_BRONZE_WRITER
- **Mascaramento**: Nomes em ambientes não-produção
- **Auditoria**: Log completo de acessos e mudanças

## 14. Versionamento e Mudanças

### 14.1 Controle de Versão
- **Repositório**: GitHub ProjetoAlicerce
- **Script**: datawarehouse-docs/operacional/scripts/etl_metas_google/etl_002_assignments.py
- **Config**: datawarehouse-docs/operacional/scripts/etl_metas_google/config/etl_002_config.json
- **Branch strategy**: main (produção), develop (desenvolvimento)
- **Versionamento**: Semantic versioning no header do arquivo

### 14.2 Processo de Mudança
1. Mudanças de peso requerem aprovação em planilha
2. Novos tipos de indicador requerem atualização do ETL
3. Deploy sempre com backup da última versão válida
4. Rollback automático se validações críticas falharem

## 15. Anexos

### 15.1 Configuração do ETL
```json
// config/etl_002_config.json
{
    "etl_name": "ETL-IND-002-performance-assignments",
    "spreadsheet_id": "1k9gM3poSzuwEZbwaRv-AwVqQj4WjFnJE6mT5RWTvUww",
    "range_name": "Página1!A:J",
    "google_credentials_path": "credentials/google_sheets_api.json",
    "database": {
        "server": "${DB_SERVER}",
        "database": "${DB_DATABASE}",
        "user": "${DB_USERNAME}",
        "password": "${DB_PASSWORD}",
        "driver": "ODBC Driver 17 for SQL Server"
    },
    "validation": {
        "min_records": 50,
        "max_records": 1000,
        "required_fields": ["crm_id", "indicator_code", "indicator_type"],
        "valid_indicator_types": ["CARD", "GATILHO", "KPI", "PPI", "METRICA"],
        "max_weight_deviation": 0.01
    },
    "processing": {
        "batch_size": 50,
        "validate_relationships": true,
        "validate_weights": true
    }
}
```

### 15.2 Exemplos de Dados
```json
// Exemplo de registro extraído do Google Sheets
{
  "crm_id": "AAI001",
  "nome_assessor": "João Silva",
  "indicator_code": "CAPTACAO_LIQUIDA",
  "indicator_type": "CARD",
  "weight": "35",
  "valid_from": "2025-01-01",
  "valid_to": "",
  "created_by": "gestor.performance@m7.com.br",
  "approved_by": "diretor.comercial@m7.com.br",
  "comments": "Peso aumentado devido a foco em captação"
}

// Exemplo após transformação para Bronze
{
  "load_id": 123,
  "load_timestamp": "2025-01-17 15:10:55",
  "load_source": "GoogleSheets:1k9gM3poSzuwEZbwaRv-AwVqQj4WjFnJE6mT5RWTvUww",
  "crm_id": "AAI001",
  "nome_assessor": "João Silva",
  "indicator_code": "CAPTACAO_LIQUIDA",
  "indicator_type": "CARD",
  "weight": "35",
  "valid_from": "2025-01-01",
  "valid_to": "",
  "created_by": "gestor.performance@m7.com.br",
  "approved_by": "diretor.comercial@m7.com.br",
  "comments": "Peso aumentado devido a foco em captação",
  "row_number": 15,
  "row_hash": "e5f6g7h8i9...",
  "is_current": 1,
  "is_processed": 0,
  "weight_sum_valid": 1,
  "indicator_exists": 1,
  "validation_errors": null
}

// Exemplo de erro de validação
{
  "error_type": "INVALID_WEIGHT_SUM",
  "crm_id": "AAI002",
  "valid_from": "2025-01-01",
  "total_weight": 95.0,
  "expected": 100.0,
  "deviation": 5.0
}
```

### 15.3 Fluxo de Carga Detalhado
```python
# Processo de carga em lotes para evitar timeouts
def load(self, dry_run: bool = False) -> int:
    # TRUNCATE para garantir dados limpos
    conn.execute(text("TRUNCATE TABLE bronze.performance_assignments"))
    
    # Preparar dados
    load_data = self.processed_data.copy()
    load_data['load_timestamp'] = datetime.now()
    load_data['load_source'] = f'GoogleSheets:{SPREADSHEET_ID}'
    
    # Converter para string (Bronze preserva formato original)
    string_columns = ['crm_id', 'nome_assessor', 'indicator_code', 
                     'indicator_type', 'weight', 'valid_from', 'valid_to']
    for col in string_columns:
        if col in load_data.columns:
            load_data[col] = load_data[col].astype(str).replace('nan', '')
    
    # Inserir em lotes de 50 registros
    batch_size = 50
    for i in range(0, len(load_data), batch_size):
        batch = load_data.iloc[i:i+batch_size]
        batch.to_sql(
            'performance_assignments',
            conn,
            schema='bronze',
            if_exists='append',
            index=False
        )
```

### 15.4 Referências
- [Google Sheets API v4 Documentation](https://developers.google.com/sheets/api/quickstart/python)
- [MOD-IND-003 - Modelo de Dados Performance Assignments Silver](../modelos-dados/MOD-IND-003-performance-assignments-silver.md)
- [QRY-ASS-001 - DDL Bronze Performance Assignments](../../operacional/queries/bronze/QRY-ASS-001-create_bronze_performance_assignments.sql)
- [QRY-ASS-003 - Procedure Bronze to Silver Assignments](../../operacional/queries/bronze/QRY-ASS-003-prc_bronze_to_silver_assignments.sql)
- [ETL-IND-001 - Extração de Indicadores de Performance](ETL-IND-001-extracao-indicadores-performance.md)
- [ARQ-DWH-001 - Visão Geral do Data Warehouse](../../estrategico/arquiteturas/ARQ-DWH-001-visao-geral-datawarehouse.md)
- [ARQ-IND-001 - Performance Tracking System](../../estrategico/arquiteturas/ARQ-IND-001-performance-tracking-system.md)
- [CLAUDE.md - Diretrizes do Projeto](../../CLAUDE.md)

---

**Documento criado por**: Bruno Chiaramonti  
**Data**: 2025-01-18  
**Revisão**: 2.0.0 - Atualização completa seguindo template ETL