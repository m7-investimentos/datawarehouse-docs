# Tests Directory - ETL Metas Google

Esta pasta contém scripts de teste, diagnóstico e verificação otimizados e consolidados.

## 📁 Estrutura Organizada

### 🔧 Utilitários Base
- **`test_utilities.py`** - Funções comuns reutilizáveis:
  - `DatabaseConnection` - Gerenciamento de conexões
  - `StructureChecker` - Verificação de estruturas de tabelas
  - `DataAnalyzer` - Análise de dados
  - `DataValidator` - Validação de regras de negócio

### 📊 Scripts de Análise
- **`analyze_all_data.py`** - Análise completa de todas as camadas:
  ```bash
  python analyze_all_data.py              # Análise completa
  python analyze_all_data.py --bronze     # Apenas Bronze
  python analyze_all_data.py --silver     # Apenas Silver
  python analyze_all_data.py --flow       # Fluxo Bronze→Silver
  ```

### 🏥 Diagnóstico
- **`diagnose_etl_issues.py`** - Diagnóstico automático de problemas:
  ```bash
  python diagnose_etl_issues.py           # Diagnóstico completo
  python diagnose_etl_issues.py --quick   # Verificação rápida
  ```

### ✅ Verificação de Dados
- **`verify_etl_data.py`** - Verificação pós-ETL:
  ```bash
  python verify_etl_data.py               # Verificação completa
  python verify_etl_data.py --etl 001    # Apenas ETL-001
  python verify_etl_data.py --details    # Com detalhes
  python verify_etl_data.py --quality    # Qualidade de dados
  ```

### 🧪 Suite de Testes
- **`test_suite.py`** - Testes automatizados:
  ```bash
  python test_suite.py                    # Todos os testes
  python test_suite.py --test connection  # Teste específico
  python test_suite.py --quick           # Testes rápidos
  ```

## 🚀 Uso Recomendado

### 1. Primeira Execução
```bash
# 1. Verificar ambiente
python diagnose_etl_issues.py

# 2. Se tudo OK, testar conexão
python test_suite.py --test connection

# 3. Verificar estruturas
python test_suite.py --test structure
```

### 2. Após Executar ETL
```bash
# 1. Verificar dados carregados
python verify_etl_data.py

# 2. Análise detalhada se necessário
python analyze_all_data.py --details

# 3. Verificar qualidade
python verify_etl_data.py --quality
```

### 3. Troubleshooting
```bash
# 1. Diagnóstico completo
python diagnose_etl_issues.py

# 2. Analisar fluxo de dados
python analyze_all_data.py --flow

# 3. Executar testes
python test_suite.py
```

## 📝 Histórico de Consolidação

### Arquivos Consolidados
Os seguintes arquivos foram consolidados nos scripts acima:

**Em `analyze_all_data.py`:**
- analyze_assignments_data.py
- analyze_bronze_processing.py
- debug_columns.py
- debug_etl001.py
- debug_etl002_assignments.py
- debug_etl003_columns.py

**Em `verify_etl_data.py`:**
- verify_data.py
- verify_all_data.py
- verify_etl002_complete.py
- verify_etl003_data.py

**Em `test_utilities.py`:**
- test_connection.py
- check_bronze_structure.py
- check_bronze_columns.py
- Funções comuns de vários scripts

### Arquivos Mantidos (Correções Finais)
- **SQL Files**: Apenas as versões finais das procedures corrigidas
- **Python Scripts**: Scripts consolidados e otimizados

## 🎯 Benefícios da Consolidação

1. **Menos arquivos**: De 63 para ~5 arquivos principais
2. **Código reutilizável**: Funções comuns em `test_utilities.py`
3. **Interface consistente**: Todos os scripts usam argumentos similares
4. **Manutenção facilitada**: Código organizado e documentado
5. **Execução simplificada**: Comandos padronizados

## 💡 Dicas

- Use `--help` em qualquer script para ver opções disponíveis
- Scripts retornam código 0 (sucesso) ou 1 (falha) para automação
- Logs são exibidos no console com níveis apropriados (INFO, WARNING, ERROR)
- Todos os scripts são compatíveis com Python 3.6+