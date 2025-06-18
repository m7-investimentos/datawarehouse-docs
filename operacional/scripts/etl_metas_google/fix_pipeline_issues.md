# Resumo dos Problemas Identificados e Soluções

## Problemas Encontrados

### 1. Fórmulas no Google Sheets ainda contêm `cod_assessor`
- **Status**: 3 fórmulas precisam ser atualizadas
- **Indicadores afetados**: NPS_NOTA, NPS_TX_RESP, CAPT_LIQ
- **Ação**: Atualizar manualmente as fórmulas no Google Sheets para usar `crm_id`

### 2. Dados carregados no Bronze mas com NULLs
- **Status**: 7 registros carregados, mas 4 sem fórmulas
- **Causa**: Algumas linhas no Google Sheets não têm fórmulas definidas
- **Ação**: Isso é esperado - nem todos os indicadores têm fórmulas SQL

### 3. Apenas 1 registro chegou ao Silver (de 7)
- **Status**: Problema na transformação Bronze → Silver
- **Causa**: Procedure está processando apenas parcialmente
- **Ação**: Investigar a procedure `bronze.prc_process_indicators_to_silver`

### 4. Tabelas e procedures faltantes
- **Status**: Não existem tabelas/procedures para assignments e targets
- **Tabelas faltantes**:
  - bronze.performance_assignments
  - bronze.performance_targets
  - silver.performance_assignments
  - silver.performance_targets
- **Procedures faltantes**:
  - bronze.prc_bronze_to_silver_assignments
  - bronze.prc_bronze_to_silver_performance_targets

## Solução Imediata

### Passo 1: Atualizar Google Sheets
Acessar a planilha e substituir nas fórmulas:
- `cod_assessor` → `crm_id`

### Passo 2: Criar tabelas faltantes
Execute os scripts SQL já existentes:
```bash
# Bronze
sqlcmd -S server -U user -P pass -i QRY-ASS-001-create_bronze_performance_assignments.sql
sqlcmd -S server -U user -P pass -i QRY-TAR-001-create_bronze_performance_targets.sql

# Silver
sqlcmd -S server -U user -P pass -i QRY-ASS-002-create_silver_performance_assignments.sql
sqlcmd -S server -U user -P pass -i QRY-TAR-002-create_silver_performance_targets.sql
```

### Passo 3: Criar procedures faltantes
Execute os scripts das procedures:
```bash
sqlcmd -S server -U user -P pass -i QRY-ASS-003-prc_bronze_to_silver_assignments.sql
sqlcmd -S server -U user -P pass -i QRY-TAR-003-prc_bronze_to_silver_performance_targets.sql
```

### Passo 4: Investigar procedure de indicators
A procedure está processando apenas 1 de 7 registros. Verificar:
- Condições WHERE na procedure
- Critérios de validação muito restritivos
- Logs de erro na procedure

## Próximos Passos

1. **Criar script de setup** para criar todas as tabelas/procedures necessárias
2. **Revisar procedure de indicators** para entender por que processa apenas 1 registro
3. **Testar pipeline completo** após correções