# Alterações Realizadas - ETL Metas Google

## Renomeação de Arquivos de Configuração

### Arquivo Renomeado:
- `config/config.json` → `config/etl_001_config.json`

### Arquivos Atualizados:

1. **etl_001_indicators.py**
   - Linha 556: Atualizado caminho padrão de configuração
   - Linha 557: Atualizado texto de ajuda
   - Linha 35: Atualizado exemplo de uso no header

2. **etl_002_assignments.py**
   - Linha 772: Atualizado caminho padrão de configuração
   - Linha 773: Atualizado texto de ajuda
   - Linha 36: Atualizado exemplo de uso no header

3. **README.md**
   - Linha 11: Atualizado nome do arquivo de configuração do ETL-001
   - Linha 26: Atualizado estrutura de diretórios

## Estrutura Final de Configurações:
```
config/
├── etl_001_config.json   # Configuração para ETL de indicadores
└── etl_002_config.json   # Configuração para ETL de atribuições
```

## Justificativa:
- Melhor organização e clareza sobre qual configuração pertence a qual ETL
- Evita confusão ao executar múltiplos ETLs
- Segue padrão de nomenclatura consistente com os scripts

## Data: 2025-01-17
## Autor: bruno.chiaramonti@multisete.com

## Correção de Conversão de Decimais - ETL-003

### Data: 2025-01-20
### Autor: bruno.chiaramonti@multisete.com

### Problema:
- Valores decimais com vírgula (ex: "0,2") estavam sendo convertidos para NULL no banco
- O `pd.to_numeric` não reconhece vírgula como separador decimal
- Isso causava perda de dados de metas como NPS_TX_RESP

### Solução:
- Adicionado pré-processamento para converter vírgulas em pontos antes da conversão numérica
- Linha 372 em `etl_003_targets.py`:
  ```python
  # Antes:
  df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
  
  # Depois:
  df[col] = df[col].astype(str).str.replace(',', '.').apply(pd.to_numeric, errors='coerce').fillna(0.0)
  ```

### Impacto:
- Todos os valores decimais com vírgula agora são corretamente convertidos
- Resolve o problema de target_value NULL para indicadores com valores decimais pequenos

### Versão: 1.0.0 → 1.0.1