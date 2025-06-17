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