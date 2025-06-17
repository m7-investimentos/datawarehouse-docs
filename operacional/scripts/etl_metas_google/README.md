# ETL Metas Google - Performance Indicators

Este diretório contém os scripts ETL para extração de dados de performance do Google Sheets para o Data Warehouse.

## Estrutura do Projeto

```
etl_metas_google/
├── config/                 # Arquivos de configuração
│   └── config.json        # Configuração principal
├── credentials/           # Credenciais do Google (não versionadas)
│   └── .gitkeep
├── logs/                  # Logs de execução
│   └── .gitkeep
├── etl_001_indicators.py  # Script principal do ETL
├── requirements.txt       # Dependências Python
└── README.md             # Este arquivo
```

## Configuração

1. **Instalar dependências:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configurar credenciais do Google:**
   - Obter arquivo JSON de Service Account com acesso à planilha
   - Salvar como `credentials/google_sheets_api.json`

3. **Configurar banco de dados:**
   - Editar `config/config.json` com as credenciais corretas
   - Ou usar variáveis de ambiente:
     ```bash
     export DB_SERVER=seu_servidor
     export DB_NAME=M7Medallion
     export DB_USER=seu_usuario
     export DB_PASSWORD=sua_senha
     ```

## Execução

### Execução básica:
```bash
python etl_001_indicators.py
```

### Modo debug com dry-run:
```bash
python etl_001_indicators.py --debug --dry-run
```

### Com configuração customizada:
```bash
python etl_001_indicators.py --config config/production.json
```

## Monitoramento

- Logs são salvos em `logs/` com timestamp
- Tabela de auditoria: `audit.etl_executions`
- Validações pós-carga automáticas

## Troubleshooting

1. **Erro de autenticação Google:**
   - Verificar se o arquivo de credenciais existe
   - Confirmar que o Service Account tem acesso à planilha

2. **Erro de conexão com banco:**
   - Verificar configurações em `config/config.json`
   - Testar conectividade com o servidor SQL

3. **Dados não aparecem:**
   - Verificar logs para erros de validação
   - Confirmar que a planilha tem dados no range esperado