# ETL M7 - Sistema Simples

Sistema ETL enxuto usando classe Tabela genérica.

## Estrutura

```
novo_ETL/
├── config/                 # Configurações
├── extractors/             # Extração S3  
├── loaders/               # Insert banco
├── processors/            # Classe Tabela + processadores
├── utils/                 # Helpers
├── main.py                # Pipeline S3
└── executar_diversificacao.py  # Script direto
```

## Como Usar

**Pipeline S3 completo:**
```bash
python main.py
```

**Diversificação direta:**
```bash
python executar_diversificacao.py arquivo.xlsx
```

## Filosofia

- **Classe Tabela** = genérica para qualquer Excel
- **Cada arquivo** = objeto Tabela  
- **Processadores** = fazem transformações específicas
- **Main simples** = só coordena, sem lógica complexa

## Adicionar Novo Tipo

1. Criar `processors/vendas.py`
2. Função `processar_vendas(file_path) -> Tabela`
3. Configurar `NOME_TABELA`, `LOAD_STRATEGY`, `BATCH_SIZE`
4. Adicionar no main.py

**Sistema simples e escalável! 🎯**
