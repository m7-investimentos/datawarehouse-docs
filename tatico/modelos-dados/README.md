# Modelos de Dados

Este diretório contém a documentação de estruturas de dados, relacionamentos e regras de negócio.

## Padrão de Nomenclatura

- `MOD-###-[descricao].md`

Onde:
- MOD = Prefixo para Modelo de Dados
- ### = Número sequencial de 3 dígitos
- [descricao] = Descrição resumida em lowercase com hífens

## Documentos

| Código | Documento | Descrição |
|--------|-----------|-----------|
| MOD-001 | [Modelo Dimensional](MOD-001-modelo-dimensional.md) | Modelo dimensional do Data Warehouse |
| MOD-002 | [Captação Líquida](MOD-002-captacao-liquida.md) | Modelo de dados para análise de captação |

## Template

Utilize o template [MOD-template.md](https://github.com/m7-investimentos/.github-private/blob/main/templates/MOD-template.md) para criar novos documentos.

## Dicionário de Dados

Cada documento MOD deve incluir um dicionário de dados completo com:
- Nome do campo
- Tipo de dado
- Descrição
- Constraints
- Regras de negócio
