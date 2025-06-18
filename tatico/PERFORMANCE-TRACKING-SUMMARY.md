---
título: Resumo do Sistema de Performance Tracking - Documentação
tipo: DOC
código: DOC-IND-001
versão: 1.0.0
data_criação: 2025-01-18
última_atualização: 2025-01-18
próxima_revisão: 2025-07-18
responsável: bruno.chiaramonti@multisete.com
aprovador: arquitetura.dados@m7investimentos.com.br
tags: [performance, tracking, documentação, referência]
status: aprovado
confidencialidade: interno
---

# DOC-IND-001 - Resumo do Sistema de Performance Tracking

## 1. Visão Geral

Este documento apresenta um resumo consolidado da documentação do Sistema de Performance Tracking, incluindo todos os processos ETL, modelos de dados e suas inter-relações.

## 2. Documentos Relacionados

### 2.1 Tabela de Referências

| Código | Título | Tipo | Repositório | Status |
|--------|--------|------|-------------|---------|
| ETL-IND-001 | Extração de Indicadores de Performance | ETL | datawarehouse-docs | Aprovado |
| ETL-IND-002 | Extração de Atribuições de Performance | ETL | datawarehouse-docs | Aprovado |
| ETL-IND-003 | Extração de Metas de Performance | ETL | datawarehouse-docs | Aprovado |
| MOD-IND-001 | Sistema Tracking Performance KPIs | MOD | datawarehouse-docs | Aprovado |
| MOD-IND-002 | Performance Indicators Silver | MOD | datawarehouse-docs | Aprovado |
| MOD-IND-003 | Performance Assignments Silver | MOD | datawarehouse-docs | Aprovado |
| MOD-IND-004 | Performance Targets Silver | MOD | datawarehouse-docs | Aprovado |
| POL-GOV-001 | Política de Performance e Desempenho | POL | business-documentation | Aprovado |
| MAN-GES-001 | Manual de Gestão de Performance | MAN | business-documentation | Aprovado |

### 2.2 Fluxo de Dados

```
Google Sheets → ETLs → Bronze → Procedures → Silver → Gold
                 ↓        ↓         ↓           ↓        ↓
              ETL-001  QRY-001   QRY-003    MOD-002   Cálculos
              ETL-002  QRY-002              MOD-003
              ETL-003  QRY-003              MOD-004
```

## 3. Mudanças Realizadas

### 3.1 Alinhamento com Taxonomia

1. **Correção de Headers YAML**: Todos os documentos agora seguem o padrão mandatório com campos obrigatórios
2. **Correção de Códigos**: 
   - MOD-ASS-001 → MOD-IND-003
   - MOD-TAR-001 → MOD-IND-004
3. **Renomeação de Arquivos**: Arquivos físicos renomeados para corresponder aos novos códigos

### 3.2 Correção de Referências Cruzadas

- Todas as referências internas atualizadas com paths relativos corretos
- Links para queries SQL apontando para arquivos existentes
- Referências externas para políticas e manuais corrigidas

## 4. Validação de Conformidade

✅ Código segue padrão [TIPO]-[CATEGORIA]-[SEQUENCIAL]
✅ Descrição em kebab-case com máximo 5 palavras
✅ Cabeçalho YAML completo e válido
✅ Tags apropriadas incluídas
✅ Versionamento iniciado corretamente
✅ Localização correta na estrutura de pastas
✅ Referências cruzadas no formato padrão
✅ Extensão de arquivo apropriada (.md)

## 5. Próximas Ações Recomendadas

1. Atualizar queries SQL que referenciam os antigos códigos de documentação
2. Verificar se existem outros documentos que precisam ser alinhados
3. Criar processo automatizado de validação de conformidade com taxonomia

---

**Documento criado por**: Bruno Chiaramonti  
**Data**: 2025-01-18  
**Versão**: 1.0.0