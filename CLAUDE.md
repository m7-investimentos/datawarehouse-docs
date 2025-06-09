# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Data Warehouse Documentation project for M7 Investimentos following a medallion architecture (Bronze → Silver → Gold layers). The project focuses on tracking and analyzing "captação líquida" (net funding/capital flows) for investment advisors.

## Architecture

- **Bronze Layer**: Raw data from source systems (XP Investimentos, Open Investment, etc.)
- **Silver Layer**: Cleaned, transformed facts and dimensions
- **Gold Layer**: Business-ready aggregated views and tables, divided into:
  - **gold_performance**: Performance metrics and KPIs for business analysis
  - **gold_llm**: Data optimized for Large Language Models and AI applications

## File Naming Conventions

### SQL Query Files (MANDATORY)
All SQL queries MUST follow the template located at `/Users/bchiaramonti/Downloads/QRY-template.sql`. This template is REQUIRED for all new queries.

Query naming pattern: `QRY-[AREA]-[SEQUENTIAL]-[description].sql`

Area codes:
- CAP: Captação (Funding)
- CDI: CDI rate
- CLI: Clientes (Clients)
- PES: Pessoas (People)
- EST: Estruturas (Structures)
- CAL: Calendário (Calendar)
- RES: Resgates (Withdrawals)
- ESP: Estrutura Pessoas
- TRF: Transferências
- IPCA: IPCA inflation index
- OIN: Open Investment
- POS: Positivador

### Documentation Files
- `ARQ-###-[description].md`: Architecture documents
- `MOD-###-[description].md`: Data models
- `ETL-###-[description].md`: ETL processes
- `IND-###-[description].md`: Performance indicators

## Query Template Requirements

**ALL queries created in this project MUST use the standard template**. The template includes 10 mandatory sections:

1. Header with metadata (type, version, author, tags, status)
2. Objective and use cases
3. Input parameters documentation
4. Output structure documentation
5. Dependencies listing
6. Configuration and optimizations
7. CTEs with descriptions
8. Main query with organized sections
9. Auxiliary validation queries
10. Change history and notes

Key template sections to customize:
- Replace `[nome.sobrenome@m7investimentos.com.br]` with `bruno.chiaramonti@multisete.com`
- Database type: `SQL Server`
- Schema: Use `bronze`, `silver`, `gold_performance`, or `gold_llm` as appropriate
- Status: Options are `rascunho`, `desenvolvimento`, `produção`

## Database Standards

- **Platform**: SQL Server (Microsoft)
- **Database**: M7Medallion (primary), M7InvestimentosOLAP (legacy)
- **Schemas**: bronze, silver, gold_performance, gold_llm, modelagem_b2s, fato
- **Required for all tables**:
  - Extended properties for documentation (MS_Description)
  - Primary keys with clustered indexes
  - Foreign key constraints between layers (except Bronze)
  
## Common SQL Server Commands

### Database Context
```sql
USE [M7Medallion];  -- Primary database for medallion architecture
-- Legacy: USE [M7InvestimentosOLAP];
```

### Creating Tables/Views
```sql
-- Drop existing object if needed
IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'[schema].[table_name]'))
    DROP TABLE [schema].[table_name]
GO

-- Create with extended properties
CREATE TABLE [schema].[table_name] (...)
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'description' ...
```

### ETL Procedures
```sql
-- Bronze to Silver
EXEC [modelagem_b2s].[prc_bronze_to_silver_[entity]];

-- Stage to Fact
EXEC [fato].[prc_stage2fact_[entity]];

-- Gold layer procedures
EXEC [dbo].[prc_gold_performance_to_table_[entity]] @debug = 1;
```

### Common Development Tasks

### Creating a new Bronze table
1. Use naming: `QRY-[AREA]-001-create_bronze_[source]_[entity].sql`
2. Include extended properties for all columns
3. No foreign keys in bronze layer

### Creating a new Silver table
1. Use naming: `QRY-[AREA]-001-create_silver_[fact|dim]_[entity].sql`
2. Add foreign key constraints to dimensions
3. Include extended properties documentation

### Creating a new Gold view/table
1. Views: `QRY-[AREA]-001-create_gold_performance_view_[business_metric].sql`
2. Tables: `QRY-[AREA]-002-create_gold_performance_[business_metric].sql`
3. Procedures: `QRY-[AREA]-003-prc_gold_performance_to_table_[business_metric].sql`
4. Include CTEs for complex logic
5. Add comprehensive documentation in header
6. Choose appropriate schema:
   - Use `gold_performance` for business KPIs and metrics
   - Use `gold_llm` for AI/ML optimized data structures

## Key Business Entities

- **Assessores** (Advisors): Track via cod_assessor/cod_aai
- **Captação** (Funding): Inflows from clients
- **Resgates** (Withdrawals): Outflows from clients  
- **Estruturas** (Organizational structures): Hierarchical teams
- **Pessoas** (People): Master data for all individuals
- **Transferências** (Transfers): Client transfers between advisors/offices

### Key Business Metrics
- **Captação Líquida**: captacao_bruta - resgate_bruto
- **Ticket Médio**: Average transaction value
- **Comportamento Cliente**: Analysis of client behavior (only deposits, only withdrawals, both)

## SQL Patterns to Follow

1. Always use `COALESCE()` for handling NULLs in aggregations
2. Use CTEs for complex queries (named descriptively)
3. Include comments for business logic sections
4. Group columns by type in SELECT (dimensions, metrics, calculations)
5. Use extended properties for table/column documentation

## Version Control

- Main branch: `main`
- Always update version history in queries (section 9 of template)
- Author email: bruno.chiaramonti@multisete.com

## Data Flow and ETL Patterns

1. Source systems → Bronze (daily loads)
2. Bronze → Silver (transformation procedures)
3. Silver → Gold (business views/aggregations)

Always verify data lineage when modifying queries - changes cascade downstream.

### Common ETL Patterns

#### Incremental Loading
```sql
WHERE NOT EXISTS (
    SELECT 1 FROM [target_table] t
    WHERE t.key_col = source.key_col
)
```

#### Rolling Aggregations
```sql
WITH base AS (...),
     mensal AS (...),
     acumulado_3m AS (...),
     acumulado_12m AS (...)
```

#### MERGE Pattern for Updates
```sql
MERGE [target] AS destino
USING [source] AS origem
ON (destino.key = origem.key)
WHEN MATCHED AND (conditions) THEN UPDATE SET ...
WHEN NOT MATCHED BY TARGET THEN INSERT ...
WHEN NOT MATCHED BY SOURCE THEN DELETE;
```

## Project State and Migration

- **Current**: SQL Server with medallion architecture in M7Medallion database
- **Legacy**: M7InvestimentosOLAP database (being phased out)
- **Future**: Planning migration to Google BigQuery
- **Scripts Directory**: Currently empty, awaiting migration from legacy system