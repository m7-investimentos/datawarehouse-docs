-- ================================================================================
-- ANÁLISE COMPARATIVA DAS ESTRUTURAS DAS TABELAS
-- bronze.performance_assignments vs silver.performance_assignments
-- ================================================================================

-- 1. ESTRUTURA DAS COLUNAS - BRONZE
PRINT '=========================================='
PRINT '1. ESTRUTURA DA TABELA BRONZE'
PRINT '=========================================='

SELECT 
    c.COLUMN_NAME,
    c.DATA_TYPE + 
    CASE 
        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN 
            '(' + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN 'MAX' ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) END + ')'
        WHEN c.NUMERIC_PRECISION IS NOT NULL THEN 
            '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + 
            CASE WHEN c.NUMERIC_SCALE > 0 THEN ',' + CAST(c.NUMERIC_SCALE AS VARCHAR) ELSE '' END + ')'
        ELSE ''
    END AS DATA_TYPE_FULL,
    c.IS_NULLABLE,
    c.COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = 'bronze' AND c.TABLE_NAME = 'performance_assignments'
ORDER BY c.ORDINAL_POSITION;

-- 2. ESTRUTURA DAS COLUNAS - SILVER
PRINT ''
PRINT '=========================================='
PRINT '2. ESTRUTURA DA TABELA SILVER'
PRINT '=========================================='

SELECT 
    c.COLUMN_NAME,
    c.DATA_TYPE + 
    CASE 
        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN 
            '(' + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN 'MAX' ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) END + ')'
        WHEN c.NUMERIC_PRECISION IS NOT NULL THEN 
            '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + 
            CASE WHEN c.NUMERIC_SCALE > 0 THEN ',' + CAST(c.NUMERIC_SCALE AS VARCHAR) ELSE '' END + ')'
        ELSE ''
    END AS DATA_TYPE_FULL,
    c.IS_NULLABLE,
    c.COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = 'silver' AND c.TABLE_NAME = 'performance_assignments'
ORDER BY c.ORDINAL_POSITION;

-- 3. COMPARAÇÃO DAS COLUNAS
PRINT ''
PRINT '=========================================='
PRINT '3. COMPARAÇÃO DAS COLUNAS'
PRINT '=========================================='

-- Colunas apenas em BRONZE
PRINT ''
PRINT 'Colunas APENAS em BRONZE:'
SELECT 'BRONZE ONLY: ' + b.COLUMN_NAME AS COLUMN_DIFF
FROM INFORMATION_SCHEMA.COLUMNS b
WHERE b.TABLE_SCHEMA = 'bronze' AND b.TABLE_NAME = 'performance_assignments'
AND NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS s
    WHERE s.TABLE_SCHEMA = 'silver' AND s.TABLE_NAME = 'performance_assignments'
    AND s.COLUMN_NAME = b.COLUMN_NAME
)
ORDER BY b.COLUMN_NAME;

-- Colunas apenas em SILVER
PRINT ''
PRINT 'Colunas APENAS em SILVER:'
SELECT 'SILVER ONLY: ' + s.COLUMN_NAME AS COLUMN_DIFF
FROM INFORMATION_SCHEMA.COLUMNS s
WHERE s.TABLE_SCHEMA = 'silver' AND s.TABLE_NAME = 'performance_assignments'
AND NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS b
    WHERE b.TABLE_SCHEMA = 'bronze' AND b.TABLE_NAME = 'performance_assignments'
    AND b.COLUMN_NAME = s.COLUMN_NAME
)
ORDER BY s.COLUMN_NAME;

-- Colunas com tipos diferentes
PRINT ''
PRINT 'Colunas com TIPOS DIFERENTES:'
SELECT 
    b.COLUMN_NAME,
    'BRONZE: ' + b.DATA_TYPE + 
    CASE 
        WHEN b.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN 
            '(' + CASE WHEN b.CHARACTER_MAXIMUM_LENGTH = -1 THEN 'MAX' ELSE CAST(b.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) END + ')'
        WHEN b.NUMERIC_PRECISION IS NOT NULL THEN 
            '(' + CAST(b.NUMERIC_PRECISION AS VARCHAR) + 
            CASE WHEN b.NUMERIC_SCALE > 0 THEN ',' + CAST(b.NUMERIC_SCALE AS VARCHAR) ELSE '' END + ')'
        ELSE ''
    END AS BRONZE_TYPE,
    'SILVER: ' + s.DATA_TYPE + 
    CASE 
        WHEN s.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN 
            '(' + CASE WHEN s.CHARACTER_MAXIMUM_LENGTH = -1 THEN 'MAX' ELSE CAST(s.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) END + ')'
        WHEN s.NUMERIC_PRECISION IS NOT NULL THEN 
            '(' + CAST(s.NUMERIC_PRECISION AS VARCHAR) + 
            CASE WHEN s.NUMERIC_SCALE > 0 THEN ',' + CAST(s.NUMERIC_SCALE AS VARCHAR) ELSE '' END + ')'
        ELSE ''
    END AS SILVER_TYPE
FROM INFORMATION_SCHEMA.COLUMNS b
INNER JOIN INFORMATION_SCHEMA.COLUMNS s ON b.COLUMN_NAME = s.COLUMN_NAME
WHERE b.TABLE_SCHEMA = 'bronze' AND b.TABLE_NAME = 'performance_assignments'
AND s.TABLE_SCHEMA = 'silver' AND s.TABLE_NAME = 'performance_assignments'
AND (
    b.DATA_TYPE != s.DATA_TYPE OR
    ISNULL(b.CHARACTER_MAXIMUM_LENGTH, 0) != ISNULL(s.CHARACTER_MAXIMUM_LENGTH, 0) OR
    ISNULL(b.NUMERIC_PRECISION, 0) != ISNULL(s.NUMERIC_PRECISION, 0) OR
    ISNULL(b.NUMERIC_SCALE, 0) != ISNULL(s.NUMERIC_SCALE, 0)
)
ORDER BY b.COLUMN_NAME;

-- 4. CONSTRAINTS
PRINT ''
PRINT '=========================================='
PRINT '4. CONSTRAINTS'
PRINT '=========================================='

-- Constraints BRONZE
PRINT ''
PRINT 'BRONZE Constraints:'
SELECT 
    tc.CONSTRAINT_NAME,
    tc.CONSTRAINT_TYPE,
    STRING_AGG(kcu.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY kcu.ORDINAL_POSITION) as COLUMNS
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
    AND tc.TABLE_NAME = kcu.TABLE_NAME
WHERE tc.TABLE_SCHEMA = 'bronze' AND tc.TABLE_NAME = 'performance_assignments'
GROUP BY tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE
ORDER BY tc.CONSTRAINT_TYPE, tc.CONSTRAINT_NAME;

-- Constraints SILVER
PRINT ''
PRINT 'SILVER Constraints:'
SELECT 
    tc.CONSTRAINT_NAME,
    tc.CONSTRAINT_TYPE,
    STRING_AGG(kcu.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY kcu.ORDINAL_POSITION) as COLUMNS
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
    AND tc.TABLE_NAME = kcu.TABLE_NAME
WHERE tc.TABLE_SCHEMA = 'silver' AND tc.TABLE_NAME = 'performance_assignments'
GROUP BY tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE
ORDER BY tc.CONSTRAINT_TYPE, tc.CONSTRAINT_NAME;

-- 5. FOREIGN KEYS DETALHADAS
PRINT ''
PRINT '=========================================='
PRINT '5. FOREIGN KEYS DETALHADAS'
PRINT '=========================================='

-- FKs SILVER (Bronze não tem FKs)
PRINT ''
PRINT 'SILVER Foreign Keys:'
SELECT 
    fk.name AS FK_NAME,
    COL_NAME(fc.parent_object_id, fc.parent_column_id) AS COLUMN_NAME,
    '->' AS REFERENCIA,
    OBJECT_SCHEMA_NAME(fk.referenced_object_id) + '.' + 
    OBJECT_NAME(fk.referenced_object_id) + '.' + 
    COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS REFERENCED
FROM sys.foreign_keys AS fk
INNER JOIN sys.foreign_key_columns AS fc ON fk.object_id = fc.constraint_object_id
WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = 'silver' 
AND OBJECT_NAME(fk.parent_object_id) = 'performance_assignments';

-- 6. ÍNDICES
PRINT ''
PRINT '=========================================='
PRINT '6. ÍNDICES'
PRINT '=========================================='

-- Índices BRONZE
PRINT ''
PRINT 'BRONZE Indexes:'
SELECT 
    i.name AS INDEX_NAME,
    i.type_desc AS INDEX_TYPE,
    CASE WHEN i.is_unique = 1 THEN 'UNIQUE' ELSE '' END AS IS_UNIQUE,
    CASE WHEN i.is_primary_key = 1 THEN 'PRIMARY KEY' ELSE '' END AS IS_PK,
    STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS COLUMNS
FROM sys.indexes i
INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE OBJECT_SCHEMA_NAME(i.object_id) = 'bronze' 
AND OBJECT_NAME(i.object_id) = 'performance_assignments'
AND i.type > 0
GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key
ORDER BY i.name;

-- Índices SILVER
PRINT ''
PRINT 'SILVER Indexes:'
SELECT 
    i.name AS INDEX_NAME,
    i.type_desc AS INDEX_TYPE,
    CASE WHEN i.is_unique = 1 THEN 'UNIQUE' ELSE '' END AS IS_UNIQUE,
    CASE WHEN i.is_primary_key = 1 THEN 'PRIMARY KEY' ELSE '' END AS IS_PK,
    STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS COLUMNS
FROM sys.indexes i
INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE OBJECT_SCHEMA_NAME(i.object_id) = 'silver' 
AND OBJECT_NAME(i.object_id) = 'performance_assignments'
AND i.type > 0
GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key
ORDER BY i.name;

-- 7. MAPEAMENTO DE COLUNAS PARA TRANSFORMAÇÃO
PRINT ''
PRINT '=========================================='
PRINT '7. MAPEAMENTO BRONZE -> SILVER'
PRINT '=========================================='

SELECT 
    'Bronze: ' + b.COLUMN_NAME + ' (' + b.DATA_TYPE + ')' AS BRONZE_COLUMN,
    CASE 
        WHEN s.COLUMN_NAME IS NOT NULL THEN 'Silver: ' + s.COLUMN_NAME + ' (' + s.DATA_TYPE + ')'
        WHEN b.COLUMN_NAME = 'indicator_code' THEN '-> Necessita JOIN com performance_indicators para obter indicator_id'
        WHEN b.COLUMN_NAME = 'weight' THEN '-> Mapeia para indicator_weight (converter para decimal)'
        WHEN b.COLUMN_NAME = 'is_active' THEN '-> Converter de varchar para bit'
        WHEN b.COLUMN_NAME = 'valid_from' THEN '-> Converter de varchar para date'
        WHEN b.COLUMN_NAME = 'valid_to' THEN '-> Converter de varchar para date'
        WHEN b.COLUMN_NAME = 'created_by' THEN '-> Mapeia direto (truncar para 100 chars)'
        WHEN b.COLUMN_NAME = 'approved_by' THEN '-> Mapeia direto (truncar para 100 chars)'
        WHEN b.COLUMN_NAME = 'comments' THEN '-> Converter de varchar para nvarchar'
        WHEN b.COLUMN_NAME = 'load_id' THEN '-> Mapeia para bronze_load_id'
        ELSE '-> Não mapeado para silver'
    END AS MAPPING
FROM INFORMATION_SCHEMA.COLUMNS b
LEFT JOIN INFORMATION_SCHEMA.COLUMNS s 
    ON s.TABLE_SCHEMA = 'silver' 
    AND s.TABLE_NAME = 'performance_assignments'
    AND s.COLUMN_NAME = b.COLUMN_NAME
WHERE b.TABLE_SCHEMA = 'bronze' 
AND b.TABLE_NAME = 'performance_assignments'
ORDER BY b.ORDINAL_POSITION;