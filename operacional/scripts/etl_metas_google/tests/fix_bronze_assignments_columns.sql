-- ==============================================================================
-- Script para adicionar colunas faltantes na tabela bronze.performance_assignments
-- ==============================================================================

USE M7Medallion;
GO

-- Adicionar coluna created_by
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
               WHERE TABLE_SCHEMA = 'bronze' 
               AND TABLE_NAME = 'performance_assignments' 
               AND COLUMN_NAME = 'created_by')
BEGIN
    ALTER TABLE bronze.performance_assignments
    ADD created_by VARCHAR(200) NULL;
    
    PRINT 'Coluna created_by adicionada com sucesso';
END
ELSE
BEGIN
    PRINT 'Coluna created_by já existe';
END
GO

-- Adicionar coluna approved_by
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
               WHERE TABLE_SCHEMA = 'bronze' 
               AND TABLE_NAME = 'performance_assignments' 
               AND COLUMN_NAME = 'approved_by')
BEGIN
    ALTER TABLE bronze.performance_assignments
    ADD approved_by VARCHAR(200) NULL;
    
    PRINT 'Coluna approved_by adicionada com sucesso';
END
ELSE
BEGIN
    PRINT 'Coluna approved_by já existe';
END
GO

-- Adicionar coluna comments
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
               WHERE TABLE_SCHEMA = 'bronze' 
               AND TABLE_NAME = 'performance_assignments' 
               AND COLUMN_NAME = 'comments')
BEGIN
    ALTER TABLE bronze.performance_assignments
    ADD comments VARCHAR(1000) NULL;
    
    PRINT 'Coluna comments adicionada com sucesso';
END
ELSE
BEGIN
    PRINT 'Coluna comments já existe';
END
GO

-- Verificar estrutura final
SELECT 
    COLUMN_NAME as [Coluna],
    DATA_TYPE as [Tipo],
    CHARACTER_MAXIMUM_LENGTH as [Tamanho],
    IS_NULLABLE as [Nullable]
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'bronze' 
AND TABLE_NAME = 'performance_assignments'
ORDER BY ORDINAL_POSITION;
GO