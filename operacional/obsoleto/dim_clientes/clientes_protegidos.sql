CREATE TABLE investimentos.clientes_investimentos (
    id_cliente INT IDENTITY(1,1) PRIMARY KEY,
    nome NVARCHAR(255) NULL,
    cod_xp INT NOT NULL,
    email NVARCHAR(255) NULL,
    telefone NVARCHAR(255) NULL,
    data_nascimento DATE NULL,
    cpf NVARCHAR(255) NULL
);