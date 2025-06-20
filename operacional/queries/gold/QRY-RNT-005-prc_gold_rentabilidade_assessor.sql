SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [gold].[prc_gold_rentabilidade_assessor]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Inicia transação
        BEGIN TRANSACTION;
        
        -- Trunca a tabela para limpar dados antigos
        TRUNCATE TABLE gold.rentabilidade_assessor;
        
        -- Insere dados atualizados da view
        INSERT INTO gold.rentabilidade_assessor (
            	[ano_mes],
	[ano],
	[mes],
	[nome_mes],
	[trimestre],
	[semestre],
	[cod_assessor],
	[codigo_crm_assessor],
	[nome_assessor],
	[nivel_assessor],
	[estrutura_id],
	[estrutura_nome],
	[qtd_clientes_300k_mais],
	[qtd_clientes_acima_cdi],
	[qtd_clientes_faixa_80_cdi],
	[qtd_clientes_faixa_50_cdi],
	[qtd_clientes_rentabilidade_positiva],
	[perc_clientes_acima_cdi]

        )
        SELECT 
                            	[ano_mes],
	[ano],
	[mes],
	[nome_mes],
	[trimestre],
	[semestre],
	[cod_assessor],
	[codigo_crm_assessor],
	[nome_assessor],
	[nivel_assessor],
	[estrutura_id],
	[estrutura_nome],
	[qtd_clientes_300k_mais],
	[qtd_clientes_acima_cdi],
	[qtd_clientes_faixa_80_cdi],
	[qtd_clientes_faixa_50_cdi],
	[qtd_clientes_rentabilidade_positiva],
	[perc_clientes_acima_cdi]

        FROM gold.vw_rentabilidade_assessor;

        
        -- Conta registros inseridos
        DECLARE @RowCount INT = @@ROWCOUNT;
        
        -- Confirma transação
        COMMIT TRANSACTION;
        
        -- Retorna mensagem de sucesso
        PRINT 'Procedure executada com sucesso!';
        PRINT 'Total de registros inseridos: ' + CAST(@RowCount AS VARCHAR(10));
        
    END TRY
    BEGIN CATCH
        -- Em caso de erro, desfaz a transação
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Captura informações do erro
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        -- Relança o erro
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO
