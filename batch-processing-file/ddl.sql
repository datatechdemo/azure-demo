
-- Drop schema
IF EXISTS (
		SELECT *
		FROM sys.schemas
		WHERE name = 'active'
		)
	DROP SCHEMA [active]
GO

-- Add schema
CREATE SCHEMA [active] AUTHORIZATION [dbo]
GO



CREATE TABLE [active].[customer_acquistion_data] (
	[customer_id] INT NOT NULL
	,[relationship_manager_id] INT NOT NULL
	,[last_updated] DATE NOT NULL
	,[deposit_amount] DECIMAL NOT NULL
	)
WITH  
  (   
    DISTRIBUTION = HASH (customer_id),  
    CLUSTERED COLUMNSTORE INDEX
  )  
GO
