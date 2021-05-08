CREATE PROC [dbo].[UpdateRateCodeID]
AS
BEGIN
UPDATE [dbo].[greentaxi]
SET RatecodeID = 2
END
