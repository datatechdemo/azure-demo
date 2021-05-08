CREATE PROC [dbo].[UpdatePaymentType]
AS
BEGIN
UPDATE [dbo].[greentaxi]
SET PaymentType = 2
END
