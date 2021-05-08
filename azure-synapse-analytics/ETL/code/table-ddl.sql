CREATE TABLE dbo.greentaxi (
VendorID varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
PassengerCount bigint,
TripDistance bigint,
PickupTime DATETIME, 
DropTime DATETIME, 
PickupLocationId varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
DropLocationId varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,   
TotalAmount bigint, 
PaymentType varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
TripYear integer,  
TripMonth integer,  
TripDay integer,  
TripTimeInMinutes FLOAT,
TripType varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL 
);
