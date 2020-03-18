PRINT 'Starting Users Synchronization'
GO
MERGE INTO Users AS Target 
USING (VALUES 
	(1, 'Weeks', 'Ty', '1 E. 1st St', 'Spokane'),
	(2, 'Meharg', 'Glen', '2 E. 2nd Ave', 'Tacoma')
) 
AS Source (UserID, LastName, FirstName, Address, City) 
ON Target.UserId = Source.UserId 
WHEN MATCHED 
THEN 
	UPDATE 
	SET LastName = Source.LastName, 
		FirstName = Source.FirstName,
		Address = Source.Address,
		City = Source.City
WHEN NOT MATCHED BY TARGET 
THEN 
	INSERT (UserID, LastName, FirstName, Address, City) 
	VALUES (Source.UserID, Source.LastName, Source.FirstName, Source.Address, Source.City);