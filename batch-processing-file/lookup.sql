DECLARE @StartDate DATE = (
		SELECT dateadd(day, - 1, CONVERT(VARCHAR(10), [process_date_hour], 126)) [LAST_UPDATE]
		FROM [active].[current_watermark]
		)
DECLARE @EndDate DATE = (
		SELECT cast(DATEADD(day, - 1, getdate()) AS DATE) AS StartDate
		)

SELECT [LAST_UPDATE]
	,CONCAT (
		'customers_'
		,[LAST_UPDATE]
		) AS [FILE_NAME]
	,CONCAT (
		'data/customer/'
		,REPLACE(CONVERT(VARCHAR(7), [LAST_UPDATE], 112), '-', '/')
		) AS [DATA_DIR]
FROM (
	SELECT TOP (DATEDIFF(DAY, @StartDate, @EndDate)) [LAST_UPDATE] = convert(VARCHAR(10), cast(DATEADD(DAY, ROW_NUMBER() OVER (
						ORDER BY a.object_id
						), @StartDate) AS DATE), 126)
	FROM sys.all_objects a
	CROSS JOIN sys.all_objects b
	) A
ORDER BY [LAST_UPDATE] ASC
