-- Sample SQL for testing SqlExplorer
SELECT col1, col2
INTO [dbo].[NewTable]
FROM [dbo].[Source1] s1
INNER JOIN [dbo].[Source2] s2 ON s2.id = s1.id
LEFT JOIN "sales"."Orders" o ON o.id = s1.id;

/* Another select */
SELECT TOP 100 *
INTO dbo.OtherNew
FROM (SELECT * FROM dbo.SubSource) t
JOIN dbo.Joined ON t.id = Joined.id;
