--1 vw_Sales	View.sql
CREATE VIEW [dbo].[vw_Sales] AS SELECT 1 AS x;

--2 usp_DoWork	Procedure.sql
CREATE OR ALTER PROCEDURE [dbo].[usp_DoWork] AS BEGIN SELECT 2 AS y; END

--3 T_Sample	Table.sql
CREATE TABLE [dbo].[T_Sample](ID int not null);

