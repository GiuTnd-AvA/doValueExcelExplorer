from openpyxl import Workbook
import os
base = os.path.abspath(os.path.dirname(__file__))
os.makedirs(base, exist_ok=True)
path = os.path.join(base, 'objects_input.xlsx')
wb = Workbook()
ws = wb.active
ws.title = 'Oggetti'
ws.append(['Nome Oggetto', 'Tipo Oggetto', 'Script Creazione'])
ws.append(['vw_Sales', 'View', 'CREATE VIEW [dbo].[vw_Sales] AS SELECT 1 AS x;'])
ws.append(['usp_DoWork', 'Procedure', 'CREATE OR ALTER PROCEDURE [dbo].[usp_DoWork] AS BEGIN SELECT 2 AS y; END'])
ws.append(['', 'Table', 'CREATE TABLE [dbo].[T_Sample](ID int not null);'])
wb.save(path)
print(path)
