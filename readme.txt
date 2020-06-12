орс клиент формирует файлы, содержимое которых обрабатыватся вебсервером flask  на питоне. 
Полученный строковый параметр разбирается на строки, в которых хранятся json объекты. Поля объекта => параметры запроса insert => поля записи в БД/
 
настройки сервера в файле settings.py
webport - порт на котором слушает вебсервер.
настройки подключения к mssql в словаре 
mssql_connection = {"hostname": 'PC-LITE',
                      "database": 'zzz',
                      "username": 'sa',
                      "password": '123'}
структура таблицы приведена в виде запроса create.

run.bat запускает вебсервер

клиент запускается на пк c wincc.
пакет данных от клиента, построчно записывается в remote БД mssql, запросами insert


USE [wccdata]
GO
/****** Объект:  Table [dbo].[opc_data]    Дата сценария: 03/23/2020 02:48:37 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[opc_data](
	[dt] [datetime] NULL,
	[source_name] [nvarchar](50) NULL,
	[tag_name] [nvarchar](50) NULL,
	[new_value] [real] NULL,
	[old_value] [real] NULL,
	[tag_type] [int] NULL
) ON [PRIMARY]

GO

