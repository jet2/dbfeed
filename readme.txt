К каждому винсиси подключен клиент орс
https://github.com/jet2/sharp_opc_post
на основе симесовского примера(dotnet 3.5 и vs 2015 community edition)
, который собирает данные в минутные файлы.
Отдельный поток в клиенте ежеминутно отправляет все найденные в папке файлы,
кроме текущей минуты, на вебсервер. На вебсервере
https://github.com/jet2/dbfeed
(flask + pyodbc)
отдельньй "поток" вебсервера
собирает файлы строго по 5 минут(с Х0 по Х4 и с Х5 по Х9), отсеивает
"не-мастерские" тэги и рассчитывает результат по каждому отдельному тэгу.
Затем, пишет результат в таблицу в виде: начало, конец, имятэга, расчетное значение
 тэга.
USE [wccdata]
GO

/****** Object:  Table [dbo].[opc_data]    Script Date: 07/17/2020 05:01:46 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[opc_data](
	[dt_begin] [datetime] NOT NULL,
	[dt_end] [datetime] NOT NULL,
	[tagname] [nvarchar](50) NOT NULL,
	[tagvalue] [float] NOT NULL
) ON [PRIMARY]

GO

настройки сервера в файле settings.py
webport - порт на котором слушает вебсервер.
настройки подключения к mssql в словаре 
mssql_connection = {"hostname": 'PC-LITE',
                      "database": 'zzz',
                      "username": 'sa',
                      "password": '123'}


run_app.bat запускает вебсервер

config.txt
http://snr-ks-redis:8080/addfile
@RM_MASTER
23_1-1_1/23_1-1_1_RUN$Out#Value
23_1-1_2/23_1-1_2_RUN$Out#Value
...
