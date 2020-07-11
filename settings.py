webport=8080
mssql_connection = {"hostname": 'tcp:192.168.0.121,1433',
                      "database": 'winccdata',
                      "username": 'sa',
                      "password": '123'}
mssql_connectionn = {"hostname": 'SNR-KS-REDIS\SQLEXPRESSREDIS',
                      "database": 'wccdata',
                      "username": 'sa',
                      "password": 'Zaq12wsXCde3'}
# USE [zzz]
# GO
# /****** Object:  Table [dbo].[opc_data]    Script Date: 02/22/2020 16:17:10 ******/
# SET ANSI_NULLS ON
# GO
# SET QUOTED_IDENTIFIER ON
# GO
# SET ANSI_PADDING ON
# GO
# CREATE TABLE [dbo].[opc_data](
# 	[date_time] [datetime] NULL,
# 	[source_name] [varchar](255) COLLATE Cyrillic_General_CI_AS NULL,
# 	[opc_name] [varchar](255) COLLATE Cyrillic_General_CI_AS NOT NULL,
# 	[cur_value] [float] NULL,
# 	[prev_value] [float] NULL
# ) ON [PRIMARY]
#
# GO
# SET ANSI_PADDING OFF

postgreconnection = {'database': 'dbfeed',
                     'username': 'data_collector',
                     'password': '88005553535',
                     'ipaddress': 'localhost',
                     'port': 5432}