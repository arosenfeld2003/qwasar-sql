# my-sqlite


These projects were completed in order to understand SQL and databases, specifically related to building web apps using Ruby on Rails.

MyRedisClass acts like a Key-Value database, and creates or loads from a data-storage dump file. MySqliteRequest and MySelectQuery mimic making database requests using SQL.  

My_sqlite_cli is an interface allowing the user to call these methods from the command line.  It is run by calling `ruby my_sqlite_cli.rb` from the terminal. The queries can be any of the following commands:

'SELECT', 'INSERT INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'FROM', 'WHERE', 'JOIN', 'ON'

Example Usage:
SELECT * FROM filename.csv WHERE columnName = value
SELECT * FROM file1.csv JOIN (file2.csv) WHERE (column1 = column2)
SELECT * FROM file1.csv JOIN file2.csv ON column = value


