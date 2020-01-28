import pyodbc 
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server = 'tcp:myserver.database.windows.net' 
database = 'mydb' 
username = 'myusername' 
password = 'mypassword' 
cnxn = pyodbc.connect('Driver={ODBC Driver 13 for SQL Server};Server=tcp:sohadev.database.windows.net,1433;Database=LE Utility;Uid=soha1;Pwd={QzPmTv713?};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;)
cursor = cnxn.cursor()
