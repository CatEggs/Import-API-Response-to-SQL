import config
import pyodbc
        
connection = pyodbc.connect(
r'DRIVER={SQL Server Native Client 11.0};'
r'SERVER=' + config.server + ';'
r'DATABASE=' + config.database + ';'
r'UID=' + config.username + ';'
r'PWD=' + config.password
)

cursor = connection.cursor()

# Create JitBit backup table
jitbit_backup = 'DROP TABLE IF EXISTS JitBit_BackUp; SELECT * INTO JitBit_BackUp FROM JitBit' 
jitbit_mgmt_backup ='DROP TABLE IF EXISTS JitBit_Mgmt_BackUp; SELECT * INTO JitBit_Mgmt_BackUp FROM JitBit_Mgmt;'
cursor.execute(jitbit_backup)
cursor.execute(jitbit_mgmt_backup)
cursor.commit()