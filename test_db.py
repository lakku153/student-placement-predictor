import pypyodbc as odbc

DRIVER_NAME= 'SQL SERVER'
SERVER_NAME= 'CPPL28\\SQLEXPRESS'
DATABASE_NAME= 'student_placement'

connection_string=f'''
    DRIVER={{{DRIVER_NAME}}};
    SERVER={SERVER_NAME};
    DATABASE={DATABASE_NAME};
    Trust_Connection=yes;
'''
try:
    conn=odbc.connect(connection_string)
    print("Connection Success")
except Exception as e:
    print("Connection failed",e)

