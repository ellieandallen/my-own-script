import MySQLdb
db= MySQLdb.Connect('127.0.0.1', 'root', '', 'db')
cursor = db.cursor()
cursor.execute("select version()")
data = cursor.fetchone()
sql = """select * from employee"""
try:
    cursor.execute(sql)
    lines = cursor.fetchall()
    for line in lines:
        print line
    db.commit()
except:
    db.rollback()

db.close()






