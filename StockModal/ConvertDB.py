import os, apsw

# remove name of UNIQUE, convert to a new database to solve old issue caused by stock name changing
# due to ST or shell borrowing such shit happen
_table_prop = """(name TEXT, code TEXT, date INTEGER, 
                    shares FLOAT, value FLOAT, factor INTEGER, 
                    open INTEGER, high INTEGER, close INTEGER, 
                    low INTEGER, average FLOAT, fuquan_average FLOAT,
                    UNIQUE(code, date))"""

new_table_prop = """(name TEXT, code TEXT, date INTEGER, 
                    shares FLOAT, value FLOAT, factor FLOAT, 
                    open INTEGER, high INTEGER, close INTEGER, 
                    low INTEGER, average FLOAT, fuquan_average FLOAT,
                    UNIQUE(code, date))"""

OldDB = 'E:/SA/StockAssist_0304/DB/final.db'
NewDB = 'E:/SA/StockAssist_0304/NewDB/final.db'
DB_Path = 'C:/WebProgramming/DB/final.db'
FactorNewDB = 'C:/WebProgramming/DB/FactorNewDB/final.db'

if __name__ == '__main__':
    connection = apsw.Connection(NewDB)
    cursor = connection.cursor()
    cursor.execute("create table if not exists stock %s" % _table_prop)
    cursor.execute('attach database "%s" as old_db' % OldDB)
    cursor.execute("""insert or ignore into stock (name, code, date, shares, value, factor, open, high, close,
     low, average, fuquan_average) 
     select * from old_db.stock""")
    cursor.execute("detach database old_db")
    connection.close()

    # Convert factor from Int to float
    # connection = apsw.Connection(FactorNewDB)
    # cursor = connection.cursor()
    # cursor.execute("create table if not exists stock %s" % new_table_prop)
    # cursor.execute('attach database "%s" as old_db' % DB_Path)
    # cursor.execute("create table if not exists old_stock %s" % _table_prop)
    # cursor.execute("""insert or ignore into stock (name, code, date, shares, value, factor, open, high, close,
    #      low, average, fuquan_average)
    #      select * from old_db.old_stock""")
    # cursor.execute('update stock set factor= factor/1000 ')





