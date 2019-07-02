import apsw, os
import re

ZsDayFilePath = 'C:\WebProgramming\ZS_Data\day'
stockCodesFilename = 'C:\WebProgramming\ServerPy3.6\StockModal\stock_codes\stock_codes.db'


def GotStockCodes():
    print("Loading...")

    filesList = os.listdir(ZsDayFilePath)
    codesList = list(map((lambda str: (re.findall("\d+", str)[0])), filesList))

    codesList = sorted(list(set(codesList)))

    newconnection = apsw.Connection(":memory:")
    newcursor = newconnection.cursor()
    newcursor.execute("create table if not exists stock %s" % "(name TEXT)")

    print("Inserting")
    for code in codesList:
        newcursor.execute("insert or ignore into stock values(?)", (code,))

    print("Syncing")
    newcursor.execute('attach database "%s" as local_db' % stockCodesFilename)
    newcursor.execute("create table if not exists local_db.stock %s" % "(name TEXT)")
    newcursor.execute("""insert or ignore into local_db.stock (name) select * from stock""")
    newcursor.execute("detach database local_db")

    print("Got stock codes done!\n")

    # clean database, except vol and amount,the open to close price data are all wrong in database
    # before this date due to struct .sina operation,need re crawl
    # newcursor.execute('delete from stock where date >= 20190101')

    """ select average,nextAverage from
        (select 
        date, average,LEAD(average,1,0) over (partition by code order by date asc) nextAverage 
        from stock 
        where  date > 20190101) 
        where (nextAverage - average)/average >0.22 """

    # "cursor.execute('select distinct  code from stock where factor > 500')"

    """
        select aCode, jan,april from
        (select 
         code as aCode , 
         fuquan_average as april
         from stock
         where date = 20190409
         ),
         (select
         code as jCode, 
         fuquan_average as jan
         from stock
         where date = 20190102)
         where aCode = jCode and april > jan *1.3
        """

    """
        select aCode from
        (select 
         code as aCode , 
         fuquan_average as a
         from stock
         where date = 20190626
         ),
         (select
         code as bCode, 
         fuquan_average as b
         from stock
         where date = 20190627),
         (select
         code as cCode, 
         fuquan_average as c
         from stock
         where date = 20190628)
         where aCode = bCode and bCode=cCode and abs(b-a)/a <0.1 and abs(c-b)/b <0.1
        """


