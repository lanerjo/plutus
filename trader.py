"""Copyright (c) 2016  Ad Infinitum Ergon (http://adinfinitumergon.com)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated 
documentation files (the “Software”), to deal in the Software without restriction, including without limitation 
the rights to use, copy, modify, merge, publish, distribute, and/or sublicense copies of the Software, 
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions 
of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED 
TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF 
CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
DEALINGS IN THE SOFTWARE.
"""

import time
import psycopg2
import requests
import json
from datetime import datetime

class StockScraper():
    def __init__(self):
        #self.stock = stock
        #self.conn = self.connect_to_central()
        #self.cur = self.give_me_a_cursor()
        pass
    
    def connect_to_central(self):
        conn = psycopg2.connect("dbname='atlas' user='postgres' host='127.0.0.1' password='postgres'")
        return conn

    def give_me_a_cursor(self, conn):
        cur = conn.cursor()
        return cur

    def get_stock_object(self, stock, stocks_dict): 
        '''
        id     : ID,
        t      : StockSymbol,
        e      : Index,
        l      : LastTradePrice,
        l_self.cur  : LastTradeWithCurrency,
        ltt    : LastTradeTime,
        lt_dts : LastTradeDateTime,
        lt     : LastTradeDateTimeLong,
        div    : Dividend,
        yld    : Yield,
        s      : LastTradeSize,
        c      : Change,
        c      : ChangePercent,
        el     : ExtHrsLastTradePrice,
        el_self.cur : ExtHrsLastTradeWithCurrency,
        elt    : ExtHrsLastTradeDateTimeLong,
        ec     : ExtHrsChange,
        ecp    : ExtHrsChangePercent,
        pcls_fix: PreviousClosePrice
        '''
        
        get_stock = requests.get(("http://finance.google.com/finance/info?client=ig&q=%s" % stock))

        try:
            content_object = str(get_stock.text).strip().replace("\n", "").replace("//","").replace("[", "").replace("]","")
            json_object = json.loads(content_object)
        except:
            json_object = ""

        if "id" in json_object:
            idg = json_object["id"]
        else:
            idg = 0

        if "t" in json_object:
            stocksymbol = json_object["t"]
        else:
            stocksymbol = "None"

        if "e" in json_object:
            index = json_object["e"]
        else:
            index = "None"

        if "l" in json_object:
            lasttradeprice = json_object["l"]
        else:
            lasttradeprice = 0

        if "l_self.cur" in json_object:
            lasttradewithcurrency = json_object["l_self.cur"]
        else:
            lasttradewithcurrency = 0

        if "lt_dts" in json_object:
            lasttradedatetime = json_object["lt_dts"]
        else:
            lasttradedatetime = "2010-01-01 00:00:00"

        if "div" in json_object:
            dividend = json_object["div"]
        else:
            dividend = 0

        if "yld" in json_object:
            yields = json_object["yld"]
        else:
            yields = 0

        if "s" in json_object:
            lasttradesize = json_object["s"]
        else:
            lasttradesize = 0

        if "c" in json_object:
            change = json_object["c"]
        else:
            change = 0

        if "cp" in json_object:
            changepercent = json_object["cp"]
        else:
            changepercent = 0

        if "el" in json_object:
            exthrslasttradeprice = json_object["el"]
        else:
            exthrslasttradeprice = 0

        if "el_self.cur" in json_object:
            exthrslasttradewithcurrency = json_object["el_self.cur"]
        else:
            exthrslasttradewithcurrency = 0

        if "ec" in json_object:
            exthrschange = json_object["ec"]
        else:
            exthrschange = 0

        if "ecp" in json_object:
            exthrschangepercent = json_object["ecp"]
        else:
            exthrschangepercent = 0

        if "pcls_fix" in json_object:
            perciouscloseprice = json_object["pcls_fix"]
        else:
            perciouscloseprice = 0

        if stock not in stocks_dict:
            stocks_dict[stock] = {}
            stocks_dict[stock] = {
                    "idg":idg,
                    "stocksymbol":stocksymbol,
                    "index":index,
                    "lasttradeprice":lasttradeprice,
                    "lasttradewithcurrency":lasttradewithcurrency,
                    "lasttradedatetime":lasttradedatetime,
                    "dividend":dividend,
                    "yields":yields,
                    "lasttradesize":lasttradesize,
                    "change":change,
                    "changepercent":changepercent,
                    "exthrslasttradeprice":exthrslasttradeprice,
                    "exthrslasttradewithcurrency":exthrslasttradewithcurrency,
                    "exthrschange":exthrschange,
                    "exthrschangepercent":exthrschangepercent,
                    "perciouscloseprice":perciouscloseprice
                }
        else:
            stocks_dict[stock] = {
                    "idg":idg,
                    "stocksymbol":stocksymbol,
                    "index":index,
                    "lasttradeprice":lasttradeprice,
                    "lasttradewithcurrency":lasttradewithcurrency,
                    "lasttradedatetime":lasttradedatetime,
                    "dividend":dividend,
                    "yields":yields,
                    "lasttradesize":lasttradesize,
                    "change":change,
                    "changepercent":changepercent,
                    "exthrslasttradeprice":exthrslasttradeprice,
                    "exthrslasttradewithcurrency":exthrslasttradewithcurrency,
                    "exthrschange":exthrschange,
                    "exthrschangepercent":exthrschangepercent,
                    "perciouscloseprice":perciouscloseprice
                }

        return stocks_dict

    def update_stock_obeject(self, stock, stocks_dict):
        conn = self.connect_to_central()
        cur = self.give_me_a_cursor(conn)
        stocks_dict[stock]["lasttradedatetime"]

        date = stocks_dict[stock]["lasttradedatetime"].replace("T"," ").replace("Z", "") 

        if date:
            date_object = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        else:
            date_object = datetime.now()

        if stocks_dict[stock]["idg"]: 
            idg = stocks_dict[stock]["idg"]
        else:
            idg=0

        if stocks_dict[stock]["stocksymbol"]:
            stocksymbol = stocks_dict[stock]["stocksymbol"]
        else:
            stocksymbol = "NONE"

        if stocks_dict[stock]["index"]:
            index = stocks_dict[stock]["index"]
        else:
            index="NONE"

        if stocks_dict[stock]["lasttradeprice"]:
            lasttradeprice = stocks_dict[stock]["lasttradeprice"]
        else:
            lasttradeprice=0

        if stocks_dict[stock]["lasttradewithcurrency"]:  
            lasttradewithcurrency = stocks_dict[stock]["lasttradewithcurrency"]
        else:
            lasttradewithcurrency=0

        if stocks_dict[stock]["dividend"]:
            dividend = stocks_dict[stock]["dividend"]
        else:
            dividend=0

        if stocks_dict[stock]["yields"]:   
            yields = stocks_dict[stock]["yields"]
        else:
            yields=0

        if stocks_dict[stock]["lasttradesize"]:
            lasttradesize = stocks_dict[stock]["lasttradesize"]
        else:
            lasttradesize=0

        if stocks_dict[stock]["change"]:
            change = stocks_dict[stock]["change"]
        else:
            change=0

        if stocks_dict[stock]["changepercent"]:
            changepercent = stocks_dict[stock]["changepercent"]
        else:
            changepercent=0

        if stocks_dict[stock]["exthrslasttradeprice"]:
            exthrslasttradeprice = stocks_dict[stock]["exthrslasttradeprice"]
        else:
            exthrslasttradeprice=0

        if stocks_dict[stock]["exthrslasttradewithcurrency"]:
            exthrslasttradewithcurrency = stocks_dict[stock]["exthrslasttradewithcurrency"]
        else:
            exthrslasttradewithcurrency=0

        if stocks_dict[stock]["exthrschange"]:   
            exthrschange = stocks_dict[stock]["exthrschange"]
        else:
            exthrschange=0

        if stocks_dict[stock]["exthrschangepercent"]:
            exthrschangepercent = stocks_dict[stock]["exthrschangepercent"]
        else:
            exthrschangepercent=0

        if stocks_dict[stock]["perciouscloseprice"]:
            perciouscloseprice = stocks_dict[stock]["perciouscloseprice"]
        else:
            perciouscloseprice=0

        sql_search = ("""SELECT * from %s WHERE lasttradedatetime='%s'"""  %(stock, str(date_object)) )
        cur.execute(sql_search)

        if not cur.fetchone():
            sql_insert = ("""INSERT INTO %s (idg, 
                                            stocksymbol,
                                            index,
                                            lasttradeprice,
                                            lasttradewithcurrency,
                                            lasttradedatetime, 
                                            dividend,
                                            yields,
                                            lasttradesize,
                                            change,
                                            changepercent,
                                            exthrslasttradeprice,
                                            exthrslasttradewithcurrency,
                                            exthrschange,
                                            exthrschangepercent,
                                            perciouscloseprice) 
                             VALUES (%s,'%s','%s',%s,%s,TIMESTAMP '%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""%  (
                                                                                            stock,
                                                                                            idg,
                                                                                            stocksymbol,
                                                                                            index,
                                                                                            lasttradeprice,
                                                                                            lasttradewithcurrency,
                                                                                            str(date_object),
                                                                                            dividend,
                                                                                            yields,
                                                                                            lasttradesize,
                                                                                            change,
                                                                                            changepercent,
                                                                                            exthrslasttradeprice,
                                                                                            exthrslasttradewithcurrency,
                                                                                            exthrschange,
                                                                                            exthrschangepercent,
                                                                                            perciouscloseprice))
            try:
                cur.execute(sql_insert)
                conn.commit()
            except psycopg2.Error as e:
                print e
                conn.rollback()
                conn.close()
                return e
        else:
            print "..."
        conn.close()
    def build_table_insert_statement(self, stock_name, stocks_dict):
        table_columns = []

        for stocks in stocks_dict:
            for stock_item in stocks_dict[stocks]:
                if stock_item not in table_columns:
                    table_columns.append(str(stock_item).lower())

#        number_of_columns = len(table_columns)
        insert_statement = ""

        insert_statement = "CREATE TABLE %s (id serial PRIMARY KEY," % str(stock_name)

        for column in table_columns:
            column_search = str(column)

            if "time" in column:
                insert_statement += column_search + " timestamp,"
                continue

            if "stocksymbol" in column:
                insert_statement += column_search + " varchar,"
                continue

            if "index" in column:
                insert_statement += column_search + " varchar,"             
                continue

            insert_statement += column_search + " decimal,"

        insert_statement = insert_statement[:-1]
        insert_statement += ");"

        return insert_statement 

    def create_table(self, insert_statement):
        conn = self.connect_to_central()
        cur = self.give_me_a_cursor(conn)
        try:
            cur.execute(insert_statement)
            conn.commit()
            conn.close()
            return 0
        except psycopg2.Error as e:
            print e
            conn.rollback()
            conn.close()
            return e   

    def build_tables(self, stock_name, stocks_dict):
        insert_statement = self.build_table_insert_statement(stock_name, stocks_dict) 
        self.create_table(insert_statement)
        return 1

    def build_database_stock_tables(self, stocks_dict):
        for stock_name in stocks_dict:
            self.build_tables(stock_name, stocks_dict)
        return "success"

    def build_stock_object_dict(self, stocks, stocks_dict):
        #for stock_name in stocks:
        stocks_dict = self.get_stock_object(stocks,stocks_dict)
        return stocks_dict
    
    def worker_start(self, stock):  
        stocks_dict = {}
        stocks_dict = self.build_stock_object_dict(stock, stocks_dict)
        self.build_database_stock_tables(stocks_dict)
              
        while True:
            returndata = self.build_stock_object_dict(stock, stocks_dict)
            self.update_stock_obeject(stock, stocks_dict)
            print returndata
            time.sleep(5)
            
        





















