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
import multiprocessing
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas.io.sql as psql
import pandas.util.testing as tm; tm.N = 3
import pandas as pd
import psycopg2
from trader import StockScraper
import time
import requests

def get_stocks(chunk_size):
    chunk_count = 0
    chunk_list = []
    stocks = []
    nasdaq_urls = ["http://www.advfn.com/nasdaq/nasdaq.asp",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=B",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=C",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=D",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=E",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=F",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=G",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=H",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=I",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=J",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=K",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=L",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=M",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=N",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=O",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=P",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=Q",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=R",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=S",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=T",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=U",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=V",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=W",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=X",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=Y",
                  "http://www.advfn.com/nasdaq/nasdaq.asp?companies=Z"]
    
    for scrape_url in nasdaq_urls:
        r = requests.get(scrape_url)
        value = r.text.split("</td>")
        for values in value:
            try:
                if "</a>" in values.split('">')[1]:
                    if chunk_count == chunk_size:
                        stocks.append(chunk_list)
                        chunk_count = 0
                        chunk_list = []
                    else:
                        chunk_list.append(str(values.split('">')[1].replace("</a>", "")))
                        chunk_count += 1
            except:
                continue
    return stocks

class ChartData(object):
    
    def __init__(self, stock, hours):
        self.stock = stock
        self.seconds = hours * 60 * 60
        self.limit = float(self.seconds) / 12
        self.limit = int(self.limit)
        pass

    def get_data(self):
        conn = psycopg2.connect("dbname='atlas' user='postgres' host='localhost' password=''")
        cur = conn.cursor()     
        
        stock_index = self.stock.lower()     
        if stock_index == "true":
            return
        if stock_index == "cast":
            return
        if stock_index == "else":
            return      
        if stock_index == "to":
            return  
        get_data = """WITH t AS (SELECT lasttradedatetime,lasttradeprice FROM """ + stock_index + """ ORDER BY lasttradedatetime DESC LIMIT """ + str(self.limit) + """)
        SELECT * FROM t where lasttradeprice !=0.0 ORDER BY lasttradedatetime ASC;"""        
        dataframe = psql.read_sql_query(get_data, conn,'lasttradedatetime', parse_dates=True)
        
        mean = dataframe.mean().sum()
        percent_change = dataframe.pct_change(1).sum() 
        min = str(dataframe.min()).split()[1]
        max =  str(dataframe.max()).split()[1]
        minutes = self.limit * 12 / 60
        hours = self.limit * 12 / 60 /60
        no_errors = True
        
        
        print_statement = stock_index + " \n"
        
        if str(mean).split()[0] != "nan":        
            print_statement += str(mean).split()[0] + " \n"
        else:
            no_errors = False
            
        if str(percent_change).split()[1] != "0.0" and str(percent_change).split()[1] != "NaN":
            print_statement += str(percent_change).split()[1] + " \n"
        else:
            no_errors = False
            
        if min != "NaN":
            print_statement += min + " \n"
        else:
            no_errors = False
        
        if max != "NaN":
            print_statement += max + " \n"
        else:
            no_errors = False
        
        if no_errors == True:
            print_statement += str(self.limit) + " \n"
            print_statement +=  str(minutes) + " \n"
            print_statement +=  str(hours) + " \n"
            print_statement +=  " \n"
        
            #print print_statement
        return self



if __name__ == '__main__':
    
    stck = StockScraper()
    stocks = get_stocks(20)
    
    running_process_list = []
    failed_processes = []
    print "Start Batch Processing"
    while True:
        for chunked_list in stocks:
            jobs = []
            for stock in chunked_list:
                if stock not in running_process_list:
                    d = multiprocessing.Process(name=stock, target=stck.worker_start, args=(str(stock),))
                    d.daemon = True
                    d.start()                
                    running_process_list.append(stock)       
                    jobs.append(d)
    
            while jobs:  
                
                for index, j in enumerate(jobs):
                    j.join()
                    
                    if j.exitcode == 0:
                        jobs.pop(index)
                        #print '%s.exitcode = %s' % (j.name, j.exitcode)
                    else:
                        if j.exitcode == 1:
                            jobs.pop(index)
                        #print '%s.exitcode = %s' % (j.name, j.exitcode)
                
                if not jobs:
                    break
                
        #for stock in stocks:
        #    for chuck_item in stock:
        #        stock_charter = ChartData(chuck_item.lower(), 5)
        #        stock_charter.get_data()
                                
        
        print "Jobs are done"
        time.sleep(1)

