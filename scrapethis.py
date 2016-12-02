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
import multiprocessing
from trader import StockScraper

def get_stocks():
    stocks = ["AAPL", "GOOG", "PTN"]
    return stocks

    #turn the focet on
if __name__ == '__main__':
    stck = StockScraper()
    stocks = get_stocks()

    for stock in stocks:
                #for item in stocks_dict[stock]:
                #    print stock, item, stocks_dict[stock][item]

        d = multiprocessing.Process(name=stock, target=stck.worker_start, args=(str(stock),))
        d.daemon = True
        d.start()
    while True:
        print "Getting Data"
        time.sleep(10)
    


