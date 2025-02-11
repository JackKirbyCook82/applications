# -*- coding: utf-8 -*-
"""
Created on Fri Jan 1 2025
@name:   ETrade Market Downloader
@author: Jack Kirby Cook

"""

import os
import sys
import time
import logging
import warnings
import pandas as pd
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
REPOSITORY = os.path.join(ROOT, "repository")
RESOURCES = os.path.join(ROOT, "resources")
if ROOT not in sys.path: sys.path.append(ROOT)
TICKERS = os.path.join(RESOURCES, "tickers.txt")
API = os.path.join(MAIN, "api.txt")

from etrade.market import ETradeProductDownloader, ETradeStockDownloader, ETradeOptionDownloader
from finance.variables import Querys, Files
from webscraping.webreaders import WebAuthorizer, WebReader
from support.synchronize import RoutineThread, RepeatingThread
from support.pipelines import Producer, Processor, Consumer
from support.queues import Dequeuer, Requeuer, Queue
from support.variables import DateRange
from support.filters import Filter
from support.files import Saver

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


authorize = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
request = "https://api.etrade.com/oauth/request_token"
access = "https://api.etrade.com/oauth/access_token"
base = "https://api.etrade.com"


class SymbolDequeuer(Dequeuer, Producer, parser=Querys.Symbol): pass
class StockDownloader(ETradeStockDownloader, Processor): pass
class StockRequeuer(Requeuer, Consumer, parser=pd.Series.to_dict): pass

class TradeDequeuer(Dequeuer, Producer, parser=Querys.Trade): pass
class ProductDownloader(ETradeProductDownloader, Processor): pass
class OptionDownloader(ETradeOptionDownloader, Processor): pass
class OptionFilter(Filter, Processor, query=Querys.Settlement): pass
class OptionSaver(Saver, Consumer, query=Querys.Settlement): pass

class MarketFile(Files.Options.Trade + Files.Options.Quote): pass
class MarketAuthorizer(WebAuthorizer, authorize=authorize, request=request, access=access, base=base): pass
class MarketReader(WebReader, delay=10): pass


def main(*args, api={}, tickers=[], expires=[], **kwargs):
    security_authorizer = MarketAuthorizer(name="MarketAuthorizer", **api)
    symbol_queue = Queue.FIFO(name="SymbolQueue", contents=tickers, capacity=None, timeout=None)
    trade_queue = Queue.FIFO(name="TradeQueue", contents=[], capacity=None, timeout=None)
    market_file = MarketFile(name="MarketFile", folder="market", repository=REPOSITORY)

    with MarketReader(name="MarketReader", authorizer=security_authorizer) as source:
        symbol_dequeuer = SymbolDequeuer(name="SymbolDequeuer", queue=symbol_queue)
        stock_downloader = StockDownloader(name="StockDownloader", source=source)
        stock_requeuer = StockRequeuer(name="StockRequeuer", queue=trade_queue)
        stock_pipeline = symbol_dequeuer + stock_downloader + stock_requeuer
        stock_thread = RoutineThread(stock_pipeline)

        trade_dequeuer = TradeDequeuer(name="TradeDequeuer", queue=trade_queue)
        product_downloader = ProductDownloader(name="ProductDownloader", source=source)
        option_downloader = OptionDownloader(name="OptionDownloader", source=source)
        option_saver = OptionSaver(name="OptionSaver", file=market_file, mode="w")
        option_pipeline = trade_dequeuer + product_downloader + option_downloader + option_saver
        option_thread = RepeatingThread(option_pipeline).setup(expires=expires)

        stock_thread.start()
        option_thread.start()
        while bool(stock_thread) or bool(symbol_queue) or bool(trade_queue):
            time.sleep(10)
        stock_thread.cease()
        option_thread.cease()
        stock_thread.join()
        option_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    with open(TICKERS, "r") as tickerfile:
        sysTickers = list(map(str.strip, tickerfile.read().split("\n")))
        sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysAPI = pd.read_csv(API, header=0, index_col="website", dtype=str).to_dict("index")["etrade"]
    main(api=sysAPI, tickers=sysTickers, expires=sysExpires)



