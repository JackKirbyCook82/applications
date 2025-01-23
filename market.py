# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Market Downloader
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import pandas as pd
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
TICKERS = os.path.join(ROOT, "applications", "tickers.txt")
API = os.path.join(ROOT, "applications", "api.txt")
if ROOT not in sys.path: sys.path.append(ROOT)

from etrade.market import ETradeProductDownloader, ETradeStockDownloader, ETradeOptionDownloader
from finance.securities import OptionFile
from finance.variables import Querys
from webscraping.webreaders import WebAuthorizer, WebReader
from support.pipelines import Producer, Processor, Consumer
from support.synchronize import RoutineThread
from support.queues import Dequeuer, Queue
from support.variables import DateRange
from support.files import Saver

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


authorize = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
request = "https://api.etrade.com/oauth/request_token"
access = "https://api.etrade.com/oauth/access_token"
base = "https://api.etrade.com"

class SymbolDequeuerProducer(Dequeuer, Producer, query=Querys.Symbol): pass
class StockDownloaderProcessor(ETradeStockDownloader, Processor, query=Querys.Symbol): pass
class ProductDownloaderProcessor(ETradeProductDownloader, Processor, query=Querys.Product): pass
class OptionDownloaderProcessor(ETradeOptionDownloader, Processor, query=Querys.Settlement): pass
class OptionSaverConsumer(Saver, Consumer, query=Querys.Settlement): pass

class ETradeAuthorizer(WebAuthorizer, authorize=authorize, request=request, access=access, base=base): pass
class ETradeReader(WebReader, delay=10): pass


def main(*args, arguments, parameters, **kwargs):
    security_authorizer = ETradeAuthorizer(name="MarketAuthorizer", apikey=arguments["api"]["key"], apicode=arguments["api"]["code"])
    symbol_queue = Queue.FIFO(name="SymbolQueue", contents=arguments["symbols"], capacity=None, timeout=None)
    option_file = OptionFile(name="OptionFile", repository=MARKET)

    with ETradeReader(name="MarketReader", authorizer=security_authorizer) as source:
        symbol_dequeue = SymbolDequeuerProducer(name="SymbolsDequeuer", queue=symbol_queue)
        stock_downloader = StockDownloaderProcessor(name="StockDownloader", source=source)
        product_downloader = ProductDownloaderProcessor(name="ProductDownloader", source=source)
        option_downloader = OptionDownloaderProcessor(name="OptionDownloader", source=source)
        option_saver = OptionSaverConsumer(name="OptionSaver", file=option_file, mode="a")

        market_pipeline = symbol_dequeue + stock_downloader + product_downloader + option_downloader + option_saver
        market_thread = RoutineThread(market_pipeline, name="MarketThread").setup(**parameters)
        market_thread.start()
        market_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)

    with open(API, "r") as apifile:
        sysAPIKey, sysAPICode = [str(string).strip() for string in str(apifile.read()).split("\n")]
        sysAPI = {"key": sysAPIKey, "code": sysAPICode}
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")]
        sysSymbols = [Querys.Symbol(ticker) for ticker in sysTickers]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysArguments = dict(symbols=sysSymbols, api=sysAPI)
    sysParameters = dict(expires=sysExpires)
    main(arguments=sysArguments, parameters=sysParameters)



