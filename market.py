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
from functools import reduce
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from collections import namedtuple as ntuple

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
TICKERS = os.path.join(ROOT, "applications", "tickers.txt")
API = os.path.join(ROOT, "applications", "api.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from etrade.market import ETradeProductDownloader, ETradeStockDownloader, ETradeOptionDownloader
from finance.variables import Querys
from finance.securities import OptionFile
from webscraping.webreaders import WebAuthorizer, WebReader
from support.pipelines import Producer, Processor, Consumer
from support.files import Saver, FileTypes, FileTimings
from support.queues import Dequeuer, Queue
from support.synchronize import RoutineThread
from support.variables import DateRange
from support.filtering import Filter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


authorize = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
request = "https://api.etrade.com/oauth/request_token"
access = "https://api.etrade.com/oauth/access_token"
base = "https://api.etrade.com"

class SymbolDequeuerProducer(Dequeuer, Producer): pass
class StockDownloaderProcessor(ETradeStockDownloader, Processor): pass
class ProductDownloaderProcessor(ETradeProductDownloader, Processor): pass
class OptionDownloaderProcessor(ETradeOptionDownloader, Processor): pass
class OptionFilterProcessor(Filter, Processor): pass
class OptionSaverConsumer(Saver, Consumer, query=Querys.Contract): pass

class ETradeAuthorizer(WebAuthorizer, authorize=authorize, request=request, access=access, base=base): pass
class ETradeReader(WebReader, delay=10): pass

class ETradeAPI(ntuple("API", "key code")): pass
class ETradeCriterion(ntuple("Criterion", "sizing")):
    Sizing = ntuple("Sizing", "size volume interest")

    def __new__(cls, *args, sizing, **kwargs):
        sizing = cls.Sizing(*[sizing[field] for field in cls.Sizing._fields])
        return cls(sizing)

    def options(self, table): return self.interest(table) & self.volume(table) & self.size(table)
    def interest(self, table): return table["interest"] >= self.sizing.interest
    def volume(self, table): return table["volume"] >= self.sizing.volume
    def size(self, table): return table["size"] >= self.sizing.size


def main(symbols, *args, api, criterion, parameters={}, **kwargs):
    security_authorizer = ETradeAuthorizer(name="MarketAuthorizer", apikey=api.key, apicode=api.code)
    option_file = OptionFile(name="OptionFile", filetype=FileTypes.CSV, filetiming=FileTimings.EAGER, repository=MARKET)
    symbol_queue = Queue.FIFO(name="SymbolQueue", contents=symbols, capacity=None, timeout=None)

    with ETradeReader(name="MarketReader", authorizer=security_authorizer) as reader:
        symbol_dequeue = SymbolDequeuerProducer(name="SymbolsDequeuer", queue=symbol_queue)
        stock_downloader = StockDownloaderProcessor(name="StockDownloader", feed=reader)
        product_downloader = ProductDownloaderProcessor(name="ProductDownloader", feed=reader)
        option_downloader = OptionDownloaderProcessor(name="OptionDownloader", feed=reader)
        option_filter = OptionFilterProcessor(name="OptionFilter", criterion=criterion.options)
        option_saver = OptionSaverConsumer(name="OptionSaver", file=option_file, mode="a")

        market_pipeline = symbol_dequeue + stock_downloader + product_downloader + option_downloader + option_filter + option_saver
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
        sysAPI = ETradeAPI(sysAPIKey, sysAPICode)
    with open(TICKERS, "r") as tickerfile:
        sysSymbols = [Querys.Symbol(str(string).strip().upper()) for string in tickerfile.read().split("\n")]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysSizing = dict(size=0, volume=0, interest=0)
    sysCriterion = ETradeCriterion(sizing=sysSizing)
    sysParameters = dict(expires=sysExpires)
    main(sysSymbols, api=sysAPI, criterion=sysCriterion, parameters=sysParameters)



