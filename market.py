# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Trading Platform Downloader
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
if ROOT not in sys.path:
    sys.path.append(ROOT)

from etrade.market import ETradeProductDownloader, ETradeSecurityDownloader
from finance.variables import Variables, Querys, DateRange
from finance.securities import OptionFile
from webscraping.webreaders import WebAuthorizer, WebReader
from support.files import Saver, FileTypes, FileTimings
from support.filtering import Filter, Criterion
from support.synchronize import RoutineThread
from support.queues import Dequeue, QueueTypes, Queue
from support.mixins import AttributeNode

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


authorize = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
request = "https://api.etrade.com/oauth/request_token"
access = "https://api.etrade.com/oauth/access_token"
base = "https://api.etrade.com"


class ETradeAuthorizer(WebAuthorizer, authorize=authorize, request=request, access=access, base=base): pass
class ETradeReader(WebReader, delay=10): pass

class ETradeMarket(object):
    def __init__(self, symbols, options, *args, downloaders, filters, **kwargs):
        self.downloaders = AttributeNode(downloaders)
        self.filters = AttributeNode(filters)
        self.options = options
        self.symbols = symbols

    def __call__(self, *args, **kwargs):
        for symbol in self.symbols:
            source = (symbol,)
            stocks = self.downloaders.stock(source, *args, **kwargs)
            source = (source, stocks)
            products = self.downloaders.product(source, *args, **kwargs)
            for product in products:
                source = (product,)
                options = self.downloaders.option(source, *args, **kwargs)
                source = (product, options)
                options = self.filters.option(source, *args, **kwargs)
                if options is None: continue
                source = (product, options)
                self.savers.option(source, *args, **kwargs)


def main(*args, arguments, parameters, **kwargs):
    option_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    security_authorizer = ETradeAuthorizer(name="MarketAuthorizer", apikey=arguments["apikey"], apicode=arguments["apicode"])
    option_file = OptionFile(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    symbol_queue = FIFOQueue(name="SymbolQueue", contents=arguments["symbols"], capacity=None, timeout=None)

    with ETradeReader(name="MarketReader", authorizer=security_authorizer) as reader:
        symbol_source = Dequeue(name="SymbolSource", queue=symbol_queue, query=Querys.Symbol)
        stock_downloader = ETradeSecurityDownloader(name="StockDownloader", feed=reader, instrument=Variables.Instruments.STOCK)
        product_downloader = ETradeProductDownloader(name="ProductDownloader", feed=reader)
        option_downloader = ETradeSecurityDownloader(name="OptionDownloader", feed=reader, instrument=Variables.Instruments.OPTION)
        option_filter = Filter(name="OptionFilter", criterion=option_criterion)
        option_saver = Saver(name="OptionSaver", file=option_file, mode="a")

        downloaders = dict(stock=stock_downloader, product=product_downloader, option=option_downloader)
        filters = dict(option=option_filter)

        market_routine = ETradeMarket(symbol_source, option_saver, downloaders=downloaders, filters=filters)
        market_thread = RoutineThread(market_routine).setup(**parameters)
        market_thread.start()
        market_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    with open(API, "r") as apifile:
        sysApiKey, sysApiCode = [str(string).strip() for string in str(apifile.read()).split("\n")]
    with open(TICKERS, "r") as tickerfile:
        sysSymbols = [{"ticker": str(string).strip().upper()} for string in tickerfile.read().split("\n")]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysArguments = dict(apikey=sysApiKey, apicode=sysApiCode, symbols=sysSymbols, size=0, volume=0, interest=0)
    sysParameters = dict(expires=sysExpires)
    main(arguments=sysArguments, parameters=sysParameters)



