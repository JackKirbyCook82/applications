# -*- coding: utf-8 -*-
"""
Created on Fri Jan 1 2025
@name:   ETrade Market Downloader
@author: Jack Kirby Cook

"""

import os
import sys
import json
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
API = os.path.join(RESOURCES, "api.txt")

from etrade.market import ETradeProductDownloader, ETradeStockDownloader, ETradeOptionDownloader
from finance.variables import Querys, Files
from webscraping.webreaders import WebAuthorizer, WebAuthorizerAPI, WebReader
from support.pipelines import Producer, Processor, Consumer
from support.synchronize import RoutineThread
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


class StockDownloader(ETradeStockDownloader, Producer): pass
class ProductDownloader(ETradeProductDownloader, Producer): pass
class OptionDownloader(ETradeOptionDownloader, Processor): pass
class OptionFilter(Filter, Processor, query=Querys.Settlement): pass
class OptionSaver(Saver, Consumer, query=Querys.Settlement): pass


def market(*args, source, destination, **kwargs):
    product_downloader = ProductDownloader(name="ProductDownloader", source=source)
    option_downloader = OptionDownloader(name="OptionDownloader", source=source)
    option_saver = OptionSaver(name="OptionSaver", file=destination, mode="w")
    market_pipeline = product_downloader + option_downloader + option_saver
    return market_pipeline


def main(*args, api, symbols=[], expires=[], **kwargs):
    file = (Files.Options.Trade + Files.Options.Quote)(name="MarketFile", folder="market", repository=REPOSITORY)
    authorizer = WebAuthorizer(api=api, authorize=authorize, request=request, access=access, base=base)
    with WebReader(authorizer=authorizer, delay=10) as source:
        stocks = StockDownloader(name="StockDownloader", source=source)
        pipeline = market(*args, source=source, destination=file, **kwargs)
        trades = [Querys.Trade(series.to_dict()) for series in stocks(symbols)]
        thread = RoutineThread(pipeline, name="MarketThread").setup(trades, expires=expires)
        thread.start()
        thread.cease()
        thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    with open(TICKERS, "r") as tickerfile:
        sysTickers = list(map(str.strip, tickerfile.read().split("\n")))
        sysSymbols = list(map(Querys.Symbol, sysTickers))
        sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    with open(API, "r") as apifile:
        sysAPI = WebAuthorizerAPI(*json.loads(apifile.read())["etrade"])
    main(api=sysAPI, symbols=sysSymbols, expires=sysExpires)



