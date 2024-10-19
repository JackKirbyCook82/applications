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


def main(*args, arguments, parameters, **kwargs):
    option_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    security_authorizer = ETradeAuthorizer(name="SecurityAuthorizer", apikey=arguments["apikey"], apicode=arguments["apicode"])
    option_file = OptionFile(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    with ETradeReader(name="MarketReader", authorizer=security_authorizer) as reader:
        product_downloader = ETradeProductDownloader(name="ProductDownloader", feed=reader, technical=Variables.Technicals.BARS)
        stock_downloader = ETradeSecurityDownloader(name="StockDownloader", feed=reader, instrument=Variables.Instruments.STOCK)
        option_downloader = ETradeSecurityDownloader(name="OptionDownloader", feed=reader, instrument=Variables.Instruments.OPTION)
        option_filter = Filter(name="OptionFilter", criterion=option_criterion)
        option_saver = Saver(name="OptionSaver", file=option_file, mode="w", query=Querys.Contract)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    with open(API, "r") as apifile:
        sysApiKey, sysApiCode = [str(string).strip() for string in str(apifile.read()).split("\n")]
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")]
        sysSymbols = [Querys.Symbol(ticker) for ticker in sysTickers]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysArguments = dict(apikey=sysApiKey, apicode=sysApiCode, symbols=sysSymbols, size=0, volume=0, interest=0)
    sysParameters = dict(expires=sysExpires)
    main(arguments=sysArguments, parameters=sysParameters)



