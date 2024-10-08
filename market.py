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
from functools import reduce
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
TICKERS = os.path.join(ROOT, "applications", "tickers.txt")
API = os.path.join(ROOT, "applications", "api.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import DateRange, Variables, Symbol
from finance.securities import OptionFile, SecurityFilter
from etrade.market import ETradeSecurityDownloader, ETradeProductDownloader
from webscraping.webreaders import WebAuthorizer, WebReader
from support.files import FileTypes, FileTimings
from support.filtering import Criterion

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

    stock_downloader = ETradeSecurityDownloader(name="StockDownloader", instrument=Variables.Instruments.STOCK, feed=None)
    product_downloader = ETradeProductDownloader(name="ProductDownloader", feed=None)
    option_downloader = ETradeSecurityDownloader(name="OptionDownloader", instrument=Variables.Instruments.OPTION, feed=None)
    option_filter = SecurityFilter(name="OptionFilter", criterion=option_criterion)
    option_file = OptionFile(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)

    with ETradeReader(name="SecurityReader", authorizer=security_authorizer) as reader:

        pipeline = [stock_downloader, product_downloader, option_downloader, option_filter]
        producer = pipeline[0](source=arguments["symbols"], **parameters)
        pipeline = reduce(lambda source, function: function(source=source, **parameters), pipeline[1:], producer)
        for dataframe in iter(pipeline):
            for contract, dataset in dataframe.groupby(["ticker", "expire"]):
                contract = Variables.Querys.CONTRACT(*contract)
                option_file.write(dataset, query=contract, mode="a")


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    with open(API, "r") as apifile:
        sysApiKey, sysApiCode = [str(string).strip() for string in str(apifile.read()).split("\n")]
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")][0:10]
        sysSymbols = [Symbol(ticker) for ticker in sysTickers]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysArguments = dict(apikey=sysApiKey, apicode=sysApiCode, symbols=sysSymbols, size=0, volume=0, interest=0)
    sysParameters = dict(expires=sysExpires)
    main(arguments=sysArguments, parameters=sysParameters)



