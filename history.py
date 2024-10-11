# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 2024
@name:   Yahoo Trading Platform Downloader
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
HISTORY = os.path.join(ROOT, "repository", "history")
TICKERS = os.path.join(ROOT, "applications", "tickers.txt")
CHROME = os.path.join(ROOT, "resources", "chromedriver.exe")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import DateRange, Symbol
from finance.technicals import BarsFile
from yahoo.history import YahooTechnicalDownloader
from webscraping.webdrivers import WebDriver, WebBrowser
from support.files import FileTypes, FileTimings

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class YahooDriver(WebDriver, browser=WebBrowser.CHROME, executable=CHROME, delay=10):
    pass


def main(*args, arguments, parameters, **kwargs):
    with YahooDriver(name="HistoryReader") as reader:
        bars_downloader = YahooTechnicalDownloader(name="BarsDownloader", feed=reader)
        bars_file = BarsFile(name="BarsFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
        history_producer = bars_downloader(source=arguments["symbols"], **parameters)
        for bars in iter(history_producer):
            for symbol, dataframe in bars.groupby(Symbol.variables):
                symbol = Symbol(*symbol)
                bars_file.write(dataframe, query=symbol, mode="w")


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    logging.getLogger("seleniumwire").setLevel(logging.ERROR)
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")]
        sysSymbols = [Symbol(ticker) for ticker in sysTickers]
    sysDates = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() - Timedelta(weeks=104)).date()])
    sysArguments = dict(symbols=sysSymbols)
    sysParameters = dict(dates=sysDates, period=252)
    main(arguments=sysArguments, parameters=sysParameters)



