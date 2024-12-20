# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 2024
@name:   Yahoo Technical Downloader
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

from yahoo.technicals import YahooHistoryDownloader
from finance.variables import Querys
from finance.technicals import BarsFile
from webscraping.webdrivers import WebDriver, WebBrowser
from support.pipelines import Producer, Processor, Consumer
from support.synchronize import RoutineThread
from support.queues import Dequeuer, Queue
from support.variables import DateRange
from support.files import Saver

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class SymbolDequeuerProducer(Dequeuer, Producer): pass
class HistoryDownloaderProcessor(YahooHistoryDownloader, Processor): pass
class HistorySaverConsumer(Saver, Consumer, query=Querys.Symbol): pass

class YahooDriver(WebDriver, browser=WebBrowser.Chrome, executable=CHROME, delay=10):
    pass


def main(*args, arguments={}, parameters={}, **kwargs):
    symbol_queue = Queue.FIFO(name="SymbolQueue", contents=arguments["symbols"], capacity=None, timeout=None)
    bars_file = BarsFile(name="BarsFile", repository=HISTORY)

    with YahooDriver(name="TechnicalReader") as source:
        symbol_dequeue = SymbolDequeuerProducer(name="SymbolDequeue", queue=symbol_queue)
        bars_downloader = HistoryDownloaderProcessor(name="BarDownloader", source=source)
        bars_saver = HistorySaverConsumer(name="BarSaver", file=bars_file, mode="a")

        technical_pipeline = symbol_dequeue + bars_downloader + bars_saver
        technical_thread = RoutineThread(technical_pipeline, name="TechnicalThread").setup(**parameters)
        technical_thread.start()
        technical_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    logging.getLogger("seleniumwire").setLevel(logging.ERROR)
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")]
        sysSymbols = [Querys.Symbol(ticker) for ticker in sysTickers]
    sysDates = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() - Timedelta(weeks=104)).date()])
    sysArguments = dict(symbols=sysSymbols)
    sysParameters = dict(dates=sysDates, period=252)
    main(sysSymbols, parameters=sysParameters)



