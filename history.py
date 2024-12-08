# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 2024
@name:   Yahoo History Downloader
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

from yahoo.history import YahooTechnicalDownloader
from finance.variables import Variables, Querys
from finance.technicals import BarsFile
from webscraping.webdrivers import WebDriver, WebBrowser
from support.pipelines import Producer, Processor, Consumer
from support.queues import Dequeuer, Queue
from support.files import Saver, FileTypes, FileTimings
from support.synchronize import RoutineThread
from support.variables import DateRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class SymbolDequeuerProducer(Dequeuer, Producer): pass
class HistoryDownloaderProcessor(YahooTechnicalDownloader, Processor): pass
class HistorySaverConsumer(Saver, Consumer, query=Querys.Symbol): pass

class YahooDriver(WebDriver, browser=WebBrowser.CHROME, executable=CHROME, delay=10):
    pass


def main(symbols, *args, parameters={}, **kwargs):
    bars_file = BarsFile(name="BarsFile", filetype=FileTypes.CSV, filetiming=FileTimings.EAGER, repository=HISTORY)
    symbol_queue = Queue.FIFO(name="SymbolQueue", contents=symbols, capacity=None, timeout=None)

    with YahooDriver(name="HistoryReader") as reader:
        symbol_dequeue = SymbolDequeuerProducer(name="SymbolDequeue", queue=symbol_queue)
        history_downloader = HistoryDownloaderProcessor(name="HistoryDownloader", feed=reader, instrument=Variables.Technicals.BARS)
        history_saver = HistorySaverConsumer(name="HistorySaver", file=bars_file, mode="a")

        history_pipeline = symbol_dequeue + history_downloader + history_saver
        history_thread = RoutineThread(history_pipeline, name="HistoryThread").setup(**parameters)
        history_thread.start()
        history_thread.join()


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
    sysParameters = dict(dates=sysDates, period=252)
    main(sysSymbols, parameters=sysParameters)



