# -*- coding: utf-8 -*-
"""
Created on Fri Jan 1 2025
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
REPOSITORY = os.path.join(ROOT, "repository")
RESOURCES = os.path.join(ROOT, "resources")
if ROOT not in sys.path: sys.path.append(ROOT)
CHROME = os.path.join(RESOURCES, "chromedriver.exe")
TICKERS = os.path.join(RESOURCES, "tickers.txt")

from yahoo.history import YahooBarsDownloader
from finance.technicals import TechnicalCalculator
from finance.variables import Querys, Variables, Files
from webscraping.webdrivers import WebDriver, WebBrowser
from support.pipelines import Producer, Processor, Consumer
from support.synchronize import RoutineThread
from support.queues import Dequeuer, Queue
from support.variables import DateRange
from support.files import Saver

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


class SymbolDequeuer(Dequeuer, Producer, parser=Querys.Symbol): pass
class BarsDownloader(YahooBarsDownloader, Processor): pass
class TechnicalCalculator(TechnicalCalculator, Processor): pass
class TechnicalSaver(Saver, Consumer, query=Querys.Symbol): pass

class HistoryFile(Files.Stocks.Bars + Files.Stocks.Statistic + Files.Stocks.Stochastic): pass
class HistoryDriver(WebDriver, browser=WebBrowser.Chrome, executable=CHROME, delay=5): pass


def main(*args, tickers=[], dates=[], period, **kwargs):
    history_queue = Queue.FIFO(name="HistoryQueue", contents=tickers, capacity=None, timeout=None)
    history_file = HistoryFile(name="HistoryFile", folder="history", repository=REPOSITORY)
    history_technicals = [Variables.Analysis.Technical.STATISTIC, Variables.Analysis.Technical.STOCHASTIC]

    with HistoryDriver(name="HistoryDriver") as history_source:
        history_dequeue = SymbolDequeuer(name="HistoryDequeue", queue=history_queue)
        history_downloader = BarsDownloader(name="HistoryDownloader", source=history_source)
        history_calculator = TechnicalCalculator(name="HistoryCalculator", technicals=history_technicals)
        history_saver = TechnicalSaver(name="HistorySaver", file=history_file, mode="w")

        history_pipeline = history_dequeue + history_downloader + history_calculator + history_saver
        history_thread = RoutineThread(history_pipeline, name="HistoryThread").setup(dates=dates, period=period)
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
        sysTickers = list(map(str.strip, tickerfile.read().split("\n")))
        sysDates = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() - Timedelta(weeks=52*3)).date()])
    main(tickers=sysTickers, dates=sysDates, period=252)



