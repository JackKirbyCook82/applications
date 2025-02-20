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
DRIVER = os.path.join(RESOURCES, "chromedriver.exe")
TICKERS = os.path.join(RESOURCES, "tickers.txt")

from yahoo.history import YahooBarsDownloader
from finance.technicals import TechnicalCalculator
from finance.variables import Querys, Variables, Files
from webscraping.webdrivers import WebDriver
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


def history(*args, source, file, symbols, **kwargs):
    history_dequeuer = SymbolDequeuer(name="HistoryDequeuer", queue=symbols)
    history_downloader = BarsDownloader(name="HistoryDownloader", source=source)
    history_calculator = TechnicalCalculator(name="HistoryCalculator", technicals=[Variables.Analysis.Technical.STATISTIC, Variables.Analysis.Technical.STOCHASTIC])
    history_saver = TechnicalSaver(name="HistorySaver", file=file, mode="w")
    history_pipeline = history_dequeuer + history_downloader + history_calculator + history_saver
    return history_pipeline


def main(*args, tickers=[], dates=[], period, **kwargs):
    history_file = (Files.Stocks.Bars + Files.Stocks.Statistic + Files.Stocks.Stochastic)(name="HistoryFile", folder="history", repository=REPOSITORY)
    symbol_queue = Queue.FIFO(name="SymbolQueue", contents=tickers, capacity=None, timeout=None)

    with WebDriver(executable=DRIVER, delay=5) as source:
        history_parameters = dict(source=source, file=history_file, symbols=symbol_queue)
        history_pipeline = history(*args, **history_parameters, **kwargs)
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



