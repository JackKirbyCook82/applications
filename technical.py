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
TECHNICAL = os.path.join(ROOT, "repository", "technical")
TICKERS = os.path.join(ROOT, "applications", "tickers.txt")
CHROME = os.path.join(ROOT, "resources", "chromedriver.exe")
if ROOT not in sys.path: sys.path.append(ROOT)

from yahoo.technicals import YahooHistoryDownloader
from finance.variables import Querys
from finance.technicals import StatisticCalculator, StochasticCalculator, HistoryFile, StatisticFile, StochasticFile
from webscraping.webdrivers import WebDriver, WebBrowser
from support.pipelines import Producer, Processor, Consumer
from support.files import Loader, Saver, Directory
from support.synchronize import RoutineThread
from support.queues import Dequeuer, Queue
from support.variables import DateRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class SymbolDequeuerProducer(Dequeuer, Producer): pass
class HistoryDownloaderProcessor(YahooHistoryDownloader, Processor): pass
class HistorySaverConsumer(Saver, Consumer, query=Querys.Symbol): pass
class HistoryDirectoryProducer(Directory, Producer, query=Querys.Symbol): pass
class HistoryLoaderProcessor(Loader, Processor, query=Querys.Symbol): pass
class StatisticCalculatorProcessor(StatisticCalculator, Processor): pass
class StochasticCalculatorProcessor(StochasticCalculator, Processor): pass
class StatisticSaverConsumer(Saver, Consumer, query=Querys.Symbol): pass
class StochasticSaverConsumer(Saver, Consumer, query=Querys.Symbol): pass

class YahooDriver(WebDriver, browser=WebBrowser.Chrome, executable=CHROME, delay=10):
    pass


def main(*args, arguments={}, parameters={}, **kwargs):
    symbol_queue = Queue.FIFO(name="SymbolQueue", contents=arguments["symbols"], capacity=None, timeout=None)
    stochastic_file = StochasticFile(name="StochasticFile", repository=TECHNICAL)
    statistic_file = StatisticFile(name="StatisticFile", repository=TECHNICAL)
    history_file = HistoryFile(name="BarsFile", repository=TECHNICAL)

    with YahooDriver(name="HistoryReader") as source:
        symbol_dequeue = SymbolDequeuerProducer(name="SymbolDequeue", queue=symbol_queue)
        history_downloader = HistoryDownloaderProcessor(name="HistoryDownloader", source=source)
        history_saver = HistorySaverConsumer(name="HistorySaver", file=history_file, mode="a")

        history_pipeline = symbol_dequeue + history_downloader + history_saver
        history_thread = RoutineThread(history_pipeline, name="HistoryThread").setup(**parameters)
        history_thread.start()
        history_thread.join()

    history_directory = HistoryDirectoryProducer(name="HistoryDirectory", file=history_file, mode="r")
    history_loader = HistoryLoaderProcessor(name="HistoryLoader", file=history_file, mode="r")
    statistic_calculator = StatisticCalculatorProcessor(name="StatisticCalculator")
    stochastic_calculator = StochasticCalculatorProcessor(name="StochasticCalculator")
    statistic_saver = StatisticSaverConsumer(name="StatisticSaver", file=statistic_file, mode="w")
    stochastic_saver = StochasticSaverConsumer(name="StochasticSaver", file=stochastic_file, mode="w")

    statistic_pipeline = history_directory + history_loader + statistic_calculator + statistic_saver
    statistic_thread = RoutineThread(statistic_pipeline, name="StatisticThread").setup(**parameters)
    stochastic_pipeline = history_directory + history_loader + stochastic_calculator + stochastic_saver
    stochastic_thread = RoutineThread(stochastic_pipeline, name="StochasticThread").setup(**parameters)

    statistic_thread.start()
    stochastic_thread.start()
    statistic_thread.join()
    stochastic_thread.join()


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
    sysDates = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() - Timedelta(weeks=52*3)).date()])
    sysArguments = dict(symbols=sysSymbols)
    sysParameters = dict(dates=sysDates, period=252)
    main(arguments=sysArguments, parameters=sysParameters)



