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

from yahoo.history import YahooHistoryDownloader
from finance.variables import DateRange, Variables, Symbol
from finance.technicals import TechnicalCalculator, TechnicalFiles
from webscraping.webdrivers import WebDriver, WebBrowser
from support.files import Loader, Saver, FileTypes, FileTimings
from support.queues import Dequeuer, Queues
from support.synchronize import RoutineThread

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class YahooDriver(WebDriver, browser=WebBrowser.CHROME, executable=CHROME, delay=10): pass
class SymbolLoader(Loader, variable=Variables.Querys.SYMBOL, function=Symbol.fromstr): pass
class SymbolSaver(Saver, variable=Variables.Querys.SYMBOL): pass
class SymbolDequeuer(Dequeuer, variable=Variables.Querys.SYMBOL, function=Symbol.fromstr): pass


def history(*args, reader, source, saving, parameters={}, **kwargs):
    history_dequeuer = SymbolDequeuer(name="HistoryDequeuer", queue=source)
    history_downloader = YahooHistoryDownloader(name="HistoryDownloader", feed=reader)
    history_saver = SymbolSaver(name="HistorySaver", files=saving)
    history_pipeline = history_dequeuer + history_downloader + history_saver
    history_thread = RoutineThread(history_pipeline, name="HistoryThread")
    history_thread.setup(**parameters)
    return history_thread


def technicals(*args, directory, loading, saving, parameters={}, functions={}, **kwargs):
    technical_loader = SymbolLoader(name="TechnicalLoader", files=loading, directory=directory, **functions)
    technical_calculator = TechnicalCalculator(name="TechnicalCalculator")
    technical_saver = SymbolSaver(name="TechnicalSaver", files=saving)
    technical_pipeline = technical_loader + technical_calculator + technical_saver
    technical_thread = RoutineThread(technical_pipeline, name="TechnicalThread")
    technical_thread.setup(**parameters)
    return technical_thread


def main(*args, arguments, parameters, **kwargs):
    bars_queue = Queues.FIFO(name="BarsQueue", contents=arguments["symbols"], capacity=None)
    bars_file = TechnicalFiles.Bars(name="BarsFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    statistic_file = TechnicalFiles.Statistic(name="StatisticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    stochastic_file = TechnicalFiles.Stochastic(name="StochasticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)

    with YahooDriver(name="HistoryReader") as history_reader:
        history_parameters = dict(reader=history_reader, source=bars_queue, saving={bars_file: "w"}, parameters=parameters)
        history_thread = history(*args, **history_parameters, **kwargs)
        history_thread.start()
        history_thread.join()

    technical_parameters = dict(directory=bars_file, loading={bars_file: "r"}, saving={statistic_file: "w", stochastic_file: "w"}, parameters=parameters)
    technical_thread = technicals(*args, **technical_parameters, **kwargs)
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
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")][0:10]
        sysSymbols = [Symbol(ticker) for ticker in sysTickers]
    sysDates = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() - Timedelta(weeks=104)).date()])
    sysArguments = dict(symbols=sysSymbols)
    sysParameters = dict(dates=sysDates, period=252)
    main(arguments=sysArguments, parameters=sysParameters)



