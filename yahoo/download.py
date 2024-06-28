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
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
HISTORY = os.path.join(ROOT, "repository", "history")
TICKERS = os.path.join(ROOT, "applications", "tickers.txt")
CHROME = os.path.join(ROOT, "resources", "chromedriver.exe")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from yahoo.history import YahooHistoryDownloader
from finance.variables import Variables, DateRange
from finance.technicals import TechnicalFiles
from webscraping.webdrivers import WebDriver, WebBrowser
from support.files import Saver, FileTypes, FileTimings
from support.queues import Dequeuer, Queues
from support.synchronize import SideThread

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class YahooDriver(WebDriver, browser=WebBrowser.CHROME, executable=CHROME, delay=10): pass
class SymbolDequeuer(Dequeuer, query=Variables.Querys.SYMBOL): pass
class SymbolSaver(Saver, query=Variables.Querys.SYMBOL): pass


def history(*args, reader, source, saving, dates=[], parameters={}, **kwargs):
    history_dequeuer = SymbolDequeuer(name="HistoryDequeuer", source=source)
    history_downloader = YahooHistoryDownloader(name="HistoryDownloader", feed=reader)
    history_saver = SymbolSaver(name="HistorySaver", destination=saving)
    history_pipeline = history_dequeuer + history_downloader + history_saver
    history_thread = SideThread(history_pipeline, name="HistoryThread")
    history_thread.setup(dates=dates, **parameters)
    return history_thread


def main(*args, symbols=[], **kwargs):
    bars_queue = Queues.FIFO(name="BarsQueue", contents=symbols, capacity=None)
    bars_file = TechnicalFiles.Bars(name="BarsFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    with YahooDriver(name="HistoryReader") as history_reader:
        history_parameters = dict(reader=history_reader, source=bars_queue, saving={bars_file: "w"})
        history_thread = history(*args, **history_parameters, **kwargs)
        history_thread.start()
        history_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    logging.getLogger("seleniumwire").setLevel(logging.ERROR)
    warnings.filterwarnings("ignore")
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")][0:2]
        sysSymbols = [Variables.Querys.SYMBOL(ticker) for ticker in sysTickers]
    sysDates = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() - Timedelta(weeks=60)).date()])
    sysParameters = dict()
    main(symbols=sysSymbols, dates=sysDates, parameters=sysParameters)



