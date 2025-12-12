# -*- coding: utf-8 -*-
"""
Created on Thurs Dec 11 2025
@name:   Technical Analysis
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import pandas as pd
from enum import Enum
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from collections import namedtuple as ntuple

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
RESOURCES = os.path.join(ROOT, "resources")
if ROOT not in sys.path: sys.path.append(ROOT)
WEBAPI = os.path.join(RESOURCES, "webapi.txt")

from alpaca.history import AlpacaBarsDownloader
from finance.technicals import TechnicalCalculator, TechnicalEquation
from finance.backtesting import BackTestingCalculator
from finance.concepts import Querys
from webscraping.webreaders import WebReader
from support.pipelines import Producer, Processor, Consumer
from support.synchronize import RoutineThread
from support.concepts import DateRange
from support.mixins import Delayer

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


Website = Enum("WebSite", "ALPACA ETRADE")
class BarDownloader(AlpacaBarsDownloader, Producer): pass
class TechnicalCalculator(TechnicalCalculator, Processor): pass
class BackTestingCalculator(BackTestingCalculator, Consumer): pass


def main(*args, symbol, webapi, delayer, parameters={}, **kwargs):
    with WebReader(delayer=delayer) as alpaca_source:
        macd_equation = TechnicalEquation.MACD()
        rsi_equation = TechnicalEquation.RSI()
        bb_equation = TechnicalEquation.BB()
        mfi_equation = TechnicalEquation.MFI()
        technical_equations = [macd_equation, rsi_equation, bb_equation, mfi_equation]
        bar_downloader = BarDownloader(name="BarDownloader", source=alpaca_source, webapi=webapi[Website.ALPACA])
        technical_calculator = TechnicalCalculator(name="TechnicalCalculator", equations=[technical_equations])
        backtesting_calculator = BackTestingCalculator(name="BackTestingCalculator")
        backtesting_pipeline = bar_downloader + technical_calculator + backtesting_calculator
        backtesting_thread = RoutineThread(backtesting_pipeline, name="BackTestingThread").setup(symbol, **parameters)
        backtesting_thread.start()
        backtesting_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    function = lambda contents: ntuple("WebApi", list(contents.keys()))(*contents.values())
    sysWebApi = pd.read_csv(WEBAPI, sep=" ", header=0, index_col=0, converters={0: lambda website: Website[str(website).upper()]})
    sysWebApi = function(sysWebApi.to_dict("index")[Website.ALPACA])
    sysHistory = DateRange([(Datetime.today() - Timedelta(weeks=52*5)).date(), (Datetime.today() - Timedelta(days=1)).date()])
    sysParameters = dict(current=Datetime.now().date(), history=sysHistory)
    main(symbol=Querys.Symbol("SPY"), webapi=sysWebApi, delayer=Delayer(3), parameters=sysParameters)

