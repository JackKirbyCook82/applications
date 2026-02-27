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
from types import SimpleNamespace
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from collections import namedtuple as ntuple

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
RESOURCES = os.path.join(ROOT, "resources")
if ROOT not in sys.path: sys.path.append(ROOT)
AUTHENTICATORS = os.path.join(RESOURCES, "authenticators.txt")
ACCOUNTS = os.path.join(RESOURCES, "accounts.txt")

from alpaca.history import AlpacaBarsDownloader
from finance.technicals import TechnicalCalculator, TechnicalEquation
from finance.trendlines import TrendlineCalculator
from finance.backtesting import BackTestingCalculator
from finance.concepts import Concepts, Querys
from webscraping.webreaders import WebReader
from webscraping.websources import WebDelayer
from support.pipelines import Producer, Processor
from support.synchronize import RoutineThread
from support.concepts import DateRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


class BarDownloader(AlpacaBarsDownloader, Producer): pass
class TechnicalCalculator(TechnicalCalculator, Processor): pass
class TrendlineCalculator(TrendlineCalculator, Processor): pass
class BackTestingCalculator(BackTestingCalculator, Processor): pass


def main(*args, symbol, account, authenticator, delayer, parameters={}, **kwargs):
    with WebReader(account=account, authenticator=authenticator, delayer=delayer) as source:
        technical_equations = [TechnicalEquation.MACD(), TechnicalEquation.ATR(period=14)]
        bar_downloader = BarDownloader(name="BarDownloader", source=source)
        technical_calculator = TechnicalCalculator(name="TechnicalCalculator", equations=technical_equations)
        trendline_calculator = TrendlineCalculator(name="TrendlineCalculator", indicator=Concepts.Technicals.Trend.MACD, window=7, threshold=2/100, period=5)
        backtesting_calculator = BackTestingCalculator(name="BackTestingCalculator")
        backtesting_pipeline = bar_downloader + technical_calculator + trendline_calculator + backtesting_calculator
        backtesting_thread = RoutineThread(backtesting_pipeline, name="BackTestingThread").setup(symbol, **parameters)
        backtesting_thread.start()
        backtesting_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    sysAuthenticator = SimpleNamespace(**pd.read_csv(AUTHENTICATORS, sep=r"\s+", converters={"live": to_bool}).set_index(["website", "live"], inplace=False).loc[("alpaca", False)].to_dict())
    sysAccount = SimpleNamespace(**pd.read_csv(ACCOUNTS, sep=r"\s+", converters={"live": to_bool}).set_index(["website", "live"], inplace=False).loc[("alpaca", False)].to_dict())
    sysDelayer = WebDelayer(3)
    sysHistory = DateRange([(Datetime.today() - Timedelta(weeks=52*5)).date(), (Datetime.today() - Timedelta(days=1)).date()])
    sysParameters = dict(current=Datetime.now().date(), history=sysHistory)
    main(symbol=Querys.Symbol("SPY"), account=sysAccount, authenticator=sysAuthenticator, delayer=sysDelayer, parameters=sysParameters)



