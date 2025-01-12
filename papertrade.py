# -*- coding: utf-8 -*-
"""
Created on Tues Nov 19 2024
@name:   ETrade PaperTrading
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
from collections import namedtuple as ntuple
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
TICKERS = os.path.join(ROOT, "applications", "tickers.txt")
CHROME = os.path.join(ROOT, "resources", "chromedriver.exe")
API = os.path.join(ROOT, "applications", "api.txt")
if ROOT not in sys.path: sys.path.append(ROOT)

from etrade.market import ETradeProductDownloader, ETradeStockDownloader, ETradeOptionDownloader
from etrade.papertrade import ETradeTerminalWindow
from finance.variables import Variables, Categories, Querys
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.prospects import ProspectCalculator, ProspectWriter
from webscraping.webreaders import WebAuthorizer, WebReader
from webscraping.webdrivers import WebDriver, WebBrowser
from support.pipelines import Producer, Processor, Consumer
from support.queues import Dequeuer
from support.transforms import Pivot
from support.filters import Filter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


ETradeAPI = ntuple("API", "key code")
authorize = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
request = "https://api.etrade.com/oauth/request_token"
access = "https://api.etrade.com/oauth/access_token"
base = "https://api.etrade.com"

class SymbolDequeuerProducer(Dequeuer, Producer): pass
class StockDownloaderProcessor(ETradeStockDownloader, Processor): pass
class ProductDownloaderProcessor(ETradeProductDownloader, Processor): pass
class OptionDownloaderProcessor(ETradeOptionDownloader, Processor): pass
class OptionFilterProcessor(Filter, Processor, query=Querys.Contract): pass
class StrategyCalculatorProcessor(StrategyCalculator, Processor): pass
class ValuationCalculatorProcessor(ValuationCalculator, Processor): pass
class ValuationPivotProcessor(Pivot, Processor, query=Querys.Contract): pass
class ValuationFilterProcessor(Filter, Processor, query=Querys.Contract): pass
class ProspectCalculatorProcessor(ProspectCalculator, Processor): pass
class ProspectWriterConsumer(ProspectWriter, Consumer, query=Querys.Contract): pass

class ETradeAuthorizer(WebAuthorizer, authorize=authorize, request=request, access=access, base=base): pass
class ETradeDriver(WebDriver, browser=WebBrowser.Chrome, executable=CHROME, delay=10): pass
class ETradeReader(WebReader, delay=10): pass

Contract = ntuple("Contract", "ticker expire")
Stock = ntuple("Stock", "action quantity instrument")
Option = ntuple("Option", "action quantity instrument option strike")
Transaction = ntuple("Transaction", "terms price")
Order = ntuple("Order", "contract securities transaction")


def main(*args, **kwargs):
    contract = Contract("AAPL", Datetime(year=2025, month=2, day=25).date())
    stock = Stock(Variables.Actions.BUY, 100, Variables.Instruments.STOCK)
    put = Option(Variables.Actions.BUY, 1, Variables.Instruments.OPTION, Variables.Options.PUT, 235)
    call = Option(Variables.Actions.SELL, 1, Variables.Instruments.OPTION, Variables.Options.CALL, 235)
    transaction = (Variables.Terms.LIMITDEBIT, 235)
    order = Order(contract, [stock, put, call], transaction)

    with ETradeDriver(name="PaperTradeTerminal", port=8989) as source:
        window = ETradeTerminalWindow(*args, source=source, **kwargs)
        window.execute(order, *args, timeout=15, **kwargs)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    logging.getLogger("seleniumwire").setLevel(logging.ERROR)
    warnings.filterwarnings("ignore")
    main()



