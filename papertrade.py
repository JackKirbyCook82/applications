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
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
TICKERS = os.path.join(ROOT, "applications", "tickers.txt")
CHROME = os.path.join(ROOT, "resources", "chromedriver.exe")
API = os.path.join(ROOT, "applications", "api.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from etrade.market import ETradeProductDownloader, ETradeStockDownloader, ETradeOptionDownloader
from finance.variables import Variables, Querys, DateRange
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationWriter, ValuationTable
from webscraping.webreaders import WebAuthorizer, WebReader
from webscraping.webdrivers import WebDriver, WebBrowser
from support.pipelines import Producer, Processor, Consumer
from support.queues import Dequeuer, QueueTypes, Queue
from support.operations import Filter, Criterion
from support.synchronize import RoutineThread
from support.mixins import Carryover


__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


authorize = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
request = "https://api.etrade.com/oauth/request_token"
access = "https://api.etrade.com/oauth/access_token"
base = "https://api.etrade.com"

class SymbolDequeuerProducer(Dequeuer, Producer): pass
class StockDownloaderProcessor(ETradeStockDownloader, Processor, Carryover, carryover="symbol", leading=True): pass
class ProductDownloaderProcessor(ETradeProductDownloader, Processor): pass
class OptionDownloaderProcessor(ETradeOptionDownloader, Processor, Carryover, carryover="product", leading=True): pass
class OptionFilterProcessor(Filter, Processor, Carryover, carryover="product", leading=True): pass
class StrategyCalculatorProcessor(StrategyCalculator, Processor, Carryover, carryover="product", leading=True): pass
class ValuationCalculatorProcessor(ValuationCalculator, Processor, Carryover, carryover="product", leading=True): pass
class ValuationFilterProcessor(Filter, Processor, Carryover, carryover="product", leading=True): pass
class ValuationWriterConsumer(ValuationWriter, Consumer): pass

class ETradeAuthorizer(WebAuthorizer, authorize=authorize, request=request, access=access, base=base): pass
class ETradeDriver(WebDriver, browser=WebBrowser.CHROME, executable=CHROME, delay=10): pass
class ETradeReader(WebReader, delay=10): pass


def main(*args, arguments, parameters, **kwargs):
    security_authorizer = ETradeAuthorizer(name="MarketAuthorizer", apikey=arguments["apikey"], apicode=arguments["apicode"])
    symbol_queue = Queue[QueueTypes.FIFO](name="SymbolQueue", contents=arguments["symbols"], capacity=None, timeout=None)
    option_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    valuation_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    acquisition_table = ValuationTable(name="AcquisitionTable", valuation=Variables.Valuations.ARBITRAGE)

    with ETradeReader(name="MarketReader", authorizer=security_authorizer) as reader:
        symbol_dequeue = SymbolDequeuerProducer(name="SymbolsDequeuer", queue=symbol_queue)
        stock_downloader = StockDownloaderProcessor(name="StockDownloader", feed=reader)
        product_downloader = ProductDownloaderProcessor(name="ProductDownloader", feed=reader)
        option_downloader = OptionDownloaderProcessor(name="OptionDownloader", feed=reader)
        option_filter = OptionFilterProcessor(name="OptionFilter", criterion=option_criterion)
        strategy_calculator = StrategyCalculatorProcessor(name="StrategyCalculator", strategies=Variables.Strategies)
        valuation_calculator = ValuationCalculatorProcessor(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
        valuation_filter = ValuationFilterProcessor(name="ValuationFilter", criterion=valuation_criterion)
        valuation_writer = ValuationWriterConsumer(name="ValuationWriter", table=acquisition_table, valuation=Variables.Valuations.ARBITRAGE, priority=valuation_priority)

        market_pipeline = symbol_dequeue + stock_downloader + product_downloader + option_downloader + option_filter + strategy_calculator + valuation_calculator + valuation_filter + valuation_writer
        market_thread = RoutineThread(market_pipeline, name="MarketThread").setup(**parameters)
        market_thread.start()
        market_thread.join()

    with ETradeDriver(name="PaperTradeReader", port=8989) as driver:
        pass


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    logging.getLogger("seleniumwire").setLevel(logging.ERROR)
    warnings.filterwarnings("ignore")
    with open(API, "r") as apifile:
        sysApiKey, sysApiCode = [str(string).strip() for string in str(apifile.read()).split("\n")]
    with open(TICKERS, "r") as tickerfile:
        sysSymbols = [Querys.Symbol(str(string).strip().upper()) for string in tickerfile.read().split("\n")]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysArguments = dict(apikey=sysApiKey, apicode=sysApiCode, symbols=sysSymbols, apy=0.25, size=10, volume=100, interest=100)
    sysParameters = dict(expires=sysExpires, discount=0.05, fees=1.00)
    main(arguments=sysArguments, parameters=sysParameters)



