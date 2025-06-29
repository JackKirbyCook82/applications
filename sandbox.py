# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 2025
@name:   Portfolio Trading
@author: Jack Kirby Cook

"""

import os
import sys
import random
import logging
import warnings
import pandas as pd
import xarray as xr
from enum import Enum
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from collections import namedtuple as ntuple

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
REPOSITORY = os.path.join(ROOT, "repository")
RESOURCES = os.path.join(ROOT, "resources")
if ROOT not in sys.path: sys.path.append(ROOT)
TICKERS = os.path.join(RESOURCES, "tickers.txt")
WEBAPI = os.path.join(RESOURCES, "webapi.txt")

from alpaca.market import AlpacaStockDownloader, AlpacaContractDownloader, AlpacaOptionDownloader
from alpaca.history import AlpacaBarsDownloader
from finance.securities import SecurityCalculator, PricingCalculator, AnalyticCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.technicals import TechnicalCalculator
from finance.greeks import GreekCalculator
from finance.variables import Querys, Variables, Strategies
from webscraping.webreaders import WebReader
from support.pipelines import Producer, Processor, Carryover
from support.synchronize import RoutineThread
from support.queues import Dequeuer, Queue
from support.variables import DateRange
from support.filters import Filter
from support.mixins import Delayer

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


Website = Enum("WebSite", "ALPACA ETRADE")
class SymbolDequeuer(Dequeuer, Carryover, Producer, signature="->symbol"): pass
class BarsDownloader(AlpacaBarsDownloader, Carryover, Processor, signature="symbol->bars"): pass
class StockDownloader(AlpacaStockDownloader, Carryover, Processor, signature="symbol->stock"): pass
class ContractDownloader(AlpacaContractDownloader, Carryover, Processor, signature="symbol->contract"): pass
class OptionDownloader(AlpacaOptionDownloader, Carryover, Processor, signature="contract->option"): pass
class TechnicalDownloader(TechnicalCalculator, Carryover, Processor, signature="bars->technical"): pass
class StockPricing(PricingCalculator, Carryover, Processor, query=Querys.Symbol, signature="stock->stock"): pass
class OptionPricing(PricingCalculator, Carryover, Processor, query=Querys.Settlement, signature="option->option"): pass
class AnalyticCalculator(AnalyticCalculator, Carryover, Processor, query=Querys.Settlement, signature="option,technical->option"): pass
class GreekCalculator(GreekCalculator, Carryover, Processor, signature="option->option"): pass
class SecurityCalculator(SecurityCalculator, Carryover, Processor, signature="stock,option->security"): pass
class SecurityFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="security->security"): pass
class StrategyCalculator(StrategyCalculator, Carryover, Processor, signature="security->strategy"): pass
class ValuationCalculator(ValuationCalculator, Carryover, Processor, signature="strategy->"): pass


def main(*args, symbols=[], webapi={}, delayers={}, parameters={}, **kwargs):
    symbol_feed = Queue.FIFO(contents=symbols, capacity=None, timeout=None)
    stock_pricing = lambda series: (series["ask"] * series["supply"] + series["bid"] * series["demand"]) / (series["supply"] + series["demand"])
    option_pricing = lambda series: (series["ask"] * series["supply"] + series["bid"] * series["demand"]) / (series["supply"] + series["demand"])
    security_criteria = lambda table: table["size"] >= 25

    with WebReader(delayer=delayers[Website.ALPACA]) as alpaca_source:
        symbols_dequeuer = SymbolDequeuer(name="SymbolDequeuer", feed=symbol_feed)
        bars_downloader = BarsDownloader(name="BarsDownloader", source=alpaca_source, webapi=webapi[Website.ALPACA])
        stocks_downloader = StockDownloader(name="StockDownloader", source=alpaca_source, webapi=webapi[Website.ALPACA])
        contract_downloader = ContractDownloader(name="ContractDownloader", source=alpaca_source, webapi=webapi[Website.ALPACA])
        options_downloader = OptionDownloader(name="OptionDownloader", source=alpaca_source, webapi=webapi[Website.ALPACA])
        technical_calculator = TechnicalDownloader(name="TechnicalCalculator", technicals=[Variables.Technical.STATISTIC])
        stock_pricing = StockPricing(name="StockPricing", pricing=stock_pricing)
        option_pricing = OptionPricing(name="OptionPricing", pricing=option_pricing)
        analytic_calculator = AnalyticCalculator(name="AnalyticCalculator")
        greek_calculator = GreekCalculator(name="GreekCalculator")
        security_calculator = SecurityCalculator(name="SecurityCalculator")
        security_filter = SecurityFilter(name="SecurityFilter", criteria=security_criteria)
        strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=[Strategies.Verticals.Call])
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        portfolio_pipeline = symbols_dequeuer + bars_downloader + stocks_downloader + contract_downloader + options_downloader + stocks_downloader
        portfolio_pipeline = portfolio_pipeline + technical_calculator + stock_pricing + option_pricing + analytic_calculator + security_calculator + security_filter
        portfolio_pipeline = portfolio_pipeline + strategy_calculator + valuation_calculator
        thread = RoutineThread(portfolio_pipeline, name="PortfolioThread").setup(**parameters)
        thread.start()
        thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 150)
    pd.set_option("display.width", 250)
    xr.set_options(**{"display_max_rows": 25, "display_width": 250})
    function = lambda contents: ntuple("Account", list(contents.keys()))(*contents.values())
    sysWebApi = pd.read_csv(WEBAPI, sep=" ", header=0, index_col=0, converters={0: lambda website: Website[str(website).upper()]})
    sysWebApi = {website: function(contents) for website, contents in sysWebApi.to_dict("index").items()}
    sysDelayers = {Website.ETRADE: Delayer(5), Website.ALPACA: Delayer(3)}
    with open(TICKERS, "r") as tickerfile:
        sysTickers = list(map(str.strip, tickerfile.read().split("\n")))
        sysSymbols = list(map(Querys.Symbol, sysTickers))
        random.shuffle(sysSymbols)
    sysHistory = DateRange([(Datetime.today() - Timedelta(days=1)).date(), (Datetime.today() - Timedelta(weeks=104)).date()])
    sysExpiry = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysParameters = dict(current=Datetime.now().date(), history=sysHistory, expiry=sysExpiry)
    sysParameters.update({"period": 252, "interest": 0.05, "dividend": 0.00, "discount": 0.05, "fees": 1.00})
    main(webapi=sysWebApi, delayers=sysDelayers, symbols=sysSymbols, parameters=sysParameters)
