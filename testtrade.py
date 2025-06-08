# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 2025
@name:   TestTrading
@author: Jack Kirby Cook

"""

import os
import sys
import json
import random
import logging
import warnings
import pandas as pd
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
AUTHORIZE = os.path.join(RESOURCES, "authorize.txt")
WEBAPI = os.path.join(RESOURCES, "webapi.txt")

from alpaca.market import AlpacaStockDownloader, AlpacaContractDownloader, AlpacaOptionDownloader
from alpaca.orders import AlpacaOrderUploader
from finance.securities import SecurityCalculator, PricingCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.market import MarketCalculator
from finance.variables import Variables, Querys, Strategies
from webscraping.webreaders import WebReader
from support.pipelines import Producer, Processor, Consumer, Carryover
from support.synchronize import RoutineThread
from support.queues import Dequeuer, Queue
from support.variables import DateRange
from support.filters import Filter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


Website = Enum("WebSite", "ALPACA ETRADE")
WebAPI = ntuple("WebAPI", "identity code")
Authorize = ntuple("Authorize", "username password")
Criterions = ntuple("Criterions", "security valuation")
Pricings = ntuple("Pricings", "stock option security")


class SymbolDequeuer(Dequeuer, Carryover, Producer, signature="->symbol"): pass
class StockDownloader(AlpacaStockDownloader, Carryover, Processor, signature="symbol->stock"): pass
class ContractDownloader(AlpacaContractDownloader, Carryover, Processor, signature="symbol->contract"): pass
class OptionDownloader(AlpacaOptionDownloader, Carryover, Processor, signature="contract->option"): pass
class StockPricing(PricingCalculator, Carryover, Processor, query=Querys.Symbol, signature="stock->stock"): pass
class OptionPricing(PricingCalculator, Carryover, Processor, query=Querys.Settlement, signature="option->option"): pass
class SecurityCalculator(SecurityCalculator, Carryover, Processor, signature="stock,option->security"): pass
class SecurityFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="security->security"): pass
class StrategyCalculator(StrategyCalculator, Carryover, Processor, signature="security->strategy"): pass
class ValuationCalculator(ValuationCalculator, Carryover, Processor, signature="strategy->valuation"): pass
class ValuationFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="valuation->valuation"): pass
class MarketCalculator(MarketCalculator, Carryover, Processor, signature="valuation,security->prospect"): pass
class OrderUploader(AlpacaOrderUploader, Carryover, Consumer, signature="prospect->"): pass


def main(*args, webapi, symbols=[], parameters={}, **kwargs):
    symbol_feed = Queue.FIFO(contents=symbols, capacity=None, timeout=None)
    stock_pricing = lambda series: (series["ask"] * series["supply"] + series["bid"] * series["demand"]) / (series["supply"] + series["demand"])
    option_pricing = lambda series: (series["ask"] * series["supply"] + series["bid"] * series["demand"]) / (series["supply"] + series["demand"])
    valuation_priority = lambda series: series[("npv", Variables.Scenario.MINIMUM)]
    valuation_liquidity = lambda series: series["size"] * 0.1
    valuation_criteria = lambda table: table[("npv", Variables.Scenario.MINIMUM)] >= 10
    security_criteria = lambda table: table["size"] >= 10
    strategy_selection = list(Strategies)

    alpaca_parameters = dict(delay=3, api=webapi[Website.ALPACA])
    with WebReader(**alpaca_parameters) as alpaca_source:
        symbols_dequeuer = SymbolDequeuer(name="SymbolDequeuer", feed=symbol_feed)
        stocks_downloader = StockDownloader(name="StockDownloader", source=alpaca_source, api=webapi[Website.ALPACA])
        contract_downloader = ContractDownloader(name="ContractDownloader", source=alpaca_source, api=webapi[Website.ALPACA])
        options_downloader = OptionDownloader(name="OptionDownloader", source=alpaca_source, api=webapi[Website.ALPACA])
        stock_pricing = StockPricing(name="StockPricing", pricing=stock_pricing)
        option_pricing = OptionPricing(name="OptionPricing", pricing=option_pricing)
        security_calculator = SecurityCalculator(name="SecurityCalculator")
        security_filter = SecurityFilter(name="SecurityFilter", criteria=security_criteria)
        strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=strategy_selection)
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        valuation_filter = ValuationFilter(name="ValuationFilter", criteria=valuation_criteria)
        market_calculator = MarketCalculator(name="MarketCalculator", priority=valuation_priority, liquidity=valuation_liquidity)
        order_uploader = OrderUploader(name="OrderUploader", source=alpaca_source, api=webapi[Website.ALPACA])
        algotrade_pipeline = symbols_dequeuer + stocks_downloader + contract_downloader + options_downloader
        algotrade_pipeline = algotrade_pipeline + stock_pricing + option_pricing + security_calculator + security_filter
        algotrade_pipeline = algotrade_pipeline + strategy_calculator + valuation_calculator + valuation_filter
        algotrade_pipeline = algotrade_pipeline + market_calculator + order_uploader
        thread = RoutineThread(algotrade_pipeline, name="TestTradeThread").setup(**parameters)
        thread.start()
        thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    with open(WEBAPI, "r") as apifile:
        sysWebAPI = json.loads(apifile.read()).items()
        sysWebAPI = {Website[str(website).upper()]: WebAPI(*values) for website, values in sysWebAPI}
    with open(AUTHORIZE, "r") as authfile:
        sysAuthorize = json.loads(authfile.read()).items()
        sysAuthorize = {Website[str(website).upper()]: Authorize(*values) for website, values in sysAuthorize}
    with open(TICKERS, "r") as tickerfile:
        sysTickers = list(map(str.strip, tickerfile.read().split("\n")))
        sysSymbols = list(map(Querys.Symbol, sysTickers))
        random.shuffle(sysSymbols)
    sysExpiry = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysParameters = dict(current=Datetime.now().date(), expiry=sysExpiry, term=Variables.Markets.Term.LIMIT, tenure=Variables.Markets.Tenure.DAY)
    sysParameters.update({"period": 252, "interest": 0.00, "dividend": 0.00, "discount": 0.00, "fees": 0.00})
    main(website=Website.ETRADE, webapi=sysWebAPI, authorize=sysAuthorize, symbols=sysSymbols, parameters=sysParameters)



