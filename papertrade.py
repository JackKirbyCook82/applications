# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 2025
@name:   Alpaca Paper Trading
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
DRIVER = os.path.join(RESOURCES, "chromedriver.exe")
AUTHORIZE = os.path.join(RESOURCES, "authorize.txt")
WEBAPI = os.path.join(RESOURCES, "webapi.txt")

from etrade.market import ETradeStockDownloader, ETradeExpireDownloader, ETradeOptionDownloader
from etrade.service import ETradeWebService
from alpaca.market import AlpacaStockDownloader, AlpacaOptionDownloader, AlpacaContractDownloader
from alpaca.history import AlpacaBarsDownloader
from alpaca.orders import AlpacaOrderUploader
from finance.securities import StockCalculator, OptionCalculator, SecurityCalculator
from finance.technicals import TechnicalCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.greeks import GreekCalculator
from finance.market import MarketCalculator
from finance.variables import Variables, Querys, Strategies
from webscraping.webreaders import WebReader
from support.pipelines import Producer, Processor, Consumer, Carryover
from support.decorators import ValueDispatcher
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
Criterions = ntuple("Criterions", "security valuation")
Pricings = ntuple("Pricings", "stock option security")
Authorize = ntuple("Authorize", "username password")
WebAPI = ntuple("WebAPI", "identity code")


class SymbolDequeuer(Dequeuer, Carryover, Producer, signature="->symbol"): pass
class AlpacaBarsDownloader(AlpacaBarsDownloader, Carryover, Processor, signature="symbol->technical"): pass
class AlpacaStockDownloader(AlpacaStockDownloader, Carryover, Processor, signature="symbol->stock"): pass
class AlpacaContractDownloader(AlpacaContractDownloader, Carryover, Processor, signature="symbol->contract"): pass
class AlpacaOptionDownloader(AlpacaOptionDownloader, Carryover, Processor, signature="contract->option"): pass
class ETradeStockDownloader(ETradeStockDownloader, Carryover, Processor, signature="symbol->stock"): pass
class ETradeExpireDownloader(ETradeExpireDownloader, Carryover, Processor, signature="symbol->expire"): pass
class ETradeOptionDownloader(ETradeOptionDownloader, Carryover, Processor, signature="symbol,expire->option"): pass
class TechnicalCalculator(TechnicalCalculator, Carryover, Processor, signature="technical->technical"): pass
class StockCalculator(StockCalculator, Carryover, Processor, signature="stock,technical->stock"): pass
class OptionCalculator(OptionCalculator, Carryover, Processor, signature="option,stock->option"): pass
class GreekCalculator(GreekCalculator, Carryover, Processor, signature="option->option"): pass
class SecurityCalculator(SecurityCalculator, Carryover, Processor, signature="option->security"): pass
class SecurityFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="security->security"): pass
class StrategyCalculator(StrategyCalculator, Carryover, Processor, signature="security->strategy"): pass
class ValuationCalculator(ValuationCalculator, Carryover, Processor, signature="strategy->valuation"): pass
class ValuationFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="valuation->valuation"): pass
class MarketCalculator(MarketCalculator, Carryover, Processor, signature="valuation,security->prospect"): pass
class AlpacaOrderUploader(AlpacaOrderUploader, Carryover, Consumer, signature="prospect->"): pass


@ValueDispatcher(locator="website")
def papertrade(*args, website, **kwargs): raise ValueError(website)

@papertrade.register(Website.ALPACA)
def alpaca(*args, webapi, feed, pricings, criterions, priority, liquidity, parameters={}, **kwargs):
    with WebReader(delay=5) as source:
        symbols_dequeuer = SymbolDequeuer(name="SymbolDequeuer", feed=feed)
        bars_downloader = AlpacaBarsDownloader(name="BarsDownloader", source=source, api=webapi[Website.ALPACA])
        stocks_downloader = AlpacaStockDownloader(name="StockDownloader", source=source, api=webapi[Website.ALPACA])
        contracts_downloader = AlpacaContractDownloader(name="ContractDownloader", source=source, api=webapi[Website.ALPACA])
        options_downloader = AlpacaOptionDownloader(name="OptionDownloader", source=source, api=webapi[Website.ALPACA])
        technicals_calculator = TechnicalCalculator(name="TechnicalCalculator", technicals=[Variables.Technical.STATISTIC])
        stock_calculator = StockCalculator(name="StockCalculator", pricing=pricings.stock)
        option_calculator = OptionCalculator(name="OptionCalculator", pricing=pricings.option)
        greek_calculator = GreekCalculator(name="GreekCalculator")
        security_calculator = SecurityCalculator(name="SecurityCalculator", pricing=pricings.security)
        security_filter = SecurityFilter(name="SecurityFilter", criteria=criterions.security)
        strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies))
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        valuation_filter = ValuationFilter(name="ValuationFilter", criteria=criterions.valuation)
        market_calculator = MarketCalculator(name="MarketCalculator", priority=priority, liquidity=liquidity)
        order_uploader = AlpacaOrderUploader(name="OrderUploader", source=source, api=webapi[Website.ALPACA])
        pipeline = symbols_dequeuer + bars_downloader + stocks_downloader + contracts_downloader + options_downloader
        pipeline = pipeline + technicals_calculator + stock_calculator + option_calculator + greek_calculator + security_calculator + security_filter
        pipeline = pipeline + strategy_calculator + valuation_calculator + valuation_filter + market_calculator + order_uploader
        thread = RoutineThread(pipeline, name="PaperTradeThread").setup(**parameters)
        thread.start()
        thread.join()

@papertrade.register(Website.ETRADE)
def etrade(*args, authorize, webapi, feed, pricings, criterions, priority, liquidity, parameters={}, **kwargs):
    with ETradeWebService(delay=10, timeout=60, webapi=webapi[Website.ETRADE], executable=DRIVER, authorize=authorize[Website.ETRADE]) as primary, WebReader(delay=5) as secondary:
        source = ntuple("Source", "etrade alpaca")(primary, secondary)
        symbols_dequeuer = SymbolDequeuer(name="SymbolDequeuer", feed=feed)
        bars_downloader = AlpacaBarsDownloader(name="BarsDownloader", source=source.alpaca, api=webapi[Website.ALPACA])
        stocks_downloader = ETradeStockDownloader(name="StockDownloader", source=source.etrade, api=webapi[Website.ETRADE])
        expires_downloader = ETradeExpireDownloader(name="ExpireDownloader", source=source.etrade, api=webapi[Website.ETRADE])
        options_downloader = ETradeOptionDownloader(name="OptionDownloader", source=source.etrade, api=webapi[Website.ETRADE])
        technicals_calculator = TechnicalCalculator(name="TechnicalCalculator", technicals=[Variables.Technical.STATISTIC])
        stock_calculator = StockCalculator(name="StockCalculator", pricing=pricings.stock)
        option_calculator = OptionCalculator(name="OptionCalculator", pricing=pricings.option)
        greek_calculator = GreekCalculator(name="GreekCalculator")
        security_calculator = SecurityCalculator(name="SecurityCalculator", pricing=pricings.security)
        security_filter = SecurityFilter(name="SecurityFilter", criteria=criterions.security)
        strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies))
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        valuation_filter = ValuationFilter(name="ValuationFilter", criteria=criterions.valuation)
        market_calculator = MarketCalculator(name="MarketCalculator", priority=priority, liquidity=liquidity)
        order_uploader = AlpacaOrderUploader(name="OrderUploader", source=source.alpaca, api=webapi[Website.ALPACA])
        pipeline = symbols_dequeuer + bars_downloader + stocks_downloader + expires_downloader + options_downloader
        pipeline = pipeline + technicals_calculator + stock_calculator + option_calculator + greek_calculator + security_calculator + security_filter
        pipeline = pipeline + strategy_calculator + valuation_calculator + valuation_filter + market_calculator + order_uploader
        thread = RoutineThread(pipeline, name="PaperTradeThread").setup(**parameters)
        thread.start()
        thread.join()

def main(*args, website, webapi, authorize, symbols=[], parameters={}, **kwargs):
    feed = Queue.FIFO(contents=symbols, capacity=None, timeout=None)
    pricing = lambda series: (series["ask"] * series["supply"] + series["bid"] * series["demand"]) / (series["supply"] + series["demand"])
    priority = lambda series: series[("npv", Variables.Scenario.MINIMUM)]
    liquidity = lambda series: series["size"] * 0.1
    valuation = lambda table: table[("npv", Variables.Scenario.MINIMUM)] >= 10
    security = lambda table: table["size"] >= 10
    pricings = Pricings(pricing, pricing, lambda series: series["price"])
    criterions = Criterions(security, valuation)
    keywords = dict(webapi=webapi, authorize=authorize, feed=feed, priority=priority, liquidity=liquidity, pricings=pricings, criterions=criterions)
    papertrade(*args, website=website, parameters=parameters, **keywords, **kwargs)


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
    sysDates = DateRange([(Datetime.today() - Timedelta(days=1)).date(), (Datetime.today() - Timedelta(weeks=104)).date()])
    sysExpiry = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysParameters = dict(current=Datetime.now().date(), dates=sysDates, expiry=sysExpiry, term=Variables.Markets.Term.LIMIT, tenure=Variables.Markets.Tenure.DAY)
    sysParameters.update({"period": 252, "interest": 0.00, "dividend": 0.00, "discount": 0.00, "fees": 0.00})
    main(website=Website.ETRADE, webapi=sysWebAPI, authorize=sysAuthorize, symbols=sysSymbols, parameters=sysParameters)



