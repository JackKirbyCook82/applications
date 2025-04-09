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
from abc import ABC
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
API = os.path.join(RESOURCES, "api.txt")

from alpaca.market import AlpacaStockDownloader, AlpacaOptionDownloader, AlpacaContractDownloader
from etrade.market import ETradeStockDownloader, ETradeOptionDownloader, ETradeExpireDownloader
from alpaca.portfolio import AlpacaPortfolioDownloader
from finance.market import AcquisitionCalculator, AcquisitionSaver, AcquisitionParameters
from finance.securities import StockCalculator, OptionCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.variables import Variables, Querys, Strategies
from webscraping.webreaders import WebAuthorizer, WebAuthorizerAPI, WebReader
from support.pipelines import Producer, Processor, Consumer, Carryover
from support.decorators import ValueDispatcher
from support.synchronize import RoutineThread
from support.filters import Filter, Criterion
from support.queues import Dequeuer, Queue
from support.variables import DateRange
from support.files import File

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


Website = Enum("WebSite", "ALPACA ETRADE")
authorize = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
request = "https://api.etrade.com/oauth/request_token"
access = "https://api.etrade.com/oauth/access_token"
base = "https://api.etrade.com"


class SymbolDequeuer(Dequeuer, Carryover, Producer, signature="->symbol"): pass
class PortfolioDownloader(AlpacaPortfolioDownloader, Carryover, Producer, signature="->contract"): pass
class AlpacaStockDownloader(AlpacaStockDownloader, Carryover, Processor, signature="symbol->stock"): pass
class AlpacaContractDownloader(AlpacaContractDownloader, Carryover, Processor, signature="symbol->contract"): pass
class AlpacaOptionDownloader(AlpacaOptionDownloader, Carryover, Processor, signature="contract->option"): pass
class ETradeStockDownloader(ETradeStockDownloader, Carryover, Processor, signature="symbol->stock"): pass
class ETradeExpireDownloader(ETradeExpireDownloader, Carryover, Processor, signature="symbol->expire"): pass
class ETradeOptionDownloader(ETradeOptionDownloader, Carryover, Processor, signature="symbol,expire->option"): pass
class StockCalculator(StockCalculator, Carryover, Processor, signature="stock->stock"): pass
class OptionCalculator(OptionCalculator, Carryover, Processor, signature="option->option"): pass
class OptionFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="option->option"): pass
class StrategyCalculator(StrategyCalculator, Carryover, Processor, signature="stock,option->strategy"): pass
class ValuationCalculator(ValuationCalculator, Carryover, Processor, signature="strategy->valuation"): pass
class ValuationFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="valuation->valuation"): pass
class AcquisitionCalculator(AcquisitionCalculator, Carryover, Processor, signature="valuation,option->acquisition"): pass
class AcquisitionSaver(AcquisitionSaver, Carryover, Consumer, signature="acquisition->"): pass


class Criterions(ntuple("Criterion", "security valuation")): pass
class SecurityCriterion(Criterion, ABC, fields=["size"]):
    def execute(self, table): return table["size"] >= self["size"]

class ValuationCriterion(Criterion, fields=["npv"]):
    def execute(self, table): return table[("npv", Variables.Valuations.Scenario.MINIMUM)] >= self["npv"]


@ValueDispatcher(locator="website")
def feed(*args, website, **kwargs): raise ValueError(website)

@feed.register(Website.ALPACA)
def alpaca(producer, *args, source, **kwargs):
    stock_downloader = AlpacaStockDownloader(name="StockDownloader", source=source)
    contract_downloader = AlpacaContractDownloader(name="ContractDownloader", source=source)
    option_downloader = AlpacaOptionDownloader(name="OptionDownloader", source=source)
    alpaca_producer = producer + stock_downloader + contract_downloader + option_downloader
    return alpaca_producer

@feed.register(Website.ETRADE)
def etrade(producer, *args, source, **kwargs):
    stock_downloader = ETradeStockDownloader(name="StockDownloader", source=source)
    expire_downloader = ETradeExpireDownloader(name="ExpireDownloader", source=source)
    option_downloader = ETradeOptionDownloader(name="OptionDownloader", source=source)
    etrade_producer = producer + stock_downloader + expire_downloader + option_downloader
    return etrade_producer

def authorizer(*args, website, api, **kwargs):
    if website is Website.ETRADE: return WebAuthorizer(api=api, authorize=authorize, request=request, access=access, base=base)
    else: return None

def acquisition(producer, *args, file, criterions, priority, liquidity, **kwargs):
    stock_calculator = StockCalculator(name="StockCalculator", pricing=Variables.Markets.Pricing.AGGRESSIVE)
    option_calculator = OptionCalculator(name="OptionCalculator", pricing=Variables.Markets.Pricing.AGGRESSIVE)
    option_filter = OptionFilter(name="OptionFilter", criterion=criterions.security)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies))
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    valuation_filter = ValuationFilter(name="ValuationFilter", criterion=criterions.valuation)
    acquisition_calculation = AcquisitionCalculator(name="AcquisitionCalculator", priority=priority, liquidity=liquidity)
    acquisition_saver = AcquisitionSaver(name="AcquisitionSaver", file=file, mode="a")
    acquisition_pipeline = producer + stock_calculator + option_calculator + option_filter + strategy_calculator + valuation_calculator + valuation_filter + acquisition_calculation + acquisition_saver
    return acquisition_pipeline

def main(*args, website, api, symbols=[], expiry=[], criterions, parameters={}, **kwargs):
    file = File(repository=REPOSITORY, folder="acquisitions", **dict(AcquisitionParameters))
    symbols = Queue.FIFO(contents=symbols, capacity=None, timeout=None)
    priority = lambda series: series[("npv", Variables.Valuations.Scenario.MINIMUM)]
    liquidity = lambda series: series["size"] * 0.1
    arguments = dict(criterions=criterions, priority=priority, liquidity=liquidity)
    parameters = dict(api=api, expiry=expiry) | dict(parameters)

    with WebReader(authorizer=authorizer(website=website, api=api), delay=3) as source:
        symbol_dequeuer = SymbolDequeuer(name="SymbolDequeuer", feed=symbols)
        feed_pipeline = feed(symbol_dequeuer, *args, website=website, source=source, **arguments, **kwargs)
        acquisition_pipeline = acquisition(feed_pipeline, *args, file=file, **arguments, **kwargs)
        acquisitions_thread = RoutineThread(acquisition_pipeline, name="AcquisitionThread").setup(**parameters)
        acquisitions_thread.start()
        acquisitions_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    sysWebSite = Website.ALPACA
    with open(TICKERS, "r") as tickerfile:
        sysTickers = list(map(str.strip, tickerfile.read().split("\n")))
        sysSymbols = list(map(Querys.Symbol, sysTickers))
        random.shuffle(sysSymbols)
        sysExpiry = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    with open(API, "r") as apifile:
        sysAPI = WebAuthorizerAPI(*json.loads(apifile.read())[str(sysWebSite.name).lower()])
    sysCriterions = Criterions(SecurityCriterion(size=25), ValuationCriterion(npv=100))
    sysParameters = dict(discount=0.50, fees=1.00, term=Variables.Markets.Term.LIMIT, tenure=Variables.Markets.Tenure.DAY, date=Datetime.now().date())
    main(website=sysWebSite, api=sysAPI, symbols=sysSymbols, expiry=sysExpiry, criterions=sysCriterions, parameters=sysParameters)



