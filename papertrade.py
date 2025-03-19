# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 2025
@name:   Alpaca Paper Trading
@author: Jack Kirby Cook

"""

import os
import sys
import json
import logging
import warnings
import pandas as pd
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
from alpaca.portfolio import AlpacaPortfolioDownloader
from alpaca.orders import AlpacaOrderUploader
from finance.market import AcquisitionCalculator, DivestitureCalculator
from finance.prospects import ProspectCalculator
from finance.valuations import ValuationCalculator
from finance.strategies import StrategyCalculator
from finance.securities import SecurityCalculator
from finance.variables import Variables, Querys, Strategies
from webscraping.webreaders import WebAuthorizerAPI, WebReader
from support.pipelines import Producer, Processor, Consumer, Carryover
from support.synchronize import RoutineThread, RepeatingThread
from support.filters import Filter, Criterion
from support.queues import Dequeuer, Queue
from support.variables import DateRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


class SymbolDequeuer(Dequeuer, Carryover, Producer, signature="->symbol"): pass
class PortfolioDownloader(AlpacaPortfolioDownloader, Carryover, Producer, signature="->symbol,contract"): pass
class StockDownloader(AlpacaStockDownloader, Carryover, Processor, signature="symbol->stock"): pass
class ContractDownloader(AlpacaContractDownloader, Carryover, Processor, signature="symbol->contract"): pass
class OptionDownloader(AlpacaOptionDownloader, Carryover, Processor, signature="contract->option"): pass
class OptionFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="option->option"): pass
class SecurityCalculator(SecurityCalculator, Carryover, Processor, signature="stock,option->security"): pass
class SecurityFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="security->security"): pass
class StrategyCalculator(StrategyCalculator, Carryover, Processor, signature="security->strategy"): pass
class ValuationCalculator(ValuationCalculator, Carryover, Processor, signature="strategy->valuation"): pass
class ValuationFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="valuation->valuation"): pass
class AcquisitionCalculator(AcquisitionCalculator, Carryover, Processor, signature="valuation,security->acquisition"): pass
class DivestitureCalculator(DivestitureCalculator, Carryover, Processor, signature="valuation,security->divestiture"): pass
class ProspectCalculator(ProspectCalculator, Carryover, Processor, signature="->prospect"): pass
class OrderUploader(AlpacaOrderUploader, Carryover, Consumer, signature="prospect->"): pass


class Criterions(ntuple("Criterion", "security valuation")): pass
class SecurityCriterion(Criterion, fields=["size"]):
    def execute(self, table): return self.size(table)
    def size(self, table): return table["size"] >= self["size"]

class ValuationCriterion(Criterion, fields=["apy", "npv"]):
    def execute(self, table): return self.apy(table) & self.npv(table)
    def apy(self, table): return table[("apy", Variables.Valuations.Scenario.MINIMUM)] >= self["apy"]
    def npv(self, table): return table[("npv", Variables.Valuations.Scenario.MINIMUM)] >= self["npv"]


def acquisition(*args, source, symbols, priority, liquidity, criterions, **kwargs):
    symbol_dequeuer = SymbolDequeuer(name="SymbolDequeuer", feed=symbols)
    stock_downloader = StockDownloader(name="StockDownloader", source=source)
    contract_downloader = ContractDownloader(name="ContractDownloader", source=source)
    option_downloader = OptionDownloader(name="OptionDownloader", source=source)
    security_calculator = SecurityCalculator(name="SecurityCalculator", pricing=Variables.Markets.Pricing.AGGRESSIVE)
    security_filter = SecurityFilter(name="SecurityFilter", criterion=criterions.security)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies.Verticals))
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    valuation_filter = ValuationFilter(name="ValuationFilter", criterion=criterions.valuation)
    acquisition_calculator = AcquisitionCalculator(name="AcquisitionCalculator", liquidity=liquidity, priority=priority)
    acquisition_pipeline = symbol_dequeuer + stock_downloader + contract_downloader + option_downloader
    acquisition_pipeline = acquisition_pipeline + security_calculator + security_filter + strategy_calculator
    acquisition_pipeline = acquisition_pipeline + valuation_calculator + valuation_filter + acquisition_calculator
    return acquisition_pipeline


def divestiture(*args, source, priority, liquidity, **kwargs):
    portfolio_downloader = PortfolioDownloader(name="PortfolioDownloader", source=source)
    stock_downloader = StockDownloader(name="StockDownloader", source=source)
    option_downloader = OptionDownloader(name="OptionDownloader", source=source)
    security_calculator = SecurityCalculator(name="SecurityCalculator", pricing=Variables.Markets.Pricing.AGGRESSIVE)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies.Verticals))
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    divestiture_calculator = DivestitureCalculator(name="DivestitureCalculator", liquidity=liquidity, priority=priority)
    divestiture_pipeline = portfolio_downloader + stock_downloader + option_downloader + security_calculator + strategy_calculator
    divestiture_pipeline = divestiture_pipeline + valuation_calculator + divestiture_calculator
    return divestiture_pipeline


def main(*args, api, symbols=[], expires=[], criterion={}, parameters={}, **kwargs):
    symbols = Queue.FIFO(contents=symbols, capacity=None, timeout=None)
    priority = lambda series: series[("apy", Variables.Valuations.Scenario.MINIMUM)]
    liquidity = lambda series: series[("size", "") if isinstance(series.index, pd.MultiIndex) else "size"] * 0.5
    criterions = Criterions(SecurityCriterion(**criterion), ValuationCriterion(**criterion))
    parameters = dict(api=api, expires=expires) | dict(parameters)

    with WebReader(name="PaperTradeReader", delay=2) as source:
        arguments = dict(symbols=symbols, expires=expires, priority=priority, liquidity=liquidity, criterions=criterions)
        acquisitions = acquisition(*args, source=source, **arguments, **kwargs)
        acquisitions = RoutineThread(acquisitions, name="AcquisitionThread").setup(**parameters)
        divestitures = divestiture(*args, source=source, **arguments, **kwargs)
        divestitures = RepeatingThread(divestitures, name="DivestitureThread", wait=60).setup(**parameters)
        acquisitions.start()
        divestitures.start()
        acquisitions.join()
        divestitures.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    with open(TICKERS, "r") as tickerfile:
        sysTickers = list(map(str.strip, tickerfile.read().split("\n")))
        sysSymbols = list(map(Querys.Symbol, sysTickers))
        sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    with open(API, "r") as apifile:
        sysAPI = WebAuthorizerAPI(*json.loads(apifile.read())["alpaca"])
    sysCriterion = dict(apy=2.50, npv=50, size=10)
    sysParameters = dict(discount=0.00, fees=0.00, term=Variables.Markets.Terms.MARKET, tenure=Variables.Markets.Tenure.FILLKILL, date=Datetime.now().date())
    main(api=sysAPI, symbols=sysSymbols, expires=sysExpires, criterion=sysCriterion, parameters=sysParameters)



