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

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
REPOSITORY = os.path.join(ROOT, "repository")
RESOURCES = os.path.join(ROOT, "resources")
if ROOT not in sys.path: sys.path.append(ROOT)
TICKERS = os.path.join(RESOURCES, "tickers.txt")
API = os.path.join(RESOURCES, "api.txt")

from alpaca.market import AlpacaContractDownloader, AlpacaStockDownloader, AlpacaOptionDownloader
from finance.prospects import ProspectTable, ProspectHeader, ProspectLayout
from finance.prospects import ProspectCalculator, ProspectWriter
from finance.valuations import ValuationCalculator
from finance.strategies import StrategyCalculator
from finance.securities import SecurityCalculator
from finance.variables import Variables, Querys, Strategies
from webscraping.webreaders import WebAPI, WebReader
from support.pipelines import Producer, Processor, Consumer
from support.queues import Dequeuer, Queue
from support.synchronize import RoutineThread
from support.filters import Filter, Criterion
from support.variables import DateRange
from support.transforms import Pivoter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


class SymbolDequeuer(Dequeuer, Producer, parser=Querys.Symbol): pass
class StockDownloader(AlpacaStockDownloader, Processor): pass
class ContractDownloader(AlpacaContractDownloader, Processor): pass
class OptionDownloader(AlpacaOptionDownloader, Processor): pass
class OptionFilter(Filter, Processor, query=Querys.Settlement): pass
class SecurityCalculator(SecurityCalculator, Processor): pass
class SecurityFilter(Filter, Processor, query=Querys.Settlement): pass
class StrategyCalculator(StrategyCalculator, Processor): pass
class ValuationCalculator(ValuationCalculator, Processor): pass
class ValuationPivoter(Pivoter, Processor, query=Querys.Settlement): pass
class ValuationFilter(Filter, Processor, query=Querys.Settlement): pass
class ProspectCalculator(ProspectCalculator, Processor): pass
class ProspectWriter(ProspectWriter, Consumer, query=Querys.Settlement): pass

class MarketReader(WebReader, delay=10): pass
class SecurityCriterion(Criterion, fields=["size"]):
    def execute(self, table): return self.size(table)
    def size(self, table): return table["size"] >= self["size"]

class ValuationCriterion(Criterion, fields=["apy", "cost", "size"]):
    def execute(self, table): return self.apy(table) & self.cost(table) & self.size(table)
    def apy(self, table): return table[("apy", Variables.Valuations.Scenario.MINIMUM)] >= self["apy"]
    def cost(self, table): return (table[("cost", Variables.Valuations.Scenario.MINIMUM)] > 0) & (table[("cost", Variables.Valuations.Scenario.MINIMUM)] <= self["cost"])
    def size(self, table): return table[("size", "")] >= self["size"]


def main(*args, tickers=[], expires=[], criterion={}, api, discount, fees, **kwargs):
    acquisition_layout = ProspectLayout(name="AcquisitionLayout", valuation=Variables.Valuations.Valuation.ARBITRAGE, rows=100)
    acquisition_header = ProspectHeader(name="AcquisitionHeader", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    acquisition_table = ProspectTable(name="AcquisitionTable", layout=acquisition_layout, header=acquisition_header)
    symbol_queue = Queue.FIFO(name="SymbolQueue", contents=tickers, capacity=None, timeout=None)
    acquisition_priority = lambda cols: cols[("apy", Variables.Valuations.Scenario.MINIMUM)]
    valuation_criterion = ValuationCriterion(**criterion)
    security_criterion = SecurityCriterion(**criterion)

    with MarketReader(name="MarketReader") as source:
        symbol_dequeuer = SymbolDequeuer(name="SymbolDequeuer", queue=symbol_queue)
        stock_downloader = StockDownloader(name="StockDownloader", source=source)
        contract_downloader = ContractDownloader(name="ContractDownloader", source=source)
        option_downloader = OptionDownloader(name="OptionDownloader", source=source)
        security_calculator = SecurityCalculator(name="SecurityCalculator", pricing=Variables.Markets.Pricing.CENTERED)
        security_filter = SecurityFilter(name="SecurityFilter", criterion=security_criterion)
        strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies))
        valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.Valuation.ARBITRAGE)
        valuation_pivoter = ValuationPivoter(name="ValuationPivoter", header=acquisition_header)
        valuation_filter = ValuationFilter(name="ValuationFilter", criterion=valuation_criterion)
        prospect_calculator = ProspectCalculator(name="ProspectCalculator", header=acquisition_header, priority=acquisition_priority)
        prospect_writer = ProspectWriter(name="ProspectWriter", table=acquisition_table)

        feed_pipeline = symbol_dequeuer + stock_downloader + contract_downloader + option_downloader
        market_pipeline = feed_pipeline + security_calculator + security_filter + strategy_calculator + valuation_calculator + valuation_pivoter + valuation_filter + prospect_calculator + prospect_writer
        market_thread = RoutineThread(market_pipeline, name="MarketThread").setup(expires=expires, api=api, discount=discount, fees=fees)
        market_thread.start()
        market_thread.cease()
        market_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    with open(TICKERS, "r") as tickerfile:
        sysTickers = list(map(str.strip, tickerfile.read().split("\n")))
        sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    with open(API, "r") as apifile:
        sysAPI = WebAPI(*json.loads(apifile.read())["etrade"])
    sysCriterion = dict(apy=1.50, cost=1000, size=10)
    main(api=sysAPI, tickers=sysTickers, expires=sysExpires, criterion=sysCriterion, discount=0.00, fees=0.00)



