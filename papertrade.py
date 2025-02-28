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
from alpaca.orders import AlpacaOrderUploader
from finance.prospects import ProspectCalculator, ProspectHeader
from finance.valuations import ValuationCalculator
from finance.strategies import StrategyCalculator
from finance.securities import SecurityCalculator
from finance.variables import Variables, Querys, Strategies
from webscraping.webreaders import WebAuthorizerAPI, WebReader
from support.pipelines import Producer, Processor, Consumer, Carryover
from support.filters import Filter, Criterion
from support.synchronize import RoutineThread
from support.queues import Dequeuer, Queue
from support.variables import DateRange
from support.transforms import Pivoter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


class SymbolDequeuer(Dequeuer, Carryover, Producer, signature="->symbol"): pass
class StockDownloader(AlpacaStockDownloader, Carryover, Processor, signature="symbol->stock"): pass
class ContractDownloader(AlpacaContractDownloader, Carryover, Processor, signature="symbol->contract"): pass
class OptionDownloader(AlpacaOptionDownloader, Carryover, Processor, signature="contract->option"): pass
class OptionFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="option->option"): pass
class SecurityCalculator(SecurityCalculator, Carryover, Processor, signature="stock,option->security"): pass
class SecurityFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="security->security"): pass
class StrategyCalculator(StrategyCalculator, Carryover, Processor, signature="security->strategy"): pass
class ValuationCalculator(ValuationCalculator, Carryover, Processor, signature="strategy->valuation"): pass
class ValuationPivoter(Pivoter, Carryover, Processor, query=Querys.Settlement, signature="valuation->valuation"): pass
class ValuationFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="valuation->valuation"): pass
class ProspectCalculator(ProspectCalculator, Carryover, Processor, signature="valuation->prospect"): pass
class OrderUploader(AlpacaOrderUploader, Carryover, Consumer, signature="prospect->"): pass

class Criterions(ntuple("Criterion", "security valuation")): pass
class SecurityCriterion(Criterion, fields=["size"]):
    def execute(self, table): return self.size(table)
    def size(self, table): return table["size"] >= self["size"]

class ValuationCriterion(Criterion, fields=["apy", "npv", "cost", "size"]):
    def execute(self, table): return self.apy(table) & self.npv(table) & self.cost(table) & self.size(table)
    def apy(self, table): return table[("apy", Variables.Valuations.Scenario.MINIMUM)] >= self["apy"]
    def npv(self, table): return table[("npv", Variables.Valuations.Scenario.MINIMUM)] >= self["npv"]
    def cost(self, table): return table[("cost", Variables.Valuations.Scenario.MINIMUM)] <= self["cost"]
    def size(self, table): return table[("size", "")] >= self["size"]


def acquisition(*args, source, feed, header, priority, criterions, **kwargs):
    symbol_dequeuer = SymbolDequeuer(name="SymbolDequeuer", feed=feed)
    stock_downloader = StockDownloader(name="StockDownloader", source=source)
    contract_downloader = ContractDownloader(name="ContractDownloader", source=source)
    option_downloader = OptionDownloader(name="OptionDownloader", source=source)
    security_calculator = SecurityCalculator(name="SecurityCalculator", pricing=Variables.Markets.Pricing.MARKET)
    security_filter = SecurityFilter(name="SecurityFilter", criterion=criterions.security)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies.Verticals))
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    valuation_pivoter = ValuationPivoter(name="ValuationPivoter", header=header)
    valuation_filter = ValuationFilter(name="ValuationFilter", criterion=criterions.valuation)
    prospect_calculator = ProspectCalculator(name="ProspectCalculator", header=header, priority=priority)
    order_uploader = OrderUploader(name="OrderUploader", source=source)
    acquisition_pipeline = symbol_dequeuer + stock_downloader + contract_downloader + option_downloader
    acquisition_pipeline = acquisition_pipeline + security_calculator + security_filter + strategy_calculator
    acquisition_pipeline = acquisition_pipeline + valuation_calculator + valuation_pivoter + valuation_filter
    acquisition_pipeline = acquisition_pipeline + prospect_calculator + order_uploader
    return acquisition_pipeline


def main(*args, symbols=[], expires=[], api, criterion={}, discount, fees, **kwargs):
    header = ProspectHeader(name="AcquisitionHeader", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    feed = Queue.FIFO(contents=symbols, capacity=None, timeout=None)
    priority = lambda cols: cols[("apy", Variables.Valuations.Scenario.MINIMUM)]
    criterions = Criterions(SecurityCriterion(**criterion), ValuationCriterion(**criterion))
    with WebReader(name="AcquisitionReader", delay=2) as source:
        pipeline = acquisition(*args, source=source, feed=feed, header=header, priority=priority, criterions=criterions, **kwargs)
        parameters = dict(expires=expires, api=api, discount=discount, fees=fees)
        thread = RoutineThread(pipeline, name="AcquisitionThread").setup(**parameters)
        thread.start()
        thread.cease()
        thread.join()


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
    sysCriterion = dict(apy=1.00, npv=1.00, cost=1000, size=10)
    main(api=sysAPI, symbols=sysSymbols, expires=sysExpires, criterion=sysCriterion, discount=0.00, fees=0.00)



