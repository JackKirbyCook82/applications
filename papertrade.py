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

from alpaca.market import AlpacaContractDownloader, AlpacaStockDownloader, AlpacaOptionDownloader
from alpaca.orders import AlpacaOrderUploader, AlpacaOrderCalculator
from finance.prospects import ProspectCalculator, ProspectWriter, ProspectReader, ProspectHeader
from finance.valuations import ValuationCalculator
from finance.strategies import StrategyCalculator
from finance.securities import SecurityCalculator
from finance.variables import Variables, Querys, Strategies
from webscraping.webreaders import WebAuthorizerAPI, WebReader
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
class ProspectReader(ProspectReader, Producer, query=Querys.Settlement): pass
class OrderCalculator(AlpacaOrderCalculator, Processor): pass
class OrderUploader(AlpacaOrderUploader, Consumer): pass

class AcquisitionCriterion(ntuple("Criterion", "security valuation")): pass
class SecurityCriterion(Criterion, fields=["size"]):
    def execute(self, table): return self.size(table)
    def size(self, table): return table["size"] >= self["size"]

class ValuationCriterion(Criterion, fields=["apy", "cost", "size"]):
    def execute(self, table): return self.apy(table) & self.cost(table) & self.size(table)
    def apy(self, table): return table[("apy", Variables.Valuations.Scenario.MINIMUM)] >= self["apy"]
    def cost(self, table): return table[("cost", Variables.Valuations.Scenario.MINIMUM)] <= self["cost"]
    def size(self, table): return table[("size", "")] >= self["size"]


def acquisition(*args, source, symbols, criterion, header, priority, **kwargs):
    symbol_dequeuer = SymbolDequeuer(name="SymbolDequeuer", queue=symbols)
    stock_downloader = StockDownloader(name="StockDownloader", source=source)
    contract_downloader = ContractDownloader(name="ContractDownloader", source=source)
    option_downloader = OptionDownloader(name="OptionDownloader", source=source)
    security_calculator = SecurityCalculator(name="SecurityCalculator", pricing=Variables.Markets.Pricing.CENTERED)
    security_filter = SecurityFilter(name="SecurityFilter", criterion=criterion.security)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies))
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    valuation_pivoter = ValuationPivoter(name="ValuationPivoter", header=header)
    valuation_filter = ValuationFilter(name="ValuationFilter", criterion=criterion.valuation)
    prospect_calculator = ProspectCalculator(name="ProspectCalculator", header=header, priority=priority)
    order_calculator = OrderCalculator(name="OrderCalculator")
    order_uploader = OrderUploader(name="OrderUploader", source=source)
    acquisition_pipeline = symbol_dequeuer + stock_downloader + contract_downloader + option_downloader
    acquisition_pipeline = acquisition_pipeline + security_calculator + security_filter + strategy_calculator
    acquisition_pipeline = acquisition_pipeline + valuation_calculator + valuation_pivoter + valuation_filter
    acquisition_pipeline = acquisition_pipeline + prospect_calculator + order_calculator + order_uploader
    return acquisition_pipeline


def main(*args, tickers=[], expires=[], criterion={}, api, discount, fees, **kwargs):
    symbol_queue = Queue.FIFO(name="SymbolQueue", contents=tickers, capacity=None, timeout=None)
    acquisition_header = ProspectHeader(name="AcquisitionHeader", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    acquisition_criterion = AcquisitionCriterion(SecurityCriterion(**criterion), ValuationCriterion(**criterion))
    acquisition_priority = lambda cols: cols[("apy", Variables.Valuations.Scenario.MINIMUM)]

    with WebReader(delay=10) as source:
        acquisition_parameters = dict(source=source, symbols=symbol_queue, header=acquisition_header, criterion=acquisition_criterion, priority=acquisition_priority)
        acquisition_pipeline = acquisition(*args, **acquisition_parameters, **kwargs)
        acquisition_thread = RoutineThread(acquisition_pipeline, name="AcquisitionThread").setup(expires=expires, api=api, discount=discount, fees=fees)

        acquisition_thread.start()
        acquisition_thread.cease()
        acquisition_thread.join()


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
        sysAPI = WebAuthorizerAPI(*json.loads(apifile.read())["alpaca"])
    sysCriterion = dict(apy=1.50, cost=1000, size=10)
    main(api=sysAPI, tickers=sysTickers, expires=sysExpires, criterion=sysCriterion, discount=0.00, fees=0.00)



