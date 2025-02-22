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
from alpaca.orders import AlpacaOrderUploader
from finance.prospects import ProspectCalculator, ProspectHeader
from finance.orders import OrderCalculator
from finance.valuations import ValuationCalculator
from finance.strategies import StrategyCalculator
from finance.securities import SecurityCalculator
from finance.variables import Variables, Querys, Strategies
from webscraping.webreaders import WebAuthorizerAPI, WebReader
from support.pipelines import Producer, Processor, Consumer
from support.filters import Filter, Criterion
from support.synchronize import RoutineThread
from support.variables import DateRange
from support.transforms import Pivoter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


class StockDownloader(AlpacaStockDownloader, Producer): pass
class ContractDownloader(AlpacaContractDownloader, Producer): pass
class OptionDownloader(AlpacaOptionDownloader, Processor): pass
class OptionFilter(Filter, Processor, query=Querys.Settlement): pass
class SecurityCalculator(SecurityCalculator, Processor): pass
class SecurityFilter(Filter, Processor, query=Querys.Settlement): pass
class StrategyCalculator(StrategyCalculator, Processor): pass
class ValuationCalculator(ValuationCalculator, Processor): pass
class ValuationPivoter(Pivoter, Processor, query=Querys.Settlement): pass
class ValuationFilter(Filter, Processor, query=Querys.Settlement): pass
class ProspectCalculator(ProspectCalculator, Processor): pass
class OrderCalculator(OrderCalculator, Processor): pass
class OrderUploader(AlpacaOrderUploader, Consumer): pass

class Criterions(ntuple("Criterion", "security valuation")): pass
class SecurityCriterion(Criterion, fields=["size"]):
    def execute(self, table): return self.size(table)
    def size(self, table): return table["size"] >= self["size"]

class ValuationCriterion(Criterion, fields=["apy", "cost", "size"]):
    def execute(self, table): return self.apy(table) & self.cost(table) & self.size(table)
    def apy(self, table): return table[("apy", Variables.Valuations.Scenario.MINIMUM)] >= self["apy"]
    def cost(self, table): return table[("cost", Variables.Valuations.Scenario.MINIMUM)] <= self["cost"]
    def size(self, table): return table[("size", "")] >= self["size"]


def acquisition(*args, source, header, priority, criterions, **kwargs):
    contract_downloader = ContractDownloader(name="ContractDownloader", source=source)
    option_downloader = OptionDownloader(name="OptionDownloader", source=source)
    security_calculator = SecurityCalculator(name="SecurityCalculator", pricing=Variables.Markets.Pricing.CENTERED)
    security_filter = SecurityFilter(name="SecurityFilter", criterion=criterions.security)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies))
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    valuation_pivoter = ValuationPivoter(name="ValuationPivoter", header=header)
    valuation_filter = ValuationFilter(name="ValuationFilter", criterion=criterions.valuation)
    prospect_calculator = ProspectCalculator(name="ProspectCalculator", header=header, priority=priority)
    order_calculator = OrderCalculator(name="OrderCalculator")
    order_uploader = OrderUploader(name="OrderUploader", source=source)
    acquisition_pipeline = contract_downloader + option_downloader
    acquisition_pipeline = acquisition_pipeline + security_calculator + security_filter + strategy_calculator
    acquisition_pipeline = acquisition_pipeline + valuation_calculator + valuation_pivoter + valuation_filter
    acquisition_pipeline = acquisition_pipeline + prospect_calculator + order_calculator + order_uploader
    return acquisition_pipeline


def main(*args, api, symbols=[], expires=[], criterion={}, discount, fees, **kwargs):
    header = ProspectHeader(name="AcquisitionHeader", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    priority = lambda cols: cols[("apy", Variables.Valuations.Scenario.MINIMUM)]
    criterions = Criterions(SecurityCriterion(**criterion), ValuationCriterion(**criterion))
    with WebReader(delay=10) as source:
        stocks = StockDownloader(name="StockDownloader", source=source)
        pipeline = acquisition(*args, source=source, header=header, priority=priority, criterions=criterions, **kwargs)
        trades = [Querys.Trade(series.to_dict()) for series in stocks(symbols, api=api)]
        thread = RoutineThread(pipeline, name="AcquisitionThread").setup(trades, expires=expires, api=api, discount=discount, fees=fees)
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
    sysCriterion = dict(apy=1.50, cost=1000, size=10)
    main(api=sysAPI, symbols=sysSymbols, expires=sysExpires, criterion=sysCriterion, discount=0.00, fees=0.00)



