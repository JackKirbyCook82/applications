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
from alpaca.history import AlpacaBarsDownloader
from alpaca.orders import AlpacaOrderUploader

from finance.market import AcquisitionCalculator, AcquisitionParameters
from finance.securities import StockCalculator, OptionCalculator
from finance.technicals import TechnicalCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.payoff import PayoffCalculator
from finance.variables import Variables, Querys, Strategies
from webscraping.webreaders import WebAuthorizerAPI, WebReader
from support.pipelines import Producer, Processor, Consumer, Carryover
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


class SymbolDequeuer(Dequeuer, Carryover, Producer, signature="->symbol"): pass
class AlpacaBarsDownloader(AlpacaBarsDownloader, Carryover, Processor, signature="symbol->technical"): pass
class AlpacaStockDownloader(AlpacaStockDownloader, Carryover, Processor, signature="symbol->stock"): pass
class AlpacaContractDownloader(AlpacaContractDownloader, Carryover, Processor, signature="symbol->contract"): pass
class AlpacaOptionDownloader(AlpacaOptionDownloader, Carryover, Processor, signature="contract->option"): pass
class TechnicalCalculator(TechnicalCalculator, Carryover, Processor, signature="technical->technical"): pass
class StockCalculator(StockCalculator, Carryover, Processor, signature="stock,technical->stock"): pass
class OptionCalculator(OptionCalculator, Carryover, Processor, signature="option->option"): pass
class OptionFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="option->option"): pass
class StrategyCalculator(StrategyCalculator, Carryover, Processor, signature="stock,option->strategy"): pass
class ValuationCalculator(ValuationCalculator, Carryover, Processor, signature="strategy->valuation"): pass
class ValuationFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="valuation->valuation"): pass
class AcquisitionCalculator(AcquisitionCalculator, Carryover, Processor, signature="valuation,option->acquisition"): pass
class PayoffCalculator(PayoffCalculator, Carryover, Processor, signature="acquisition->acquisition"): pass
class AlpacaOrderUploader(AlpacaOrderUploader, Carryover, Consumer, signature="acquisition->"): pass


class Criterions(ntuple("Criterion", "security valuation")): pass
class SecurityCriterion(Criterion, ABC, fields=["size"]):
    def execute(self, table): return table["size"] >= self["size"]

class ValuationCriterion(Criterion, fields=["npv"]):
    def execute(self, table): return table[("npv", Variables.Valuations.Scenario.MINIMUM)] >= self["npv"]


def acquisition(producer, *args, source, file, criterions, priority, liquidity, **kwargs):
    bars_downloader = AlpacaBarsDownloader(name="BarsDownloader", source=source)
    stock_downloader = AlpacaStockDownloader(name="StockDownloader", source=source)
    contract_downloader = AlpacaContractDownloader(name="ContractDownloader", source=source)
    option_downloader = AlpacaOptionDownloader(name="OptionDownloader", source=source)
    technical_calculator = TechnicalCalculator(name="TechnicalCalculator", technicals=[Variables.Analysis.Technical.STATISTIC])
    stock_calculator = StockCalculator(name="StockCalculator", pricing=Variables.Markets.Pricing.AGGRESSIVE)
    option_calculator = OptionCalculator(name="OptionCalculator", pricing=Variables.Markets.Pricing.AGGRESSIVE)
    option_filter = OptionFilter(name="OptionFilter", criterion=criterions.security)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies))
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    valuation_filter = ValuationFilter(name="ValuationFilter", criterion=criterions.valuation)
    acquisition_calculator = AcquisitionCalculator(name="AcquisitionCalculator", priority=priority, liquidity=liquidity)
    payoff_calculator = PayoffCalculator(name="PayoffCalculator", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    order_uploader = AlpacaOrderUploader(name="OrderUploader", source=source)
    acquisition_pipeline = producer + bars_downloader + stock_downloader + contract_downloader + option_downloader + technical_calculator + stock_calculator + option_calculator + option_filter
    acquisition_pipeline = acquisition_pipeline + strategy_calculator + valuation_calculator + valuation_filter + acquisition_calculator + payoff_calculator + order_uploader
    return acquisition_pipeline


def main(*args, api, symbols=[], dates=[], expiry=[], criterions, parameters={}, **kwargs):
    file = File(repository=REPOSITORY, folder="acquisitions", **dict(AcquisitionParameters))
    symbols = Queue.FIFO(contents=symbols, capacity=None, timeout=None)
    priority = lambda series: series[("npv", Variables.Valuations.Scenario.MINIMUM)]
    liquidity = lambda series: series["size"] * 0.1
    arguments = dict(criterions=criterions, priority=priority, liquidity=liquidity)
    parameters = dict(api=api, dates=dates, expiry=expiry) | dict(parameters)

    with WebReader(delay=3) as source:
        symbol_dequeuer = SymbolDequeuer(name="SymbolDequeuer", feed=symbols)
        acquisition_pipeline = acquisition(symbol_dequeuer, *args, source=source, file=file, **arguments, **kwargs)
        acquisitions_thread = RoutineThread(acquisition_pipeline, name="AcquisitionThread").setup(**parameters)
        acquisitions_thread.start()
        acquisitions_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    with open(TICKERS, "r") as tickerfile:
        sysTickers = list(map(str.strip, tickerfile.read().split("\n")))
        sysSymbols = list(map(Querys.Symbol, sysTickers))
        random.shuffle(sysSymbols)
    with open(API, "r") as apifile:
        sysAPI = WebAuthorizerAPI(*json.loads(apifile.read())["alpaca"])
    sysDates = DateRange([(Datetime.today() - Timedelta(days=1)).date(), (Datetime.today() - Timedelta(weeks=104)).date()])
    sysExpiry = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysCriterions = Criterions(SecurityCriterion(size=10), ValuationCriterion(npv=10))
    sysParameters = dict(period=252, discount=0.00, fees=0.00, term=Variables.Markets.Term.LIMIT, tenure=Variables.Markets.Tenure.DAY, date=Datetime.now().date())
    main(api=sysAPI, symbols=sysSymbols, dates=sysDates, expiry=sysExpiry, criterions=sysCriterions, parameters=sysParameters)



