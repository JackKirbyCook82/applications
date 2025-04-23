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
from abc import ABC, abstractmethod
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

from etrade.market import ETradeStockDownloader, ETradeExpireDownloader, ETradeOptionDownloader
from alpaca.market import AlpacaStockDownloader, AlpacaOptionDownloader, AlpacaContractDownloader
from alpaca.history import AlpacaBarsDownloader
from finance.market import AcquisitionCalculator, AcquisitionSaver, AcquisitionParameters
from finance.securities import StockCalculator, OptionCalculator
from finance.technicals import TechnicalCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.payoff import PayoffCalculator
from finance.variables import Variables, Querys, Strategies
from webscraping.webreaders import WebAuthorizerAPI, WebReader, WebAuthorizer
from support.pipelines import Producer, Processor, Consumer, Carryover
from support.synchronize import RoutineThread
from support.filters import Filter, Criterion
from support.queues import Dequeuer, Queue
from support.variables import DateRange
from support.meta import RegistryMeta
from support.files import File


__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


Website = Enum("WebSite", "ALPACA ETRADE")
Criterions = ntuple("Criterions", "security valuation")
authorize = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
request = "https://api.etrade.com/oauth/request_token"
access = "https://api.etrade.com/oauth/access_token"
base = "https://api.etrade.com"


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
class OptionCalculator(OptionCalculator, Carryover, Processor, signature="option->option"): pass
class OptionFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="option->option"): pass
class StrategyCalculator(StrategyCalculator, Carryover, Processor, signature="stock,option->strategy"): pass
class ValuationCalculator(ValuationCalculator, Carryover, Processor, signature="strategy->valuation"): pass
class ValuationFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="valuation->valuation"): pass
class AcquisitionCalculator(AcquisitionCalculator, Carryover, Processor, signature="valuation,option->acquisition"): pass
class PayoffCalculator(PayoffCalculator, Carryover, Processor, signature="acquisition->acquisition"): pass
class AcquisitionSaver(AcquisitionSaver, Carryover, Consumer, signature="acquisition->"): pass


class Acquisition(ABC, metaclass=RegistryMeta):
    def __init__(self, *args, criterions, priority, liquidity, **kwargs):
        with open(API, "r") as apifile:
            api = json.loads(apifile.read()).items()
            api = {Website[str(website).upper()]: WebAuthorizerAPI(*values) for website, values in api}
        authorizer = WebAuthorizer(api=api[Website.ETRADE], base=base, access=access, request=request, authorize=authorize)
        etrade = WebReader(delay=5, authorizer=authorizer)
        alpaca = WebReader(delay=3, authorizer=None)
        sources = {Website.ETRADE: etrade, Website.ALPACA: alpaca}
        self.__criterions = criterions
        self.__liquidity = liquidity
        self.__priority = priority
        self.__sources = sources
        self.__api = api

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, error_type, error_value, error_traceback):
        self.stop()

    def __call__(self, *args, feed, file, **kwargs):
        symbols_dequeuer = SymbolDequeuer(name="SymbolDequeuer", feed=feed)
        bars_downloader = AlpacaBarsDownloader(name="BarsDownloader", source=self.sources[Website.ALPACA], api=self.api[Website.ALPACA])
        acquisition_saver = AcquisitionSaver(name="AcquisitionSaver", file=file, mode="a")
        producer = symbols_dequeuer + bars_downloader
        producer = self.downloader(producer, *args, **kwargs)
        producer = self.calculator(producer, *args, **kwargs)
        return producer + acquisition_saver

    def calculator(self, producer, *args, **kwargs):
        technicals_calculator = TechnicalCalculator(name="TechnicalCalculator", technicals=[Variables.Analysis.Technical.STATISTIC])
        stocks_calculator = StockCalculator(name="StockCalculator", pricing=Variables.Markets.Pricing.AGGRESSIVE)
        options_calculator = OptionCalculator(name="OptionCalculator", pricing=Variables.Markets.Pricing.AGGRESSIVE)
        option_filter = OptionFilter(name="OptionFilter", criterion=self.criterions.security)
        strategies_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies))
        valuations_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.Valuation.ARBITRAGE)
        valuation_filter = ValuationFilter(name="ValuationFilter", criterion=self.criterions.valuation)
        acquisitions_calculator = AcquisitionCalculator(name="AcquisitionCalculator", priority=self.priority, liquidity=self.liquidity)
        payoffs_calculator = PayoffCalculator(name="PayoffCalculator", valuation=Variables.Valuations.Valuation.ARBITRAGE)
        pipeline = producer + technicals_calculator + stocks_calculator + options_calculator + option_filter
        pipeline = pipeline + strategies_calculator + valuations_calculator + valuation_filter
        return pipeline + acquisitions_calculator + payoffs_calculator

    @abstractmethod
    def downloader(self, producer, *args, **kwargs): pass
    @abstractmethod
    def start(self, *args, **kwargs): pass
    @abstractmethod
    def stop(self, *args, **kwargs): pass

    @property
    def criterions(self): return self.__criterions
    @property
    def liquidity(self): return self.__liquidity
    @property
    def priority(self): return self.__priority
    @property
    def sources(self): return self.__sources
    @property
    def api(self): return self.__api


class ETradeAcquisition(Acquisition, register=Website.ETRADE):
    def downloader(self, producer, *args, **kwargs):
        parameters = dict(source=self.sources[Website.ETRADE])
        stocks_downloader = ETradeStockDownloader(name="StockDownloader", **parameters)
        expires_downloader = ETradeExpireDownloader(name="ExpireDownloader", **parameters)
        options_downloader = ETradeOptionDownloader(name="OptionDownloader", **parameters)
        return producer + stocks_downloader + expires_downloader + options_downloader

    def start(self):
        self.sources[Website.ALPACA].start()
        self.sources[Website.ETRADE].start()

    def stop(self):
        self.sources[Website.ALPACA].stop()
        self.sources[Website.ETRADE].stop()


class AlpacaAcquisition(Acquisition, register=Website.ALPACA):
    def downloader(self, producer, *args, **kwargs):
        parameters = dict(source=self.sources[Website.ALPACA], api=self.api[Website.ALPACA])
        stocks_downloader = AlpacaStockDownloader(name="StockDownloader", **parameters)
        contracts_downloader = AlpacaContractDownloader(name="ContractDownloader", **parameters)
        options_downloader = AlpacaOptionDownloader(name="OptionDownloader", **parameters)
        return producer + stocks_downloader + contracts_downloader + options_downloader

    def start(self):
        self.sources[Website.ALPACA].start()

    def stop(self):
        self.sources[Website.ALPACA].stop()


class SecurityCriterion(Criterion, ABC, fields=["size"]):
    def execute(self, table): return table["size"] >= self["size"]

class ValuationCriterion(Criterion, fields=["npv"]):
    def execute(self, table): return table[("npv", Variables.Valuations.Scenario.MINIMUM)] >= self["npv"]


def main(*args, website, symbols=[], parameters={}, criterions, **kwargs):
    file = File(repository=REPOSITORY, folder="acquisitions", **dict(AcquisitionParameters))
    feed = Queue.FIFO(contents=symbols, capacity=None, timeout=None)
    priority = lambda series: series[("npv", Variables.Valuations.Scenario.MINIMUM)]
    liquidity = lambda series: series["size"] * 0.1
    with Acquisition[website](criterions=criterions, priority=priority, liquidity=liquidity) as acquisition:
        pipeline = acquisition(feed=feed, file=file)
        thread = RoutineThread(pipeline, name="AcquisitionThread").setup(**parameters)
        thread.start()
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
        random.shuffle(sysSymbols)
    sysDates = DateRange([(Datetime.today() - Timedelta(days=1)).date(), (Datetime.today() - Timedelta(weeks=104)).date()])
    sysExpiry = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysCriterions = Criterions(SecurityCriterion(size=10), ValuationCriterion(npv=10))
    sysParameters = dict(date=Datetime.now().date(), dates=sysDates, expiry=sysExpiry, term=Variables.Markets.Term.LIMIT, tenure=Variables.Markets.Tenure.DAY)
    sysParameters.update({"period": 252, "discount": 0.00, "fees": 0.00})
    main(website=Website.ALPACA, symbols=sysSymbols, criterions=sysCriterions, parameters=sysParameters)



