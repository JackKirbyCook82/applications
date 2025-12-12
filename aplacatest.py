# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 2025
@name:   Alpaca Trading
@author: Jack Kirby Cook

"""

import os
import sys
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
WEBAPI = os.path.join(RESOURCES, "webapi.txt")

from alpaca.market import AlpacaStockDownloader, AlpacaContractDownloader, AlpacaOptionDownloader
from alpaca.history import AlpacaBarsDownloader
from alpaca.orders import AlpacaOrderUploader
from finance.securities import SecurityCalculator, PricingCalculator
from finance.appraisal import AppraisalCalculator
from finance.technicals import TechnicalCalculator, TechnicalEquation
from finance.implied import ImpliedCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.prospects import ProspectCalculator
from finance.concepts import Concepts, Querys, Strategies
from webscraping.webreaders import WebReader
from support.pipelines import Producer, Processor, Consumer, Carryover
from support.synchronize import RoutineThread
from support.queues import Dequeuer, Queue
from support.concepts import DateRange
from support.filters import Filter
from support.mixins import Delayer

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


Website = Enum("WebSite", "ALPACA ETRADE")
class SymbolDequeuer(Dequeuer, Carryover, Producer, signature="->symbols"): pass
class StockDownloader(AlpacaStockDownloader, Carryover, Processor, signature="(symbols)->stocks"): pass
class ContractDownloader(AlpacaContractDownloader, Carryover, Processor, signature="(symbols)->contracts"): pass
class BarDownloader(AlpacaBarsDownloader, Carryover, Processor, signature="(symbols)->bars"): pass
class OptionDownloader(AlpacaOptionDownloader, Carryover, Processor, signature="(contracts)->options"): pass
class TechnicalCalculator(TechnicalCalculator, Carryover, Processor, signature="(bars)->technicals"): pass
class StockPricing(PricingCalculator, Carryover, Processor, query=Querys.Symbol, signature="(stocks)->stocks"): pass
class OptionPricing(PricingCalculator, Carryover, Processor, query=Querys.Settlement, signature="(options)->options"): pass
class AppraisalCalculator(AppraisalCalculator, Carryover, Processor, signature="(options),{technicals}->options"): pass
class SecurityCalculator(SecurityCalculator, Carryover, Processor, signature="(stocks,options)->securities"): pass
class SecurityFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="(securities)->securities"): pass
class ImpliedCalculator(ImpliedCalculator, Carryover, Processor, signature="(securities)->securities"): pass
class StrategyCalculator(StrategyCalculator, Carryover, Processor, signature="(securities)->strategies"): pass
class ValuationCalculator(ValuationCalculator, Carryover, Processor, signature="(strategies)->valuations"): pass
class ValuationFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="(valuations)->valuations"): pass
class ProspectCalculator(ProspectCalculator, Carryover, Processor, signature="(valuations)->prospects"): pass
class OrderUploader(AlpacaOrderUploader, Carryover, Consumer, signature="(prospects)->"): pass


def main(*args, symbols=[], webapi={}, delayers={}, period, parameters={}, **kwargs):
    symbol_feed = Queue.FIFO(contents=symbols, capacity=None, timeout=None)
    stock_pricing = lambda series: (series["ask"] * series["supply"] + series["bid"] * series["demand"]) / (series["supply"] + series["demand"])
    option_pricing = lambda series: (series["ask"] * series["supply"] + series["bid"] * series["demand"]) / (series["supply"] + series["demand"])
    valuation_criteria = lambda table: value_criteria(table) & cost_criteria(table)
    prospect_liquidity = lambda dataframe: dataframe["size"] * 0.1
    prospect_priority = lambda dataframe: dataframe["npv"]
    security_criteria = lambda table: table["size"] >= + 25
    value_criteria = lambda table: table["npv"] >= + 100
    cost_criteria = lambda table: table["spot"] >= - 500

    with WebReader(delayer=delayers[Website.ALPACA]) as alpaca_source:
        symbols_dequeuer = SymbolDequeuer(name="SymbolDequeuer", feed=symbol_feed)
        stocks_downloader = StockDownloader(name="StockDownloader", source=alpaca_source, webapi=webapi[Website.ALPACA])
        contract_downloader = ContractDownloader(name="ContractDownloader", source=alpaca_source, webapi=webapi[Website.ALPACA])
        bar_downloader = BarDownloader(name="BarDownloader", source=alpaca_source, webapi=webapi[Website.ALPACA])
        options_downloader = OptionDownloader(name="OptionDownloader", source=alpaca_source, webapi=webapi[Website.ALPACA])
        statistics_equation = TechnicalEquation.STATS(period=period)
        technical_calculator = TechnicalCalculator(name="TechnicalCalculator", equations=[statistics_equation])
        stock_pricing = StockPricing(name="StockPricing", pricing=stock_pricing)
        option_pricing = OptionPricing(name="OptionPricing", pricing=option_pricing)
        appraisal_calculator = AppraisalCalculator(name="AppraisalCalculator", appraisals=list(Concepts.Appraisal))
        security_calculator = SecurityCalculator(name="SecurityCalculator")
        security_filter = SecurityFilter(name="SecurityFilter", criteria=security_criteria)
        implied_calculator = ImpliedCalculator(name="ImpliedCalculator")
        strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies))
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        valuation_filter = ValuationFilter(name="ValuationFilter", criteria=valuation_criteria)
        prospects_calculator = ProspectCalculator(name="ProspectCalculator", priority=prospect_priority, liquidity=prospect_liquidity)
        order_uploader = OrderUploader(name="OrderUploader", source=alpaca_source, webapi=webapi[Website.ALPACA])
        alpaca_pipeline = symbols_dequeuer + stocks_downloader + contract_downloader + bar_downloader + options_downloader
        alpaca_pipeline = alpaca_pipeline + technical_calculator + stock_pricing + option_pricing + appraisal_calculator
        alpaca_pipeline = alpaca_pipeline + security_calculator + security_filter + implied_calculator + strategy_calculator
        alpaca_pipeline = alpaca_pipeline + valuation_calculator + valuation_filter + prospects_calculator + order_uploader
        alpaca_thread = RoutineThread(alpaca_pipeline, name="AlpacaThread").setup(**parameters)
        alpaca_thread.start()
        alpaca_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    function = lambda contents: ntuple("WebApi", list(contents.keys()))(*contents.values())
    sysWebApi = pd.read_csv(WEBAPI, sep=" ", header=0, index_col=0, converters={0: lambda website: Website[str(website).upper()]})
    sysWebApi = {website: function(contents) for website, contents in sysWebApi.to_dict("index").items()}
    sysDelayers = {Website.ETRADE: Delayer(3), Website.ALPACA: Delayer(3)}
    with open(TICKERS, "r") as tickerfile:
        sysTickers = list(map(str.strip, tickerfile.read().split("\n")))
        sysSymbols = list(map(Querys.Symbol, sysTickers))
        random.shuffle(sysSymbols)
    sysExpiry = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysHistory = DateRange([(Datetime.today() - Timedelta(weeks=52*2)).date(), (Datetime.today() - Timedelta(days=1)).date()])
    sysParameters = dict(current=Datetime.now().date(), expiry=sysExpiry, history=sysHistory, term=Concepts.Markets.Term.LIMIT, tenure=Concepts.Markets.Tenure.DAY)
    sysParameters.update({"interest": 0.00, "discount": 0.00, "fees": 0.00})
    main(symbols=sysSymbols, webapi=sysWebApi, delayers=sysDelayers, period=252, parameters=sysParameters)



