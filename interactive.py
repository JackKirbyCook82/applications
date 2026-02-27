# -*- coding: utf-8 -*-
"""
Created on Thurs Feb 26 2026
@name:   Interactive Broker Trading
@author: Jack Kirby Cook

"""

import os
import sys
import random
import logging
import warnings
import pandas as pd
from enum import Enum
from types import SimpleNamespace
from attr.converters import to_bool
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from collections import namedtuple as ntuple

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
REPOSITORY = os.path.join(ROOT, "repository")
RESOURCES = os.path.join(ROOT, "resources")
if ROOT not in sys.path: sys.path.append(ROOT)
ACCOUNTS = os.path.join(RESOURCES, "accounts.txt")
TICKERS = os.path.join(RESOURCES, "tickers.txt")

from interactive.market import InteractiveStockDownloader, InteractiveContractDownloader, InteractiveOptionDownloader
from interactive.orders import InteractiveOrderUploader
from interactive.source import InteractiveSource
from finance.securities import SecurityCalculator, PricingCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.prospects import ProspectCalculator
from webscraping.websources import WebDelayer
from support.pipelines import Producer, Processor, Consumer, Carryover
from support.synchronize import RoutineThread
from support.queues import Dequeuer, Queue
from support.concepts import DateRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


class SymbolDequeuer(Dequeuer, Carryover, Producer, signature="->symbols"): pass
class StockDownloader(InteractiveStockDownloader, Carryover, Processor, signature="(symbols)->stocks"): pass
class ContractDownloader(InteractiveContractDownloader, Carryover, Processor, signature="(symbols)->contracts"): pass
class OptionDownloader(InteractiveOptionDownloader, Carryover, Processor, signature="(contracts)->options"): pass
class StockPricing(PricingCalculator, Carryover, Processor, query=Querys.Symbol, signature="(stocks)->stocks"): pass
class OptionPricing(PricingCalculator, Carryover, Processor, query=Querys.Settlement, signature="(options)->options"): pass
class SecurityCalculator(SecurityCalculator, Carryover, Processor, signature="(stocks,options)->securities"): pass
class SecurityFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="(securities)->securities"): pass
class StrategyCalculator(StrategyCalculator, Carryover, Processor, signature="(securities)->strategies"): pass
class ValuationCalculator(ValuationCalculator, Carryover, Processor, signature="(strategies)->valuations"): pass
class ValuationFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="(valuations)->valuations"): pass
class ProspectCalculator(ProspectCalculator, Carryover, Processor, signature="(valuations)->prospects"): pass
class OrderUploader(InteractiveOrderUploader, Carryover, Consumer, signature="(prospects)->"): pass


def main(*args, symbols, account, delayer, parameters={}, **kwargs):
    symbol_feed = Queue.FIFO(contents=symbols, capacity=None, timeout=None)
    stock_pricing = lambda series: (series["ask"] * series["supply"] + series["bid"] * series["demand"]) / (series["supply"] + series["demand"])
    option_pricing = lambda series: (series["ask"] * series["supply"] + series["bid"] * series["demand"]) / (series["supply"] + series["demand"])
    valuation_criteria = lambda table: value_criteria(table) & cost_criteria(table)
    prospect_liquidity = lambda dataframe: dataframe["size"] * 0.1
    prospect_priority = lambda dataframe: dataframe["npv"]
    security_criteria = lambda table: table["size"] >= + 10
    value_criteria = lambda table: table["npv"] >= + 10
    cost_criteria = lambda table: table["spot"] >= - 1000

    with InteractiveSource(host="localhost", port=7497, account=account, delayer=delayer) as source:
        symbols_dequeuer = SymbolDequeuer(name="SymbolDequeuer", feed=symbol_feed)
        stocks_downloader = StockDownloader(name="StockDownloader", source=source)
        contract_downloader = ContractDownloader(name="ContractDownloader", source=source)
        options_downloader = OptionDownloader(name="OptionDownloader", source=source)
        stock_pricing = StockPricing(name="StockPricing", pricing=stock_pricing)
        option_pricing = OptionPricing(name="OptionPricing", pricing=option_pricing)
        security_calculator = SecurityCalculator(name="SecurityCalculator")
        security_filter = SecurityFilter(name="SecurityFilter", criteria=security_criteria)
        strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies))
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        valuation_filter = ValuationFilter(name="ValuationFilter", criteria=valuation_criteria)
        prospects_calculator = ProspectCalculator(name="ProspectCalculator", priority=prospect_priority, liquidity=prospect_liquidity)
        order_uploader = OrderUploader(name="OrderUploader", source=source)
        interactive_pipeline = symbols_dequeuer + stocks_downloader + contract_downloader + options_downloader
        interactive_pipeline = interactive_pipeline + stock_pricing + option_pricing + security_calculator + security_filter
        interactive_pipeline = interactive_pipeline + strategy_calculator + valuation_calculator + valuation_filter + prospects_calculator + order_uploader
        interactive_thread = RoutineThread(interactive_pipeline, name="InteractiveThread").setup(**parameters)
        interactive_thread.start()
        interactive_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    sysAccounts = SimpleNamespace(**pd.read_csv(ACCOUNTS, sep=r"\s+", converters={"live": to_bool}).set_index(["website", "live"], inplace=False).loc[("etrade", False)].to_dict())
    sysSymbols = list(map(Querys.Symbol, open(TICKERS, "r").read().splitlines()))
    random.shuffle(sysSymbols)
    sysDelayer = WebDelayer(3)
    sysExpiry = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52*2)).date()])
    sysParameters = dict(current=Datetime.now().date(), expiry=sysExpiry, term=Concepts.Markets.Term.LIMIT, tenure=Concepts.Markets.Tenure.DAY)
    sysParameters.update({"interest": 0.05, "discount": 0.05, "fees": 1.00})
    main(symbols=sysSymbols, accounts=sysAccounts, delayer=sysDelayer, parameters=sysParameters)



