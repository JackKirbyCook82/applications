# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 2025
@name:   ETrade Trading
@author: Jack Kirby Cook

"""

import os
import sys
import random
import logging
import warnings
import pandas as pd
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
AUTHENTICATORS = os.path.join(RESOURCES, "authenticators.txt")
ACCOUNTS = os.path.join(RESOURCES, "accounts.txt")
TICKERS = os.path.join(RESOURCES, "tickers.txt")

from etrade.market import ETradeStockDownloader, ETradeExpireDownloader, ETradeOptionDownloader
from etrade.orders import ETradeOrderUploader
from etrade.service import ETradePromptService
from finance.securities import SecurityCalculator, PricingCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.prospects import ProspectCalculator
from finance.concepts import Concepts, Querys, Strategies
from webscraping.webreaders import WebReader
from webscraping.websupport import WebDelayer
from support.pipelines import Producer, Processor, Consumer, Carryover
from support.synchronize import RoutineThread
from support.queues import Dequeuer, Queue
from support.concepts import DateRange
from support.filters import Filter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


class SymbolDequeuer(Dequeuer, Carryover, Producer, signature="->symbols"): pass
class StockDownloader(ETradeStockDownloader, Carryover, Processor, signature="(symbols)->stocks"): pass
class ExpireDownloader(ETradeExpireDownloader, Carryover, Processor, signature="(symbols)->expires"): pass
class OptionDownloader(ETradeOptionDownloader, Carryover, Processor, signature="(symbols,expires)->options"): pass
class StockPricing(PricingCalculator, Carryover, Processor, query=Querys.Symbol, signature="(stocks)->stocks"): pass
class OptionPricing(PricingCalculator, Carryover, Processor, query=Querys.Settlement, signature="(options)->options"): pass
class SecurityCalculator(SecurityCalculator, Carryover, Processor, signature="(stocks,options)->securities"): pass
class SecurityFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="(securities)->securities"): pass
class StrategyCalculator(StrategyCalculator, Carryover, Processor, signature="(securities)->strategies"): pass
class ValuationCalculator(ValuationCalculator, Carryover, Processor, signature="(strategies)->valuations"): pass
class ValuationFilter(Filter, Carryover, Processor, query=Querys.Settlement, signature="(valuations)->valuations"): pass
class ProspectCalculator(ProspectCalculator, Carryover, Processor, signature="(valuations)->prospects"): pass
class OrderUploader(ETradeOrderUploader, Carryover, Consumer, signature="(prospects)->"): pass


def main(*args, symbols, account, authenticator, delayer, parameters={}, **kwargs):
    symbol_feed = Queue.FIFO(contents=symbols, capacity=None, timeout=None)
    stock_pricing = lambda series: (series["ask"] * series["supply"] + series["bid"] * series["demand"]) / (series["supply"] + series["demand"])
    option_pricing = lambda series: (series["ask"] * series["supply"] + series["bid"] * series["demand"]) / (series["supply"] + series["demand"])
    valuation_criteria = lambda table: value_criteria(table) & cost_criteria(table)
    prospect_liquidity = lambda dataframe: dataframe["size"] * 0.1
    prospect_priority = lambda dataframe: dataframe["npv"]
    security_criteria = lambda table: table["size"] >= + 10
    value_criteria = lambda table: table["npv"] >= + 10
    cost_criteria = lambda table: table["spot"] >= - 1000

    with WebReader(service=ETradePromptService(), account=account, authenticator=authenticator, delayer=delayer) as source:
        symbols_dequeuer = SymbolDequeuer(name="SymbolDequeuer", feed=symbol_feed)
        stocks_downloader = StockDownloader(name="StockDownloader", source=source)
        expires_downloader = ExpireDownloader(name="ExpireDownloader", source=source)
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
        etrade_pipeline = symbols_dequeuer + stocks_downloader + expires_downloader + options_downloader
        etrade_pipeline = etrade_pipeline + stock_pricing + option_pricing + security_calculator + security_filter
        etrade_pipeline = etrade_pipeline + strategy_calculator + valuation_calculator + valuation_filter + prospects_calculator + order_uploader
        etrade_thread = RoutineThread(etrade_pipeline, name="ETradeThread").setup(**parameters)
        etrade_thread.start()
        etrade_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    sysAuthenticators = SimpleNamespace(**pd.read_csv(AUTHENTICATORS, sep=r"\s+", converters={"live": to_bool}).set_index(["website", "live"], inplace=False).loc[("etrade", False)].to_dict())
    sysAccounts = SimpleNamespace(**pd.read_csv(ACCOUNTS, sep=r"\s+", converters={"live": to_bool}).set_index(["website", "live"], inplace=False).loc[("etrade", False)].to_dict())
    sysSymbols = list(map(Querys.Symbol, open(TICKERS, "r").read().splitlines()))
    random.shuffle(sysSymbols)
    sysDelayer = WebDelayer(3)
    sysExpiry = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52*2)).date()])
    sysParameters = dict(current=Datetime.now().date(), expiry=sysExpiry, term=Concepts.Markets.Term.LIMIT, tenure=Concepts.Markets.Tenure.DAY)
    sysParameters.update({"interest": 0.05, "discount": 0.05, "fees": 1.00})
    main(symbols=sysSymbols, account=sysAccount, authenticator=sysAuthenticator, delayer=sysDelayer, parameters=sysParameters)



