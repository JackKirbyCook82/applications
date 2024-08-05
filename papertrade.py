# -*- coding: utf-8 -*-
"""
Created on Fri Jul 19 2024
@name:   Trading Platform PaperTrading
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import numpy as np
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from etrade.window import PaperTradeApplication
from finance.variables import Variables, Contract
from finance.securities import SecurityFilter, SecurityFiles
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationFilter
from finance.holdings import HoldingWriter, HoldingTable
from support.synchronize import MainThread, SideThread
from support.files import Loader, Saver, FileTypes, FileTimings
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


formatter = lambda self, *, query, elapsed, **kw: f"{str(self.title)}: {repr(self)}|{str(query[Variables.Querys.CONTRACT])}[{elapsed:.02f}s]"
class ContractLoader(Loader, query=Variables.Querys.CONTRACT, create=Contract.fromstr, formatter=formatter): pass
class ContractSaver(Saver, query=Variables.Querys.CONTRACT, formatter=formatter): pass


def market(*args, directory, loading, table, parameters={}, criterion={}, functions={}, **kwargs):
    security_loader = ContractLoader(name="MarketSecurityLoader", source=loading, directory=directory, wait=10)
    security_filter = SecurityFilter(name="MarketSecurityFilter", criterion=criterion["security"])
    strategy_calculator = StrategyCalculator(name="MarketStrategyCalculator", **functions)
    valuation_calculator = ValuationCalculator(name="MarketValuationCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    valuation_filter = ValuationFilter(name="MarketValuationFilter", criterion=criterion["valuation"])
    acquisition_writer = HoldingWriter(name="MarketAcquisitionWriter", destination=table, valuation=Variables.Valuations.ARBITRAGE, **functions)
    market_pipeline = security_loader + security_filter + strategy_calculator + valuation_calculator + valuation_filter + acquisition_writer
    market_thread = SideThread(market_pipeline, name="MarketThread")
    market_thread.setup(**parameters)
    return market_thread


def papertrade(*args, acquisitions, divestitures, parameters={}, **kwargs):
    papertrade_application = PaperTradeApplication(name="PaperTradeApplication", acquisitions=acquisitions, divestitures=divestitures, wait=30)
    papertrade_thread = MainThread(papertrade_application, name="PaperTradeThread")
    papertrade_thread.setup(**parameters)
    return papertrade_thread


def main(*args, arguments, parameters, **kwargs):
    option_file = SecurityFiles.Option(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    acquisition_table = HoldingTable(name="AcquisitionTable")
    divestiture_table = HoldingTable(name="DivestitureTable")
    security_criterion = {Criterion.FLOOR: {"size": 10, "volume": 100, "interest": 100}, Criterion.NULL: ["size", "volume", "interest"]}
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.00035, "size": 10}, Criterion.NULL: ["apy", "size"]}
    liquidity_function = lambda cols: np.floor(cols["size"] * 0.1).astype(np.int32)
    priority_function = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    criterion = dict(valuation=valuation_criterion, security=security_criterion)
    functions = dict(liquidity=liquidity_function, priority=priority_function)
    market_parameters = dict(directory=option_file, loading={option_file: "r"}, table=acquisition_table, criterion=criterion, functions=functions, parameters=parameters)
    papertrade_parameters = dict(acquisitions=acquisition_table, divestitures=divestiture_table, functions=functions, parameters=parameters)
    market_thread = market(*args, **market_parameters, **kwargs)
    papertrade_thread = papertrade(*args, **papertrade_parameters, **kwargs)
    market_thread.start()
    papertrade_thread.run()
    market_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    current = Datetime(year=2024, month=7, day=18)
    sysArguments = dict(apy=0.035, size=10)
    sysParameters = dict(current=current, discount=0.0, fees=0.0)
    main(arguments=sysArguments, parameters=sysParameters)

