# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   Trading Platform Acquisitions
@author: Jack Kirby Cook

"""

import os
import sys
import time
import logging
import warnings
import pandas as pd
import xarray as xr
from functools import update_wrapper
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Variables, Contract
from finance.securities import SecurityFilter, SecurityFiles
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationFilter
from finance.holdings import ValuationTables, HoldingFiles
from support.files import Loader, Saver, FileTypes, FileTimings
from support.tables import Reader, Writer
from support.synchronize import RoutineThread, RepeatingThread
from support.filtering import Criterion
from support.pipelines import Routine

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class ContractLoader(Loader, variable=Variables.Querys.CONTRACT, function=Contract.fromstr): pass
class ContractSaver(Saver, variable=Variables.Querys.CONTRACT): pass
class ContractWriter(Writer, variable=Variables.Querys.CONTRACT, function=Contract.fromstr): pass
class ContractReader(Reader, variable=Variables.Querys.CONTRACT): pass


class TradingRoutine(Routine):
    def __new__(cls, *args, capacity=None, **kwargs):
        instance = super().__new__(cls, *args, **kwargs)
        if capacity is not None:
            pursue = instance.capacity(instance.pursue, capacity)
            setattr(instance, "pursue", pursue)
        return instance

    def __init__(self, *args, table, discount, liquidity, **kwargs):
        super().__init__(*args, **kwargs)
        self.__liquidity = liquidity
        self.__discount = discount
        self.__table = table

    def routine(self, *args, **kwargs):
        with self.table.mutex:
            if bool(self.table): print(self.table)
            self.table.change(self.rejected, "status", Variables.Status.REJECTED)
            self.table.change(self.accepted, "status", Variables.Status.ACCEPTED)
            self.table.change(self.abandon, "status", Variables.Status.ABANDONED)
            self.table.change(self.pursue, "status", Variables.Status.PENDING)
            if bool(self.table): print(self.table)

    def pursue(self, table): return (table["status"] == Variables.Status.PROSPECT) & (table["priority"] >= self.discount)
    def abandon(self, table): return (table["status"] == Variables.Status.PROSPECT) & (table["priority"] < self.discount)
    def accepted(self, table): return (table["status"] == Variables.Status.PENDING) & (table["size"] >= self.liquidity)
    def rejected(self, table): return (table["status"] == Variables.Status.PENDING) & (table["size"] < self.liquidity)

    @staticmethod
    def capacity(method, capacity):
        assert capacity is not None
        def wrapper(table): return (method(table).cumsum() < capacity + 1) & method(table)
        update_wrapper(wrapper, method)
        return wrapper

    @property
    def liquidity(self): return self.__liquidity
    @property
    def discount(self): return self.__discount
    @property
    def table(self): return self.__table


def market(*args, loading, tables, directory, parameters={}, criterion={}, functions={}, **kwargs):
    security_loader = ContractLoader(name="MarketSecurityLoader", files=loading, directory=directory)
    security_filter = SecurityFilter(name="MarketSecurityFilter", criterion=criterion["security"])
    strategy_calculator = StrategyCalculator(name="MarketStrategyCalculator", **functions)
    valuation_calculator = ValuationCalculator(name="MarketValuationCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    valuation_filter = ValuationFilter(name="MarketValuationFilter", valuation=Variables.Valuations.ARBITRAGE, criterion=criterion["valuation"])
    acquisition_writer = ContractWriter(name="MarketAcquisitionWriter", tables=tables)
    market_pipeline = security_loader + security_filter + strategy_calculator + valuation_calculator + valuation_filter + acquisition_writer
    market_thread = RoutineThread(market_pipeline, name="MarketThread")
    market_thread.setup(**parameters)
    return market_thread

def acquisition(*args, tables, saving, parameters={}, **kwargs):
    acquisition_reader = ContractReader(name="AcquisitionReader", tables=tables)
    acquisition_saver = ContractSaver(name="AcquisitionSaver", files=saving)
    acquisition_pipeline = acquisition_reader + acquisition_saver
    acquisition_thread = RepeatingThread(acquisition_pipeline, name="AcquisitionThread", wait=10)
    acquisition_thread.setup(**parameters)
    return acquisition_thread

def trading(*args, table, capacity, discount, liquidity, parameters={}, **kwargs):
    trading_routine = TradingRoutine(name="TradingRoutine", table=table, capacity=capacity, discount=discount, liquidity=liquidity)
    trading_thread = RepeatingThread(trading_routine, name="TradingThread", wait=10)
    trading_thread.setup(**parameters)
    return trading_thread


def main(*args, arguments, parameters, **kwargs):
    option_file = SecurityFiles.Option(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    acquisition_table = ValuationTables.Valuation(name="AcquisitionTable", valuation=Variables.Valuations.ARBITRAGE)

    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    security_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    priority_function = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    criterion = dict(valuation=valuation_criterion, security=security_criterion)
    functions = dict(priority=priority_function)

    market_parameters = dict(loading={option_file: "r"}, tables=[acquisition_table], directory=option_file, criterion=criterion, functions=functions, parameters=parameters)
    acquisition_parameters = dict(saving={holdings_file: "a"}, tables=[acquisition_table], criterion=criterion, functions=functions, parameters=parameters)
    trading_parameters = dict(table=acquisition_table, capacity=arguments["capacity"], discount=arguments["discount"], liquidity=arguments["liquidity"], parameters=parameters)
    market_thread = market(*args, **market_parameters, **kwargs)
    acquisition_thread = acquisition(*args, **acquisition_parameters, **kwargs)
    trading_thread = trading(*args, **trading_parameters, **kwargs)

    terminate = lambda: not bool(market_thread) and not bool(acquisition_table)
    trading_thread.start()
    acquisition_thread.start()
    market_thread.start()
    while not terminate(): time.sleep(15)
    trading_thread.cease()
    acquisition_thread.cease()
    market_thread.join()
    acquisition_thread.join()
    trading_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysCurrent = Datetime(year=2024, month=7, day=18)
    sysArguments = dict(apy=0.50, size=10, volume=100, interest=100, discount=0.75, liquidity=25, capacity=None)
    sysParameters = dict(current=sysCurrent, discount=0.0, fees=0.0)
    main(arguments=sysArguments, parameters=sysParameters)




