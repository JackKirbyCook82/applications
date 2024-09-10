# -*- coding: utf-8 -*-
"""
Created on Weds Jun 13 2024
@name:   Trading Platform Divestitures
@author: Jack Kirby Cook

"""

import os
import sys
import time
import logging
import warnings
import numpy as np
import pandas as pd
import xarray as xr
from functools import update_wrapper
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
HISTORY = os.path.join(ROOT, "repository", "history")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Variables, Contract
from finance.technicals import TechnicalFiles
from finance.securities import SecurityCalculator, SecurityFilter
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationFilter
from finance.holdings import ValuationTables, HoldingFiles
from finance.exposures import ExposureCalculator, ExposureTables
from finance.allocation import AllocationCalculator
from finance.stability import StabilityCalculator
from support.files import Loader, Saver, FileTypes, FileTimings
from support.tables import Reader, Writer
from support.synchronize import RepeatingThread
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


def portfolio(*args, loading, tables, directory, parameters={}, criterion={}, functions={}, **kwargs):
    holding_loader = ContractLoader(name="PortfolioHoldingLoader", files=loading, directory=directory)
    exposure_calculator = ExposureCalculator(name="PortfolioExposureCalculator", **functions)
    security_calculator = SecurityCalculator(name="PortfolioSecurityCalculator", **functions)
    security_filter = SecurityFilter(name="PortfolioSecurityFilter", criterion=criterion["security"])
    strategy_calculator = StrategyCalculator(name="PortfolioStrategyCalculator", **functions)
    valuation_calculator = ValuationCalculator(name="PortfolioValuationCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", valuation=Variables.Valuations.ARBITRAGE, criterion=criterion["valuation"])
    allocation_calculator = AllocationCalculator(name="PortfolioAllocationCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    stability_calculator = StabilityCalculator(name="PortfolioStabilityCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    divestiture_writer = ContractWriter(name="PortfolioDivestitureWriter", tables=tables)
    portfolio_pipeline = holding_loader + exposure_calculator + security_calculator + security_filter + strategy_calculator + valuation_calculator + valuation_filter + allocation_calculator + stability_calculator + divestiture_writer
    portfolio_thread = RepeatingThread(portfolio_pipeline, name="PortfolioThread", wait=10)
    portfolio_thread.setup(**parameters)
    return portfolio_thread


def divestiture(*args, tables, saving, parameters={}, **kwargs):
    divestiture_reader = ContractReader(name="DivestitureReader", tables=tables)
    divestiture_saver = ContractSaver(name="DivestitureSaver", files=saving)
    divestiture_pipeline = divestiture_reader + divestiture_saver
    divestiture_thread = RepeatingThread(divestiture_pipeline, name="DivestitureThread", wait=10)
    divestiture_thread.setup(**parameters)
    return divestiture_thread

def trading(*args, table, capacity, discount, liquidity, parameters={}, **kwargs):
    trading_routine = TradingRoutine(name="TradingRoutine", table=table, capacity=capacity, discount=discount, liquidity=liquidity)
    trading_thread = RepeatingThread(trading_routine, name="TradingThread", wait=10)
    trading_thread.setup(**parameters)
    return trading_thread


def main(*args, logger, arguments, parameters, **kwargs):
    statistic_file = TechnicalFiles.Statistic(name="StatisticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    divestiture_table = ValuationTables.Valuation(name="DivestitureTable", valuation=Variables.Valuations.ARBITRAGE)
    exposure_table = ExposureTables.Exposure(name="ExposureTable")

    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    security_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    priority_function = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    functions = dict(priority=priority_function, size=lambda cols: np.random.randint(10, 50), volume=lambda cols: np.random.randint(100, 150), interest=lambda cols: np.random.randint(100, 150))
    criterion = dict(valuation=valuation_criterion, security=security_criterion)
    tables = dict(divestiture=divestiture_table, exposure=exposure_table)

    portfolio_parameters = dict(loading={holdings_file: "r", statistic_file: "r"}, tables=[divestiture_table], directory=holdings_file, criterion=criterion, functions=functions, parameters=parameters)
    divestiture_parameters = dict(saving={holdings_file: "a"}, tables=[divestiture_table], criterion=criterion, functions=functions, parameters=parameters)
    trading_parameters = dict(table=divestiture_table, capacity=arguments["capacity"], discount=arguments["discount"], liquidity=arguments["liquidity"], parameters=parameters)
    portfolio_thread = portfolio(*args, **portfolio_parameters, **kwargs)
    divestiture_thread = divestiture(*args, **divestiture_parameters, **kwargs)
    trading_thread = trading(*args, **trading_parameters, **kwargs)

    terminate = lambda: not bool(portfolio_parameters) and not bool(divestiture_table) and not bool(exposure_table)
    trading_thread.start()
    divestiture_thread.start()
    portfolio_thread.start()
    while not terminate(): time.sleep(15)
    trading_thread.cease()
    divestiture_thread.cease()
    portfolio_thread.cease()
    portfolio_thread.join()
    divestiture_thread.join()
    trading_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysLogger = logging.getLogger(__name__)
    sysFactor = lambda Θ, Φ, ε, ω: 1 + (Φ * ε * ω)
    sysCurrent = Datetime(year=2024, month=7, day=18)
    sysArguments = dict(apy=0, size=10, volume=100, interest=100, discount=0.00, liquidity=25, capacity=1)
    sysParameters = dict(factor=sysFactor, current=sysCurrent, discount=0.00, fees=0.00, divergence=0.05)
    main(logger=sysLogger, arguments=sysArguments, parameters=sysParameters)




