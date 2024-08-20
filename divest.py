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
from finance.holdings import HoldingWriter, HoldingReader, HoldingTable, HoldingFiles
from finance.exposures import ExposureCalculator
from finance.allocation import AllocationCalculator
from finance.stability import StabilityCalculator
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import CycleThread
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class ContractLoader(Loader, variable=Variables.Querys.CONTRACT, create=Contract.fromstr): pass
class ContractSaver(Saver, variable=Variables.Querys.CONTRACT): pass


def portfolio(*args, directory, loading, table, parameters={}, criterion={}, functions={}, **kwargs):
    holding_loader = ContractLoader(name="PortfolioHoldingLoader", source=loading, directory=directory)
    exposure_calculator = ExposureCalculator(name="PortfolioExposureCalculator", **functions)
    security_calculator = SecurityCalculator(name="PortfolioSecurityCalculator", **functions)
    security_filter = SecurityFilter(name="PortfolioSecurityFilter", criterion=criterion["security"])
    strategy_calculator = StrategyCalculator(name="PortfolioStrategyCalculator", **functions)
    valuation_calculator = ValuationCalculator(name="PortfolioValuationCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", valuation=Variables.Valuations.ARBITRAGE, criterion=criterion["valuation"])
    allocation_calculator = AllocationCalculator(name="PortfolioAllocationCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    stability_calculator = StabilityCalculator(name="PortfolioStabilityCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    divestiture_writer = HoldingWriter(name="PortfolioDivestitureWriter", destination=table, valuation=Variables.Valuations.ARBITRAGE, **functions)
    portfolio_pipeline = holding_loader + exposure_calculator + security_calculator + security_filter + strategy_calculator + valuation_calculator + valuation_filter + allocation_calculator + stability_calculator + divestiture_writer
    portfolio_thread = CycleThread(portfolio_pipeline, name="PortfolioValuationThread", wait=10)
    portfolio_thread.setup(**parameters)
    return portfolio_thread


def divestiture(*args, table, saving, parameters={}, **kwargs):
    divestiture_reader = HoldingReader(name="PortfolioDivestitureReader", source=table, valuation=Variables.Valuations.ARBITRAGE)
    divestiture_saver = ContractSaver(name="PortfolioDivestitureSaver", destination=saving)
    divestiture_pipeline = divestiture_reader + divestiture_saver
    divestiture_thread = CycleThread(divestiture_pipeline, name="PortfolioDivestitureThread", wait=10)
    divestiture_thread.setup(**parameters)
    return divestiture_thread


def main(*args, arguments, parameters, **kwargs):
    statistic_file = TechnicalFiles.Statistic(name="StatisticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    divestiture_table = HoldingTable(name="DivestitureTable")

    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    security_criterion = {Criterion.FLOOR: {"size": arguments["size"]}}
    priority_function = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    functions = dict(priority=priority_function, size=lambda cols: np.int64(100), volume=lambda cols: np.NaN, interest=lambda cols: np.NaN)
    criterion = dict(valuation=valuation_criterion, security=security_criterion)

    portfolio_parameters = dict(directory=holdings_file, loading={holdings_file: "r", statistic_file: "r"}, table=divestiture_table, criterion=criterion, functions=functions, parameters=parameters)
    divestiture_parameters = dict(table=divestiture_table, saving={holdings_file: "a"}, criterion=criterion, functions=functions, parameters=parameters)
    portfolio_thread = portfolio(*args, **portfolio_parameters, **kwargs)
    divestiture_thread = divestiture(*args, **divestiture_parameters, **kwargs)

    wrapper_function = lambda function: lambda dataframe: (function(dataframe).cumsum() < arguments["capacity"] + 1) & function(dataframe)
    abandon_function = lambda dataframe: (dataframe["priority"] < arguments["pursue"])
    reject_function = lambda dataframe: (dataframe["priority"] >= arguments["pursue"]) & (dataframe["size"] < arguments["accept"])
    accept_function = lambda dataframe: (dataframe["priority"] >= arguments["pursue"]) & (dataframe["size"] >= arguments["accept"])
    abandon_function = wrapper_function(abandon_function)
    reject_function = wrapper_function(reject_function)
    accept_function = wrapper_function(accept_function)

    divestiture_thread.start()
    portfolio_thread.start()
    while True:
        if bool(divestiture_table):
            print(divestiture_table)
        time.sleep(10)
        divestiture_table.change(abandon_function, "status", Variables.Status.ABANDONED)
        divestiture_table.change(reject_function, "status", Variables.Status.REJECTED)
        divestiture_table.change(accept_function, "status", Variables.Status.ACCEPTED)
    portfolio_thread.cease()
    divestiture_thread.cease()
    portfolio_thread.join()
    divestiture_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysFactor = lambda Θ, Φ, ε: 1 + (Φ * ε)
    sysCurrent = Datetime(year=2024, month=7, day=18)
    sysArguments = dict(apy=0, size=10, pursue=0.25, accept=20, capacity=1)
    sysParameters = dict(factor=sysFactor, current=sysCurrent, discount=0.0, fees=0.0, divergence=0.0)
    main(arguments=sysArguments, parameters=sysParameters)



