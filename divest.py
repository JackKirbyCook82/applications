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
from finance.holdings import ValuationWriter, ValuationReader, ValuationTable, HoldingFiles
from finance.exposures import ExposureCalculator, ExposureReporter
from finance.allocation import AllocationCalculator
from finance.stability import StabilityCalculator
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import RepeatingThread
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class ContractLoader(Loader, variable=Variables.Querys.CONTRACT, function=Contract.fromstr): pass
class ContractSaver(Saver, variable=Variables.Querys.CONTRACT): pass


def portfolio(*args, directory, loading, table, reporter, parameters={}, criterion={}, functions={}, **kwargs):
    holding_loader = ContractLoader(name="PortfolioHoldingLoader", files=loading, directory=directory)
    exposure_calculator = ExposureCalculator(name="PortfolioExposureCalculator", reporter=reporter, **functions)
    security_calculator = SecurityCalculator(name="PortfolioSecurityCalculator", **functions)
    security_filter = SecurityFilter(name="PortfolioSecurityFilter", criterion=criterion["security"])
    strategy_calculator = StrategyCalculator(name="PortfolioStrategyCalculator", **functions)
    valuation_calculator = ValuationCalculator(name="PortfolioValuationCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", valuation=Variables.Valuations.ARBITRAGE, criterion=criterion["valuation"])
    allocation_calculator = AllocationCalculator(name="PortfolioAllocationCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    stability_calculator = StabilityCalculator(name="PortfolioStabilityCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    divestiture_writer = ValuationWriter(name="PortfolioDivestitureWriter", table=table, valuation=Variables.Valuations.ARBITRAGE, **functions)
    portfolio_pipeline = holding_loader + exposure_calculator + security_calculator + security_filter + strategy_calculator + valuation_calculator + valuation_filter + allocation_calculator + stability_calculator + divestiture_writer
    portfolio_thread = RepeatingThread(portfolio_pipeline, name="PortfolioThread", wait=10)
    portfolio_thread.setup(**parameters)
    return portfolio_thread


def divestiture(*args, table, saving, parameters={}, **kwargs):
    divestiture_reader = ValuationReader(name="PortfolioDivestitureReader", table=table, valuation=Variables.Valuations.ARBITRAGE)
    divestiture_saver = ContractSaver(name="PortfolioDivestitureSaver", files=saving)
    divestiture_pipeline = divestiture_reader + divestiture_saver
    divestiture_thread = RepeatingThread(divestiture_pipeline, name="PortfolioDivestitureThread", wait=10)
    divestiture_thread.setup(**parameters)
    return divestiture_thread


def main(*args, logger, arguments, parameters, **kwargs):
    statistic_file = TechnicalFiles.Statistic(name="StatisticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    divestiture_table = ValuationTable(name="DivestitureTable", valuation=Variables.Valuations.ARBITRAGE)
    exposure_reporter = ExposureReporter(name="PortfolioExposure")

    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    security_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    priority_function = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    functions = dict(priority=priority_function, size=lambda cols: np.random.randint(10, 50), volume=lambda cols: np.random.randint(100, 150), interest=lambda cols: np.random.randint(100, 150))
    criterion = dict(valuation=valuation_criterion, security=security_criterion)

    portfolio_parameters = dict(directory=holdings_file, loading={holdings_file: "r", statistic_file: "r"}, table=divestiture_table, reporter=exposure_reporter, criterion=criterion, functions=functions, parameters=parameters)
    divestiture_parameters = dict(table=divestiture_table, saving={holdings_file: "a"}, criterion=criterion, functions=functions, parameters=parameters)
    portfolio_thread = portfolio(*args, **portfolio_parameters, **kwargs)
    divestiture_thread = divestiture(*args, **divestiture_parameters, **kwargs)

    divestiture_thread.start()
    portfolio_thread.start()
    while True:
        logger.info(f"Reporting: {repr(exposure_reporter)}")
        if bool(divestiture_table): print(divestiture_table)
        time.sleep(10)
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
    sysLogger = logging.getLogger(__name__)
    sysFactor = lambda Θ, Φ, ε, ω: 1 + (Φ * ε * ω)
    sysCurrent = Datetime(year=2024, month=7, day=18)
    sysArguments = dict(apy=0, size=10, volume=100, interest=100)
    sysParameters = dict(factor=sysFactor, current=sysCurrent, discount=0.00, fees=0.00, divergence=0.05)
    main(logger=sysLogger, arguments=sysArguments, parameters=sysParameters)



