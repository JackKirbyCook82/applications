# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   Trading Platform Stability
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
from finance.valuations import ValuationCalculator, ValuationFilter, ValuationFiles
from finance.holdings import HoldingFiles
from finance.exposures import ExposureCalculator
from finance.allocation import AllocationCalculator
from finance.stability import StabilityCalculator
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import CycleThread
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class ContractLoader(Loader, query=Variables.Querys.CONTRACT, create=Contract.fromstr): pass
class ContractSaver(Saver, query=Variables.Querys.CONTRACT): pass


def valuations(*args, directory, loading, saving, parameters={}, criterion={}, functions={}, **kwargs):
    holding_loader = ContractLoader(name="PortfolioHoldingLoader", source=loading, directory=directory)
    exposure_calculator = ExposureCalculator(name="PortfolioExposureCalculator", **functions)
    security_calculator = SecurityCalculator(name="PortfolioSecurityCalculator", **functions)
    security_filter = SecurityFilter(name="PortfolioSecurityFilter", criterion=criterion["security"])
    strategy_calculator = StrategyCalculator(name="PortfolioStrategyCalculator", **functions)
    valuation_calculator = ValuationCalculator(name="PortfolioValuationCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", valuation=Variables.Valuations.ARBITRAGE, criterion=criterion["valuation"])
    allocation_calculator = AllocationCalculator(name="PortfolioAllocationCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    stability_calculator = StabilityCalculator(name="PortfolioStabilityCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    valuation_saver = ContractSaver(name="PortfolioValuationSaver", destination=saving)
    exposure_pipeline = holding_loader + exposure_calculator + security_calculator + security_filter + strategy_calculator + valuation_calculator + valuation_filter + allocation_calculator + stability_calculator + valuation_saver
    exposure_thread = CycleThread(exposure_pipeline, name="PortfolioValuationThread")
    exposure_thread.setup(**parameters)
    return exposure_thread


def main(*args, arguments, parameters, **kwargs):
    statistic_file = TechnicalFiles.Statistic(name="StatisticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    arbitrage_file = ValuationFiles.Arbitrage(name="ArbitrageFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)

    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    security_criterion = {Criterion.FLOOR: {"size": arguments["size"]}}
    functions = dict(size=lambda cols: arguments["size"], volume=lambda cols: arguments["volume"], interest=lambda cols: arguments["interest"])
    criterion = dict(security=security_criterion, valuation=valuation_criterion)

    valuations_parameters = dict(directory=holdings_file, loading={holdings_file: "r", statistic_file: "r"}, saving={arbitrage_file: "w"}, criterion=criterion, functions=functions, parameters=parameters)
    valuations_thread = valuations(*args, **valuations_parameters, **kwargs)
    valuations_thread.start()
    while True:
        time.sleep(10)
    valuations_thread.cease()
    valuations_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysCurrent = Datetime(year=2024, month=7, day=18)
    sysArguments = dict(apy=-1, size=np.int32(10), volume=np.NaN, interest=np.NaN)
    sysParameters = dict(current=sysCurrent, discount=0.0, fees=0.0)
    main(arguments=sysArguments, parameters=sysParameters)



