# -*- coding: utf-8 -*-
"""
Created on Weds Jun 13 2024
@name:   Trading Platform Divestitures
@author: Jack Kirby Cook

"""

import os
import sys
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

from finance.variables import Variables
from support.files import FileTypes, FileTimings
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


def main(*args, arguments, parameters, **kwargs):
    holdings__file = HoldingsFile(name="HoldingsFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    valuation_table = ValuationTable(name="ValuationTable", valuation=Variables.Valuations.ARBITRAGE)
    exposure_table = ExposureTable(name="ExposureTable")

    option_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    criterions = dict(valuation=valuation_criterion, option=option_criterion)

    option_functions = dict(size=lambda cols: np.random.randint(10, 50), volume=lambda cols: np.random.randint(100, 150), interest=lambda cols: np.random.randint(100, 150))
    functions = dict(option=option_functions)

    exposure_calculator = ExposureCalculator(name="ExposureCalculator")
    option_calculator = OptionCalculator(name="OptionCalculator", sizing=functions["option"])
    option_filter = OptionFilter(name="OptionFilter", criterion=criterions["option"])
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Variables.Strategies))
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="ValuationFilter", valuation=Variables.Valuations.ARBITRAGE, criterion=criterions["valuation"])
    allocation_calculator = AllocationCalculator(name="AllocationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    stability_calculator = StabilityCalculator(name="StabilityCalculator", valuation=Variables.Valuations.ARBITRAGE)

    holdings_calculator = HoldingsCalculator(name="HoldingsCalculator", valuation=Variables.Valuations.ARBITRAGE)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysFactor = lambda Θ, Φ, ε, ω: 1 + (Φ * ε * ω)
    sysCurrent = Datetime(year=2024, month=7, day=18)
    sysArguments = dict(apy=0, size=10, volume=100, interest=100, discount=0.00, liquidity=25, capacity=1)
    sysParameters = dict(factor=sysFactor, current=sysCurrent, discount=0.00, fees=0.00, divergence=0.05)
    main(arguments=sysArguments, parameters=sysParameters)




