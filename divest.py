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

from finance.variables import Variables, Querys
from finance.exposures import ExposureCalculator, ExposureWriter, ExposureTable
from finance.securities import OptionCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationWriter, ValuationReader, ValuationTable
from finance.orders import OrderCalculator
from finance.stability import StabilityCalculator, StabilityFilter
from finance.holdings import HoldingCalculator, HoldingFile
from support.files import Loader, Saver, FileTypes, FileTimings
from support.filtering import Filter, Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


def main(*args, arguments, parameters, **kwargs):
    option_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    valuation_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    sizing_functions = {"volume": lambda cols: np.NaN, "size": lambda cols: np.NaN, "interest": lambda cols: np.NaN}
    timing_functions = {"current": lambda cols: np.NaN}
    holding_file = HoldingFile(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    divestiture_table = ValuationTable(name="DivestitureTable")
    exposure_table = ExposureTable(name="ExposureTable")
    holding_loader = Loader(name="HoldingLoader", file=holding_file, mode="r", query=Querys.Contract)
    exposure_calculator = ExposureCalculator(name="ExposureCalculator")
    exposure_writer = ExposureWriter(name="ExposureWriter", table=exposure_table)
    option_calculator = OptionCalculator(name="OptionCalculator", pricing=Variables.Pricing.BLACKSCHOLES, sizings=sizing_functions, timings=timing_functions)
    option_filter = Filter(name="OptionFilter", criterion=option_criterion)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Variables.Strategies))
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    valuation_filter = Filter(name="ValuationFilter", criterion=valuation_criterion)
    order_calculator = OrderCalculator(name="OrderCalculator")
    stability_calculator = StabilityCalculator(name="StabilityCalculator")
    stability_filter = StabilityFilter(name="StabilityFilter")
    valuation_writer = ValuationWriter(name="ValuationWriter", table=divestiture_table, valuation=Variables.Valuations.ARBITRAGE, priority=valuation_priority)
    valuation_reader = ValuationReader(name="ValuationReader", table=divestiture_table, valuation=Variables.Valuations.ARBITRAGE)
    holding_calculator = HoldingCalculator(name="HoldingCalculator", valuation=Variables.Valuations.ARBITRAGE)
    holding_saver = Saver(name="HoldingSaver", file=holding_file, mode="a", query=Querys.Contract)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysCurrent = Datetime(year=2024, month=7, day=18)
    sysArguments = dict(apy=0.00, size=10, volume=100, interest=100)
    sysParameters = dict(current=sysCurrent, discount=0.00, fees=0.00)
    main(arguments=sysArguments, parameters=sysParameters)




