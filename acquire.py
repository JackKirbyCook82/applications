# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   Trading Platform Acquisitions
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import pandas as pd
import xarray as xr
from functools import reduce
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Variables
from finance.securities import OptionFile, SecurityFilter
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationFilter, ValuationCalculator, ValuationWriter, ArbitrageTable
from support.files import FileTypes, FileTimings
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


def main(*args, arguments, parameters, **kwargs):
    option_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    valuation_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    option_file = OptionFile(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    arbitrage_table = ArbitrageTable(name="ArbitrageTable")
    option_filter = SecurityFilter(name="OptionFilter", criterion=option_criterion)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator")
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="ValuationFilter", valuation=Variables.Valuations.ARBITRAGE, criterion=valuation_criterion)
    valuation_writer = ValuationWriter(name="ValuationFilter", table=arbitrage_table, valuation=Variables.Valuations.ARBITRAGE, priority=valuation_priority)

    acquire_pipeline = [option_file, option_filter, strategy_calculator, valuation_calculator, valuation_filter]
    acquire_producer = acquire_pipeline[0](**parameters)
    acquire_pipeline = reduce(lambda source, function: function(source=source, **parameters), acquire_pipeline[1:], acquire_producer)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysCurrent = Datetime(year=2024, month=10, day=9)
    sysArguments = dict(apy=0.50, size=10, volume=100, interest=100)
    sysParameters = dict(current=sysCurrent, discount=0.0, fees=0.0)
    main(arguments=sysArguments, parameters=sysParameters)




