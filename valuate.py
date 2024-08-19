# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   Trading Platform Valuation
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import pandas as pd
import xarray as xr
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Variables, Contract
from finance.securities import SecurityFilter, SecurityFiles
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationFilter, ValuationFiles
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import SideThread
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class ContractLoader(Loader, query=Variables.Querys.CONTRACT, create=Contract.fromstr): pass
class ContractSaver(Saver, query=Variables.Querys.CONTRACT): pass


def valuation(*args, directory, loading, saving, parameters={}, criterion={}, functions={}, **kwargs):
    security_loader = ContractLoader(name="MarketSecurityLoader", source=loading, directory=directory)
    security_filter = SecurityFilter(name="MarketSecurityFilter", criterion=criterion["security"])
    strategy_calculator = StrategyCalculator(name="MarketStrategyCalculator", **functions)
    valuation_calculator = ValuationCalculator(name="MarketValuationCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    valuation_filter = ValuationFilter(name="MarketValuationFilter", valuation=Variables.Valuations.ARBITRAGE, criterion=criterion["valuation"])
    valuation_saver = ContractSaver(name="MarketValuationSaver", destination=saving)
    valuation_pipeline = security_loader + security_filter + strategy_calculator + valuation_calculator + valuation_filter + valuation_saver
    valuation_thread = SideThread(valuation_pipeline, name="MarketValuationThread")
    valuation_thread.setup(**parameters)
    return valuation_thread


def main(*args, arguments, parameters, **kwargs):
    option_file = SecurityFiles.Option(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    arbitrage_file = ValuationFiles.Arbitrage(name="ArbitrageFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)

    security_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    criterion = dict(security=security_criterion, valuation=valuation_criterion)

    valuation_parameters = dict(directory=option_file, loading={option_file: "r"}, saving={arbitrage_file: "w"}, criterion=criterion, parameters=parameters)
    valuation_thread = valuation(*args, **valuation_parameters, **kwargs)
    valuation_thread.start()
    valuation_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    current = Datetime(year=2024, month=7, day=18)
    sysArguments = dict(apy=0.50, size=10, volume=100, interest=100)
    sysParameters = dict(current=current, discount=0.0, fees=0.0)
    main(arguments=sysArguments, parameters=sysParameters)



