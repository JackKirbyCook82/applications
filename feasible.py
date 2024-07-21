# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   Trading Platform Feasibility
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
from finance.feasibility import FeasibilityCalculator, ExposureCalculator
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import SideThread
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class ContractLoader(Loader, query=Variables.Querys.CONTRACT, function=Contract.fromstr): pass
class ContractSaver(Saver, query=Variables.Querys.CONTRACT): pass


def exposure(*args, directory, loading, saving, current, parameters={}, criterion={}, functions={}, **kwargs):
    holding_loader = ContractLoader(name="PortfolioHoldingLoader", source=loading, directory=directory)
    exposure_calculator = ExposureCalculator(name="PortfolioExposureCalculator", **functions)
    security_calculator = SecurityCalculator(name="PortfolioSecurityCalculator", **functions)
    security_filter = SecurityFilter(name="PortfolioSecurityFilter", criterion=criterion["security"], **functions)
    strategy_calculator = StrategyCalculator(name="PortfolioStrategyCalculator", **functions)
    valuation_calculator = ValuationCalculator(name="PortfolioValuationCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", criterion=criterion["valuation"])
    portfolio_calculator = FeasibilityCalculator(name="PortfolioFeasibilityCalculator", valuation=Variables.Valuations.ARBITRAGE, **functions)
    valuation_saver = ContractSaver(name="PortfolioValuationSaver", destination=saving)
    exposure_pipeline = holding_loader + exposure_calculator + security_calculator + security_filter + strategy_calculator + valuation_calculator + valuation_filter + portfolio_calculator + valuation_saver
    exposure_thread = SideThread(exposure_pipeline, name="PortfolioExposureThread")
    exposure_thread.setup(current=current, **parameters)
    return exposure_thread


def main(*args, **kwargs):
    statistic_file = TechnicalFiles.Statistic(name="StatisticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    arbitrage_file = ValuationFiles.Arbitrage(name="ArbitrageFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    security_criterion = {Criterion.FLOOR: {"size": 10}}
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.00035, "size": 10}, Criterion.NULL: ["apy", "size"]}
    factor_function = lambda count: 0.1 * count * np.sin(count * 2 * np.pi / 10).astype(np.float32)
    criterion = dict(security=security_criterion, valuation=valuation_criterion)
    functions = dict(size=lambda cols: np.int32(10), volume=lambda cols: np.NaN, interest=lambda cols: np.NaN, factor=factor_function)
    exposure_parameters = dict(directory=holdings_file, loading={holdings_file: "r", statistic_file: "r"}, saving={arbitrage_file: "w"}, criterion=criterion, functions=functions)
    exposure_thread = exposure(*args, **exposure_parameters, **kwargs)
    exposure_thread.start()
    exposure_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    sysCurrent = Datetime(year=2024, month=7, day=18)
    sysParameters = dict(discount=0.0, fees=0.0)
    main(current=sysCurrent, parameters=sysParameters)



