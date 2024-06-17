# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Trading Platform Exposures
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import numpy as np

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.valuations import ValuationCalculator, ValuationFilter, ValuationFiles
from finance.securities import SecurityCalculator, SecurityFilter, SecurityFiles
from finance.exposures import ExposureCalculator, ExposureFiles
from finance.variables import Variables, Strategies
from finance.strategies import StrategyCalculator
from finance.holdings import HoldingFiles
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import SideThread
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


def exposure(*args, source, destination, parameters={}, **kwargs):
    holding_loader = Loader(name="PortfolioHoldingLoader", source=source)
    exposure_calculator = ExposureCalculator(name="PortfolioExposureCalculator")
    exposure_saver = Saver(name="PortfolioExposureSaver", destination=destination)
    exposure_pipeline = holding_loader + exposure_calculator + exposure_saver
    exposure_thread = SideThread(exposure_pipeline, name="PortfolioExposureThread")
    exposure_thread.setup(**parameters)
    return exposure_thread


def security(*args, source, destination, functions, parameters={}, **kwargs):
    exposure_loader = Loader(name="PortfolioExposureLoader", source=source)
    security_calculator = SecurityCalculator(name="PortfolioSecurityCalculator", **functions)
    security_saver = Saver(name="PortfolioSecuritySaver", destination=destination)
    security_pipeline = exposure_loader + security_calculator + security_saver
    security_thread = SideThread(security_pipeline, name="PortfolioSecurityThread")
    security_thread.setup(**parameters)
    return security_thread


def valuation(*args, source, destination, calculations, criterion, parameters={}, **kwargs):
    security_loader = Loader(name="PortfolioSecurityLoader", source=source)
    security_filter = SecurityFilter(name="PortfolioSecurityFilter", criterion=criterion["security"])
    strategy_calculator = StrategyCalculator(name="PortfolioStrategyCalculator", calculations=calculations)
    valuation_calculator = ValuationCalculator(name="PortfolioValuationCalculator", calculation=Variables.Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", criterion=criterion["valuation"])
    valuation_saver = Saver(name="PortfolioValuationSaver", destination=destination)
    valuation_pipeline = security_loader + security_filter + strategy_calculator + valuation_calculator + valuation_filter + valuation_saver
    valuation_thread = SideThread(valuation_pipeline, name="PortfolioValuationThread")
    valuation_thread.setup(**parameters)
    return valuation_thread


def main(*args, **kwargs):
    security_criterion = {Criterion.FLOOR: {"size": 10}, Criterion.NULL: ["size"]}
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.0, "size": 10}, Criterion.NULL: ["apy", "size"]}
    criterion = dict(security=security_criterion, valuation=valuation_criterion)
    functions = dict(size=lambda cols: np.int32(10), volume=np.NaN, interest=np.NaN)
    calculations = [Strategies.Collar.Long, Strategies.Collar.Short, Strategies.Vertical.Put, Strategies.Vertical.Call]
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    exposure_file = ExposureFiles.Exposure(name="ExposureFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    option_file = SecurityFiles.Options(name="OptionFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    arbitrage_file = ValuationFiles.Arbitrage(name="ArbitrageFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    exposure_parameters = dict(source={holdings_file: "r"}, destination={exposure_file: "w"}, )
    exposure_thread = exposure(*args, **exposure_parameters, **kwargs)
    security_parameters = dict(source={exposure_file: "r"}, destination={option_file: "w"}, functions=functions)
    security_thread = exposure(*args, **security_parameters, **kwargs)
    valuation_parameters = dict(source={option_file: "r"}, destination={arbitrage_file: "w"}, calculations=calculations, criterion=criterion)
    valuation_thread = valuation(*args, **valuation_parameters, **kwargs)
    exposure_thread.start()
    exposure_thread.join()
    security_thread.start()
    security_thread.join()
    valuation_thread.start()
    valuation_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    main(parameters={"discount": 0.0, "fees": 0.0})



