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
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.valuations import ValuationCalculator, ValuationFilter, ValuationFiles
from finance.securities import SecurityCalculator, SecurityFilter, SecurityFiles
from finance.exposures import ExposureCalculator, ExposureFiles
from finance.variables import Strategies, Querys, Variables
from finance.strategies import StrategyCalculator
from finance.technicals import TechnicalFiles
from finance.holdings import HoldingFiles
from support.files import Loader, Saver, Directory, FileTypes, FileTimings
from support.synchronize import SideThread
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class ContractLoader(Loader, query=("contract", Querys.Contract)): pass
class ContractSaver(Saver, query=("contract", Querys.Contract)): pass
class ContractDirectory(Directory):
    @staticmethod
    def parser(filename):
        ticker, expire = str(filename).split("_")
        ticker = str(ticker).upper()
        expire = Datetime.strptime(expire, "%Y%m%d")
        return Querys.Contract(ticker, expire)


def exposure(*args, loading, saving, directory, current, parameters={}, **kwargs):
    holding_loader = ContractLoader(name="PortfolioHoldingLoader", source=loading, directory=directory)
    exposure_calculator = ExposureCalculator(name="PortfolioExposureCalculator")
    exposure_saver = ContractSaver(name="PortfolioExposureSaver", destination=saving)
    exposure_pipeline = holding_loader + exposure_calculator + exposure_saver
    exposure_thread = SideThread(exposure_pipeline, name="PortfolioExposureThread")
    exposure_thread.setup(current=current, **parameters)
    return exposure_thread


def security(*args, loading, saving, directory, functions, parameters={}, **kwargs):
    exposure_loader = ContractLoader(name="PortfolioExposureLoader", source=loading, directory=directory)
    security_calculator = SecurityCalculator(name="PortfolioSecurityCalculator", **functions)
    security_saver = ContractSaver(name="PortfolioSecuritySaver", destination=saving)
    security_pipeline = exposure_loader + security_calculator + security_saver
    security_thread = SideThread(security_pipeline, name="PortfolioSecurityThread")
    security_thread.setup(**parameters)
    return security_thread


def valuation(*args, loading, saving, directory, calculations=[], criterions={}, parameters={}, **kwargs):
    security_loader = ContractLoader(name="PortfolioSecurityLoader", source=loading, directory=directory)
    security_filter = SecurityFilter(name="PortfolioSecurityFilter", criterion=criterions["security"])
    strategy_calculator = StrategyCalculator(name="PortfolioStrategyCalculator", calculations=calculations)
    valuation_calculator = ValuationCalculator(name="PortfolioValuationCalculator", calculation=Variables.Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", criterion=criterions["valuation"])
    valuation_saver = ContractSaver(name="PortfolioValuationSaver", destination=saving)
    valuation_pipeline = security_loader + security_filter + strategy_calculator + valuation_calculator + valuation_filter + valuation_saver
    valuation_thread = SideThread(valuation_pipeline, name="PortfolioValuationThread")
    valuation_thread.setup(**parameters)
    return valuation_thread


def main(*args, **kwargs):
    security_criterion = {Criterion.FLOOR: {"size": 10}, Criterion.NULL: ["size"]}
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.0, "size": 10}, Criterion.NULL: ["apy", "size"]}
    criterions = dict(security=security_criterion, valuation=valuation_criterion)
    functions = dict(size=lambda cols: np.int32(10), volume=np.NaN, interest=np.NaN)
    calculations = [Strategies.Collar.Long, Strategies.Collar.Short, Strategies.Vertical.Put, Strategies.Vertical.Call]
    statistic_file = TechnicalFiles.Statistic(name="StatisticFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    exposure_file = ExposureFiles.Exposure(name="ExposureFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    option_file = SecurityFiles.Options(name="OptionFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    arbitrage_file = ValuationFiles.Arbitrage(name="ArbitrageFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_directory = ContractDirectory(name="HoldingDirectory", repository=PORTFOLIO, variable="holdings")
    exposure_directory = ContractDirectory(name="ArbitrageDirectory", repository=PORTFOLIO, variable="exposure")
    security_directory = ContractDirectory(name="ArbitrageDirectory", repository=PORTFOLIO, variable="option")
    exposure_parameters = dict(loading={holdings_file: "r"}, saving={exposure_file: "w"}, directory=holdings_directory)
    exposure_thread = exposure(*args, **exposure_parameters, **kwargs)
    security_parameters = dict(loading={exposure_file: "r", statistic_file: "r"}, saving={option_file: "w"}, directory=exposure_directory, functions=functions)
    security_thread = security(*args, **security_parameters, **kwargs)
    valuation_parameters = dict(loading={option_file: "r"}, saving={arbitrage_file: "w"}, directory=security_directory, calculations=calculations, criterions=criterions)
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
    sysCurrent = Datetime(year=2024, month=6, day=17)
    sysParameters = dict(discount=0.0, fees=0.0)
    main(current=sysCurrent, parameters=sysParameters)



