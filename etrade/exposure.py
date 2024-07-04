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
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
HISTORY = os.path.join(ROOT, "repository", "history")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Variables, Contract
from finance.technicals import TechnicalFiles
from finance.securities import SecurityCalculator, SecurityFilter, SecurityFiles
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationFilter, ValuationFiles
from finance.holdings import HoldingFiles
from finance.exposures import ExposureCalculator, ExposureFiles
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


def exposure(*args, directory, loading, saving, parameters={}, **kwargs):
    holding_loader = ContractLoader(name="PortfolioHoldingLoader", source=loading, directory=directory)
    exposure_calculator = ExposureCalculator(name="PortfolioExposureCalculator")
    exposure_saver = ContractSaver(name="PortfolioExposureSaver", destination=saving)
    exposure_pipeline = holding_loader + exposure_calculator + exposure_saver
    exposure_thread = SideThread(exposure_pipeline, name="PortfolioExposureThread")
    exposure_thread.setup(**parameters)
    return exposure_thread


def security(*args, directory, loading, saving, current, parameters={}, **kwargs):
    exposure_loader = ContractLoader(name="PortfolioExposureLoader", source=loading, directory=directory)
    security_calculator = SecurityCalculator(name="PortfolioSecurityCalculator")
    security_saver = ContractSaver(name="PortfolioSecuritySaver", destination=saving)
    security_pipeline = exposure_loader + security_calculator + security_saver
    security_thread = SideThread(security_pipeline, name="PortfolioSecurityThread")
    security_thread.setup(current=current, **parameters)
    return security_thread


def valuation(*args, directory, loading, saving, parameters={}, criterion={}, **kwargs):
    security_loader = ContractLoader(name="PortfolioSecurityLoader", source=loading, directory=directory)
    security_filter = SecurityFilter(name="PortfolioSecurityFilter", criterion=criterion["security"])
    strategy_calculator = StrategyCalculator(name="PortfolioStrategyCalculator")
    valuation_calculator = ValuationCalculator(name="PortfolioValuationCalculator", criterion=criterion["valuation"])
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter")
    valuation_saver = ContractSaver(name="PortfolioValuationSaver", destination=saving)
    valuation_pipeline = security_loader + security_filter + strategy_calculator + valuation_calculator + valuation_filter + valuation_saver
    valuation_thread = SideThread(valuation_pipeline, name="PortfolioValuationThread")
    valuation_thread.setup(**parameters)
    return valuation_thread


def main(*args, **kwargs):
    statistic_file = TechnicalFiles.Statistic(name="StatisticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    exposure_file = ExposureFiles.Exposure(name="ExposureFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    option_file = SecurityFiles.Option(name="OptionFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    arbitrage_file = ValuationFiles.Arbitrage(name="ArbitrageFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    security_criterion = {Criterion.FLOOR: {"size": 10}}
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.0, "size": 10}, Criterion.NULL: ["apy", "size"]}
    criterion = dict(security=security_criterion, valuation=valuation_criterion)
    exposure_parameters = dict(directory=holdings_file, loading={holdings_file: "r"}, saving={exposure_file: "w"}, criterion=criterion)
    security_parameters = dict(directory=exposure_file, loading={exposure_file: "r", statistic_file: "r"}, saving={option_file: "w"}, criterion=criterion)
    valuation_parameters = dict(directory=option_file, loading={option_file: "r"}, saving={arbitrage_file: "w"}, criterion=criterion)
    exposure_thread = exposure(*args, **exposure_parameters, **kwargs)
    security_thread = security(*args, **security_parameters, **kwargs)
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
    sysCurrent = Datetime(year=2024, month=6, day=21)
    sysParameters = dict(discount=0.0, fees=0.0)
    main(current=sysCurrent, parameters=sysParameters)



