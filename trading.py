# -*- coding: utf-8 -*-
"""
Created on Fri Jul 12 2024
@name:   Trading Platform
@author: Jack Kirby Cook

"""

import os
import sys
import time
import logging
import warnings
import numpy as np
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Variables, Contract
from finance.securities import SecurityCalculator, SecurityFilter, SecurityFiles
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationFilter
from finance.feasibility import FeasibilityCalculator, ExposureCalculator
from finance.holdings import HoldingWriter, HoldingReader, HoldingFiles, HoldingTable
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import SideThread, CycleThread
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class ContractLoader(Loader, query=Variables.Querys.CONTRACT, function=Contract.fromstr): pass
class ContractSaver(Saver, query=Variables.Querys.CONTRACT): pass


def market(*args, directory, loading, table, parameters={}, criterion={}, functions={}, **kwargs):
    security_loader = ContractLoader(name="MarketSecurityLoader", source=loading, directory=directory, wait=10)
    security_filter = SecurityFilter(name="MarketSecurityFilter", criterion=criterion["security"])
    strategy_calculator = StrategyCalculator(name="MarketStrategyCalculator")
    valuation_calculator = ValuationCalculator(name="MarketValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="MarketValuationFilter", criterion=criterion["valuation"])
    acquisition_writer = HoldingWriter(name="MarketAcquisitionWriter", destination=table, valuation=Variables.Valuations.ARBITRAGE, **functions)
    market_pipeline = security_loader + security_filter + strategy_calculator + valuation_calculator + valuation_filter + acquisition_writer
    market_thread = SideThread(market_pipeline, name="MarketThread")
    market_thread.setup(**parameters)
    return market_thread


def portfolio(*args, directory, loading, table, parameters={}, criterion={}, functions={}, **kwargs):
    holding_loader = ContractLoader(name="PortfolioHoldingLoader", source=loading, directory=directory, wait=0)
    exposure_calculator = ExposureCalculator(name="PortfolioExposureCalculator")
    security_calculator = SecurityCalculator(name="PortfolioSecurityCalculator", **functions)
    security_filter = SecurityFilter(name="PortfolioSecurityFilter", criterion=criterion["security"])
    strategy_calculator = StrategyCalculator(name="PortfolioStrategyCalculator")
    valuation_calculator = ValuationCalculator(name="PortfolioValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", criterion=criterion["valuation"])
    portfolio_calculator = FeasibilityCalculator(name="PortfolioFeasibilityCalculator", valuation=Variables.Valuations.ARBITRAGE)
    divestiture_writer = HoldingWriter(name="PortfolioDivestitureWriter", destination=table, valuation=Variables.Valuations.ARBITRAGE, **functions)
    portfolio_pipeline = holding_loader + exposure_calculator + security_calculator + security_filter + strategy_calculator + valuation_calculator + valuation_filter + portfolio_calculator + divestiture_writer
    portfolio_thread = CycleThread(portfolio_pipeline, name="PortfolioThread", wait=10)
    portfolio_thread.setup(**parameters)
    return portfolio_thread


def acquisition(*args, table, saving, parameters={}, **kwargs):
    acquisition_reader = HoldingReader(name="AcquisitionReader", source=table)
    acquisition_saver = ContractSaver(name="AcquisitionSaver", destination=saving)
    acquisition_pipeline = acquisition_reader + acquisition_saver
    acquisition_thread = CycleThread(acquisition_pipeline, name="AcquisitionThread", wait=5)
    acquisition_thread.setup(**parameters)
    return acquisition_thread


def divestiture(*args, table, saving, parameters={}, **kwargs):
    divestiture_reader = HoldingReader(name="DivestitureReader", source=table)
    divestiture_saver = ContractSaver(name="DivestitureSaver", destination=saving)
    divestiture_pipeline = divestiture_reader + divestiture_saver
    divestiture_thread = CycleThread(divestiture_pipeline, name="DivestitureThread", wait=5)
    divestiture_thread.setup(**parameters)
    return divestiture_thread


def main(*args, arguments, parameters, **kwargs):
    option_file = SecurityFiles.Option(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holding_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    acquisition_table = HoldingTable(name="AcquisitionTable")
    divestiture_table = HoldingTable(name="DivestitureTable")

    valuation_criterion = {Criterion.FLOOR: {"apy": arguments["apy"], "size": arguments["size"]}, Criterion.NULL: ["apy", "size"]}
    security_criterion = {Criterion.FLOOR: {"size": arguments["size"]}, Criterion.NULL: ["size"]}
    criterion = dict(valuation=valuation_criterion, security=security_criterion)

    liquidity_function = lambda cols: np.floor(cols["size"] * 0.1).astype(np.int32)
    priority_function = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    size_function = lambda cols: np.int32(arguments["size"])
    functions = dict(liquidity=liquidity_function, priority=priority_function, size=size_function)

    market_parameters = dict(directory=option_file, loading={option_file: "r"}, criterion=criterion, functions=functions, parameters=parameters)
    portfolio_parameters = dict(directory=holding_file, loading={holding_file: "r"}, criterion=criterion, functions=functions, parameters=parameters)
    acquisition_parameters = dict(table=acquisition_table, saving={holding_file: "a"}, parameters=parameters)
    divestiture_parameters = dict(table=divestiture_table, saving={holding_file: "a"}, parameters=parameters)

    market_thread = market(*args, **market_parameters, **kwargs)
    portfolio_thread = portfolio(*args, **portfolio_parameters, **kwargs)
    acquisition_thread = acquisition(*args, **acquisition_parameters, **kwargs)
    divestiture_thread = divestiture(*args, **divestiture_parameters, **kwargs)
    threads = [market_thread, portfolio_thread, acquisition_thread, divestiture_thread]

    for thread in iter(threads):
        thread.start()
    while True:
        print(acquisition_table)
        print(divestiture_table)
        time.sleep(10)
    for thread in iter(threads):
        thread.cease()
    for thread in reversed(threads):
        thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    current = Datetime(year=2024, month=7, day=12)
    sysArguments = dict(apy=0.0035, size=10)
    sysParameters = dict(current=current, discount=0.0, fees=0.0, factor=0.1)
    main(arguments=sysArguments, parameters=sysParameters)


