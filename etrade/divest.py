# -*- coding: utf-8 -*-
"""
Created on Weds Jun 13 2024
@name:   ETrade Trading Platform Divestitures
@author: Jack Kirby Cook

"""

import os
import sys
import time
import logging
import warnings
import numpy as np

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.divestitures import DivestitureReader, DivestitureWriter
from finance.valuations import ValuationFilter, ValuationFiles
from finance.holdings import HoldingFiles, HoldingTable
from finance.exposures import ExposureFiles
from finance.variables import Variables
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import SideThread, CycleThread
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


def portfolio(*args, source, destination, criterion, functions, capacity, parameters={}, **kwargs):
    valuation_loader = Loader(name="PortfolioValuationLoader", source=source)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", criterion=criterion["valuation"])
    divestiture_writer = DivestitureWriter(name="PortfolioDivestitureWriter", destination=destination, calculation=Variables.Valuations.ARBITRAGE, capacity=capacity, **functions)
    portfolio_pipeline = valuation_loader + valuation_filter + divestiture_writer
    portfolio_thread = SideThread(portfolio_pipeline, name="PortfolioValuationThread")
    portfolio_thread.setup(**parameters)
    return portfolio_thread


def divestiture(*args, source, destination, parameters={}, **kwargs):
    divestiture_reader = DivestitureReader(name="PortfolioDivestitureReader", source=source)
    divestiture_saver = Saver(name="PortfolioDivestitureSaver", destination=destination)
    divestiture_pipeline = divestiture_reader + divestiture_saver
    divestiture_thread = CycleThread(divestiture_pipeline, name="PortfolioDivestitureThread", wait=10)
    divestiture_thread.setup(**parameters)
    return divestiture_thread


def main(*args, **kwargs):
    liquidity_function = lambda cols: np.floor(cols["size"] * 0.2).astype(np.int32)
    priority_function = lambda cols: cols[("apy", str(Variables.Scenarios.MINIMUM.name).lower())]
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.0, "size": 5}, Criterion.NULL: ["apy", "size"]}
    functions = dict(liquidity=liquidity_function, priority=priority_function)
    criterion = dict(valuation=valuation_criterion)
    arbitrage_file = ValuationFiles.Arbitrage(name="ArbitrageFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    exposure_file = ExposureFiles.Exposure(name="ExposureFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    divestiture_table = HoldingTable(name="DivestitureTable")
    portfolio_parameters = dict(source={exposure_file: "r", arbitrage_file: "r"}, destination=divestiture_table, functions=functions, criterion=criterion, capacity=None)
    portfolio_thread = portfolio(*args, **portfolio_parameters, **kwargs)
    divestiture_parameters = dict(source=divestiture_table, destination={holdings_file: "a"})
    divestiture_thread = divestiture(*args, **divestiture_parameters, **kwargs)
    portfolio_thread.start()
    portfolio_thread.stop()
    divestiture_thread.start()
    while True:
        print(str(divestiture_table))
        if not bool(divestiture_table):
            break
        divestiture_table[0:25, "status"] = Variables.Status.PURCHASED
        time.sleep(2)
    divestiture_thread.cease()
    divestiture_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    main(parameters={})



