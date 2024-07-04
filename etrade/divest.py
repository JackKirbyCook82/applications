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

from finance.variables import Variables, Contract
from finance.valuations import ValuationFilter, ValuationFiles
from finance.holdings import HoldingWriter, HoldingReader, HoldingTable, HoldingFiles
from finance.exposures import ExposureFiles
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


def portfolio(*args, directory, loading, destination, parameters={}, criterion={}, functions={}, **kwargs):
    valuation_loader = ContractLoader(name="PortfolioValuationLoader", source=loading, directory=directory)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", criterion=criterion["valuation"])
    divestiture_writer = HoldingWriter(name="PortfolioDivestitureWriter", destination=destination, **functions)
    portfolio_pipeline = valuation_loader + valuation_filter + divestiture_writer
    portfolio_thread = SideThread(portfolio_pipeline, name="PortfolioValuationThread")
    portfolio_thread.setup(**parameters)
    return portfolio_thread


def divestiture(*args, source, saving, parameters={}, **kwargs):
    divestiture_reader = HoldingReader(name="PortfolioDivestitureReader", source=source)
    divestiture_saver = ContractSaver(name="PortfolioDivestitureSaver", destination=saving)
    divestiture_pipeline = divestiture_reader + divestiture_saver
    divestiture_thread = CycleThread(divestiture_pipeline, name="PortfolioDivestitureThread", wait=10)
    divestiture_thread.setup(**parameters)
    return divestiture_thread


def main(*args, **kwargs):
    arbitrage_file = ValuationFiles.Arbitrage(name="ArbitrageFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    exposure_file = ExposureFiles.Exposure(name="ExposureFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    divestiture_table = HoldingTable(name="DivestitureTable")
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.0, "size": 10}, Criterion.NULL: ["apy", "size"]}
    priority_function = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    liquidity_function = lambda cols: np.floor(cols["size"] * 0.1).astype(np.int32)
    criterion = dict(valuation=valuation_criterion)
    functions = dict(liquidity=liquidity_function, priority=priority_function)
    portfolio_parameters = dict(directory=arbitrage_file, loading={exposure_file: "r", arbitrage_file: "r"}, destination=divestiture_table, criterion=criterion, functions=functions)
    divestiture_parameters = dict(source=divestiture_table, saving={holdings_file: "a"}, criterion=criterion, functions=functions)
    portfolio_thread = portfolio(*args, **portfolio_parameters, **kwargs)
    divestiture_thread = divestiture(*args, **divestiture_parameters, **kwargs)
    portfolio_thread.start()
    portfolio_thread.join()
    divestiture_thread.start()
    while True:
        if not bool(divestiture_table):
            break
        divestiture_table[0:25, "status"] = Variables.Status.PURCHASED
        time.sleep(5)
    divestiture_thread.cease()
    divestiture_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    sysParameters = dict()
    main(parameters=sysParameters)



