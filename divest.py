# -*- coding: utf-8 -*-
"""
Created on Weds Jun 13 2024
@name:   Trading Platform Divestitures
@author: Jack Kirby Cook

"""

import os
import sys
import time
import logging
import warnings
import numpy as np

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Variables, Contract
from finance.valuations import ValuationFilter, ValuationFiles
from finance.holdings import HoldingWriter, HoldingReader, HoldingTable, HoldingFiles
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


def portfolio(*args, directory, loading, table, parameters={}, criterion={}, functions={}, **kwargs):
    valuation_loader = ContractLoader(name="PortfolioValuationLoader", source=loading, directory=directory)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", criterion=criterion["valuation"])
    divestiture_writer = HoldingWriter(name="PortfolioDivestitureWriter", destination=table, valuation=Variables.Valuations.ARBITRAGE, **functions)
    portfolio_pipeline = valuation_loader + valuation_filter + divestiture_writer
    portfolio_thread = SideThread(portfolio_pipeline, name="PortfolioValuationThread")
    portfolio_thread.setup(**parameters)
    return portfolio_thread


def divestiture(*args, table, saving, parameters={}, **kwargs):
    divestiture_reader = HoldingReader(name="PortfolioDivestitureReader", source=table)
    divestiture_saver = ContractSaver(name="PortfolioDivestitureSaver", destination=saving, valuation=Variables.Valuations.ARBITRAGE)
    divestiture_pipeline = divestiture_reader + divestiture_saver
    divestiture_thread = CycleThread(divestiture_pipeline, name="PortfolioDivestitureThread", wait=10)
    divestiture_thread.setup(**parameters)
    return divestiture_thread


def main(*args, **kwargs):
    arbitrage_file = ValuationFiles.Arbitrage(name="ArbitrageFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    divestiture_table = HoldingTable(name="DivestitureTable")
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.001, "size": 10}, Criterion.NULL: ["apy", "size"]}
    priority_function = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    liquidity_function = lambda cols: np.floor(cols["size"] * 0.1).astype(np.int32)
    functions = dict(liquidity=liquidity_function, priority=priority_function)
    criterion = dict(valuation=valuation_criterion)
    portfolio_parameters = dict(directory=arbitrage_file, loading={arbitrage_file: "r"}, table=divestiture_table, criterion=criterion, functions=functions)
    divestiture_parameters = dict(table=divestiture_table, saving={holdings_file: "a"}, criterion=criterion, functions=functions)
    portfolio_thread = portfolio(*args, **portfolio_parameters, **kwargs)
    divestiture_thread = divestiture(*args, **divestiture_parameters, **kwargs)

    divestiture_thread.start()
    portfolio_thread.start()
    while bool(divestiture_thread) or bool(divestiture_table):
        print(divestiture_table)
        time.sleep(30)
    divestiture_thread.cease()
    portfolio_thread.join()
    divestiture_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    main(parameters={})



