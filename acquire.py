# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   Trading Platform Acquisitions
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
MARKET = os.path.join(ROOT, "repository", "market")
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
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class ContractLoader(Loader, query=Variables.Querys.CONTRACT, function=Contract.fromstr): pass
class ContractSaver(Saver, query=Variables.Querys.CONTRACT): pass


def market(*args, directory, loading, destination, parameters={}, criterion={}, functions={}, **kwargs):
    valuation_loader = ContractLoader(name="MarketValuationLoader", source=loading, directory=directory)
    valuation_filter = ValuationFilter(name="MarketValuationFilter", criterion=criterion["valuation"])
    acquisition_writer = HoldingWriter(name="MarketAcquisitionWriter", destination=destination, valuation=Variables.Valuations.ARBITRAGE, **functions)
    market_pipeline = valuation_loader + valuation_filter + acquisition_writer
    market_thread = SideThread(market_pipeline, name="MarketValuationThread")
    market_thread.setup(**parameters)
    return market_thread


def acquisition(*args, source, saving, parameters={}, **kwargs):
    acquisition_reader = HoldingReader(name="PortfolioAcquisitionReader", source=source)
    acquisition_saver = ContractSaver(name="PortfolioAcquisitionSaver", destination=saving)
    acquisition_pipeline = acquisition_reader + acquisition_saver
    acquisition_thread = CycleThread(acquisition_pipeline, name="PortfolioAcquisitionThread", wait=10)
    acquisition_thread.setup(**parameters)
    return acquisition_thread


def main(*args, **kwargs):
    arbitrage_file = ValuationFiles.Arbitrage(name="ArbitrageFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    acquisition_table = HoldingTable(name="AcquisitionTable")
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.0035, "size": 10}, Criterion.NULL: ["apy", "size"]}
    priority_function = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    liquidity_function = lambda cols: np.floor(cols["size"] * 0.1).astype(np.int32)
    criterion = dict(valuation=valuation_criterion)
    functions = dict(liquidity=liquidity_function, priority=priority_function)
    market_parameters = dict(directory=arbitrage_file, loading={arbitrage_file: "r"}, destination=acquisition_table, criterion=criterion, functions=functions)
    acquisition_parameters = dict(source=acquisition_table, saving={holdings_file: "a"}, criterion=criterion, functions=functions)
    market_thread = market(*args, **market_parameters, **kwargs)
    acquisition_thread = acquisition(*args, **acquisition_parameters, **kwargs)
    market_thread.start()
    market_thread.join()
    acquisition_thread.start()
    while True:
        print(str(acquisition_table))
        if not bool(acquisition_table):
            break
        acquisition_table[0:25, "status"] = Variables.Status.PURCHASED
        time.sleep(5)
    acquisition_thread.cease()
    acquisition_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    sysParameters = dict()
    main(parameters=sysParameters)



