# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Trading Platform Acquisitions
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
MARKET = os.path.join(ROOT, "repository", "market")
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.valuations import ValuationFiles, ValuationFilter
from finance.holdings import HoldingFiles, HoldingTable
from finance.acquisitions import AcquisitionReader, AcquisitionWriter
from finance.variables import Status, Scenarios, Valuations
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import SideThread, CycleThread
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


def market(*args, source, destination, parameters, criterion, functions, capacity, **kwargs):
    valuation_loader = Loader(name="MarketValuationLoader", source=source)
    valuation_filter = ValuationFilter(name="MarketValuationFilter", criterion=criterion["valuation"])
    acquisition_writer = AcquisitionWriter(name="MarketAcquisitionWriter", destination=destination, calculation=Valuations.ARBITRAGE, capacity=capacity, **functions)
    market_pipeline = valuation_loader + valuation_filter + acquisition_writer
    market_thread = SideThread(market_pipeline, name="MarketValuationThread")
    market_thread.setup(**parameters)
    return market_thread


def acquisition(*args, source, destination, parameters, **kwargs):
    acquisition_reader = AcquisitionReader(name="PortfolioAcquisitionReader", source=source)
    acquisition_saver = Saver(name="PortfolioAcquisitionSaver", destination=destination)
    acquisition_pipeline = acquisition_reader + acquisition_saver
    acquisition_thread = CycleThread(acquisition_pipeline, name="PortfolioAcquisitionThread", wait=10)
    acquisition_thread.setup(**parameters)
    return acquisition_thread


def main(*args, **kwargs):
    liquidity_function = lambda cols: np.floor(cols["size"] * 0.2).astype(np.int32)
    priority_function = lambda cols: cols[("apy", str(Scenarios.MINIMUM.name).lower())]
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.0, "size": 5}, Criterion.NULL: ["apy", "size"]}
    functions = dict(liquidity=liquidity_function, priority=priority_function)
    criterion = dict(valuation=valuation_criterion)
    arbitrage_file = ValuationFiles.Arbitrage(name="ArbitrageFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holdings_file = HoldingFiles.Holding(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    acquisitions_table = HoldingTable(name="AcquisitionTable")
    market_parameters = dict(source={arbitrage_file: "r"}, destination=acquisitions_table, functions=functions, criterion=criterion, capacity=None)
    market_thread = market(*args, **market_parameters, **kwargs)
    acquisitions_parameters = dict(source=acquisitions_table, destination={holdings_file: "a"})
    acquisition_thread = acquisition(*args, **acquisitions_parameters, **kwargs)
    market_thread.start()
    market_thread.join()
    acquisition_thread.start()
    while True:
        print(str(acquisitions_table))
        if not bool(acquisitions_table):
            break
        acquisitions_table[0:25, "status"] = Status.PURCHASED
        time.sleep(2)
    acquisition_thread.cease()
    acquisition_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    main(parameters={})



