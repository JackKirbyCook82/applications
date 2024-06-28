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
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Variables
from finance.valuations import ValuationFilter, ValuationFiles
from finance.holdings import HoldingWriter, HoldingReader, HoldingFiles, HoldingTable
from support.files import Loader, Saver, Directory, FileTypes, FileTimings
from support.synchronize import SideThread, CycleThread

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class ContractLoader(Loader, query=Variables.Querys.CONTRACT): pass
class ContractSaver(Saver, query=Variables.Querys.CONTRACT): pass
class ContractDirectory(Directory):
    @staticmethod
    def parser(filename):
        ticker, expire = str(filename).split("_")
        ticker = str(ticker).upper()
        expire = Datetime.strptime(expire, "%Y%m%d")
        return Variables.Querys.CONTRACT(ticker, expire)


def market(*args, loading, directory, destination, parameters={}, **kwargs):
    valuation_loader = ContractLoader(name="MarketValuationLoader", source=loading, directory=directory)
    valuation_filter = ValuationFilter(name="MarketValuationFilter")
    acquisition_writer = HoldingWriter(name="MarketAcquisitionWriter", destination=destination)
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
    arbitrage_directory = ContractDirectory(name="ArbitrageDirectory", repository=MARKET, variable=Variables.Valuations.ARBITRAGE)
    acquisitions_table = HoldingTable(name="AcquisitionTable")
    market_parameters = dict(loading={arbitrage_file: "r"}, directory=arbitrage_directory, destination=acquisitions_table)
    acquisition_parameters = dict(source=acquisitions_table, saving={holdings_file: "a"})
    market_thread = market(*args, **market_parameters, **kwargs)
    acquisition_thread = acquisition(*args, **acquisition_parameters, **kwargs)
    market_thread.start()
    market_thread.join()
    acquisition_thread.start()
    while True:
        print(str(acquisitions_table))
        if not bool(acquisitions_table):
            break
        acquisitions_table[0:25, "status"] = Variables.Status.PURCHASED
        time.sleep(2)
    acquisition_thread.cease()
    acquisition_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    sysParameters = dict()
    main(parameters=sysParameters)



