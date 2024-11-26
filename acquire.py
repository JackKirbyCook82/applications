# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   Acquisition Calculations
@author: Jack Kirby Cook

"""

import os
import sys
import time
import logging
import warnings
import pandas as pd
import xarray as xr
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Variables, Querys
from finance.securities import OptionFile
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.prospects import ProspectCalculator, ProspectWriter, ProspectReader, ProspectTable, ProspectView, ProspectFormatting, ProspectHeader
from finance.holdings import HoldingCalculator, HoldingFile
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import RoutineThread, RepeatingThread
from support.pipelines import Producer, Processor, Consumer
from support.filtering import Filter, Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class OptionLoaderProducer(Loader, Producer, query=Querys.Contract): pass
class OptionFilterProcessor(Filter, Processor): pass
class StrategyCalculatorProcessor(StrategyCalculator, Processor): pass
class ValuationCalculatorProcessor(ValuationCalculator, Processor): pass
class ValuationFilterProcessor(Filter, Processor): pass
class ProspectCalculatorProcessor(ProspectCalculator, Processor): pass
class ProspectWriterConsumer(ProspectWriter, Consumer): pass
class ProspectReaderProducer(ProspectReader, Producer): pass
class HoldingCalculatorProcessor(HoldingCalculator, Processor): pass
class HoldingSaverConsumer(Saver, Consumer, query=Querys.Contract): pass


def main(*args, arguments, parameters, **kwargs):
    option_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    valuation_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    option_file = OptionFile(name="OptionFile", filetype=FileTypes.CSV, filetiming=FileTimings.EAGER, repository=MARKET)
    holding_file = HoldingFile(name="HoldingFile", filetype=FileTypes.CSV, filetiming=FileTimings.EAGER, repository=PORTFOLIO)
    acquisition_formatting = ProspectFormatting(name="AcquisitionFormatting", valuation=Variables.Valuations.ARBITRAGE)
    acquisition_header = ProspectHeader(name="AcquisitionHeader", valuation=Variables.Valuations.ARBITRAGE)
    acquisition_view = ProspectView(name="AcquisitionView", formatting=acquisition_formatting)
    acquisition_table = ProspectTable(name="AcquisitionTable", view=acquisition_view, header=acquisition_header)

    option_loader = OptionLoaderProducer(name="OptionDirectory", file=option_file, mode="r")
    option_filter = OptionFilterProcessor(name="OptionFilter", criterion=option_criterion)
    strategy_calculator = StrategyCalculatorProcessor(name="StrategyCalculator", strategies=Variables.Strategies)
    valuation_calculator = ValuationCalculatorProcessor(name="ValuationCalculator", header=acquisition_header)
    valuation_filter = ValuationFilterProcessor(name="ValuationFilter", criterion=valuation_criterion)
    prospect_calculator = ProspectCalculatorProcessor(name="ProspectCalculator", priority=valuation_priority)
    prospect_writer = ProspectWriterConsumer(name="ProspectWriter", table=acquisition_table)
    prospect_reader = ProspectReaderProducer(name="ProspectReader", table=acquisition_table)
    holding_calculator = HoldingCalculatorProcessor(name="HoldingCalculator", header=acquisition_header)
    holding_saver = HoldingSaverConsumer(name="HoldingSaver", file=holding_file, mode="a")

    valuation_process = option_loader + option_filter + strategy_calculator + valuation_calculator + valuation_filter + prospect_calculator + prospect_writer
    acquisition_process = prospect_reader + holding_calculator + holding_saver
    valuation_thread = RoutineThread(valuation_process, name="ValuationThread").setup(**parameters)
    acquisition_thread = RepeatingThread(acquisition_process, name="DivestitureThread", wait=10).setup(**parameters)

    acquisition_thread.start()
    valuation_thread.start()
    while True:
        if bool(acquisition_table): print(acquisition_table)
        time.sleep(10)
    valuation_thread.join()
    acquisition_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysCurrent = Datetime(year=2024, month=11, day=6)
    sysParameters = dict(current=sysCurrent, discount=0.00, fees=0.00)
    sysArguments = dict(apy=1.00, size=10, volume=100, interest=100)
    sysArguments = sysArguments | dict(discount=2.00, liquidity=25, capacity=10)
    main(arguments=sysArguments, parameters=sysParameters)




