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
from finance.valuations import ValuationCalculator, ValuationWriter, ValuationReader, ValuationTable
from finance.holdings import HoldingCalculator, HoldingFile
from support.files import Directory, Saver, FileTypes, FileTimings
from support.synchronize import RoutineThread, RepeatingThread
from support.filtering import Filter, Criterion
from support.mixins import Carryover

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


# from support.pipelines import Producer, Processor, Consumer
# class OptionDirectoryProducer(Directory, Producer): pass
# class OptionFilterProcessor(Filter, Processor, Carryover, carryover="query", leading=True): pass
# class StrategyCalculatorProcessor(StrategyCalculator, Processor, Carryover, carryover="contract", leading=True): pass
# class ValuationCalculatorProcessor(ValuationCalculator, Processor, Carryover, carryover="contract", leading=True): pass
# class ValuationFilterProcessor(Filter, Processor, Carryover, carryover="query", leading=True): pass
# class ValuationWriterConsumer(ValuationWriter, Consumer): pass
# class ValuationReaderProducer(ValuationReader, Producer): pass
# class HoldingCalculatorProcessor(HoldingCalculator, Processor, Carryover, carryover="contract", leading=True): pass
# class HoldingSaverConsumer(Saver, Consumer): pass


from support.processes import Source, Process
class OptionDirectorySource(Directory, Source, arguments=["contract", "options"]): pass
class OptionFilterProcess(Filter, Process, domain=["contract", "options"], arguments=["options"]): pass
class StrategyCalculatorProcess(StrategyCalculator, Process, domain=["contract", "options"], arguments=["strategies"]): pass
class ValuationCalculatorProcess(ValuationCalculator, Process, domain=["contract", "strategies"], arguments=["valuations"]): pass
class ValuationFilterProcess(Filter, Process, Carryover, domain=["contract", "valuations"], arguments=["valuations"]): pass
class ValuationWriterProcess(ValuationWriter, Process, domain=["contract", "valuations"]): pass
class ValuationReaderSource(ValuationReader, Source, arguments=["contract", "valuations"]): pass
class HoldingCalculatorProcess(HoldingCalculator, Process, domain=["contract", "valuations"], arguments=["holdings"]): pass
class HoldingSaverProcess(Saver, Process, domain=["contract", "holdings"]): pass


class TradingProcess(object):
    def __init__(self, table, *args, discount, liquidity, capacity, **kwargs):
        self.liquidity = liquidity
        self.discount = discount
        self.capacity = capacity
        self.table = table

    def __call__(self, *args, **kwargs):
        if not bool(self.table): return
        with self.table.mutex:
            print(self.table)
            pursue = self.status(Variables.Status.PROSPECT) & (self.table[:, "priority"] >= self.discount)
            pursue = self.limit(pursue)
            abandon = self.status(Variables.Status.PROSPECT) & (self.table[:, "priority"] < self.discount)
            accepted = self.status(Variables.Status.PENDING) & (self.table[:, "size"] >= self.liquidity)
            rejected = self.status(Variables.Status.PENDING) & (self.table[:, "size"] < self.liquidity)
            self.table.change(rejected, "status", Variables.Status.REJECTED)
            self.table.change(accepted, "status", Variables.Status.ACCEPTED)
            self.table.change(abandon, "status", Variables.Status.ABANDONED)
            self.table.change(pursue, "status", Variables.Status.PENDING)
            print(self.table)

    def status(self, status): return self.table[:, "status"] == status
    def limit(self, mask): return (mask.cumsum() < self.capacity + 1) & mask


def main(*args, arguments, parameters, **kwargs):
    option_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    valuation_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    valuation_combination = lambda valuations: pd.concat(valuations, axis=1)
    option_file = OptionFile(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holding_file = HoldingFile(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    acquisition_table = ValuationTable(name="AcquisitionTable", valuation=Variables.Valuations.ARBITRAGE)

    # option_directory = OptionDirectoryProducer(name="OptionDirectory", file=option_file, query=Querys.Contract, mode="r")
    # option_filter = OptionFilterProcessor(name="OptionFilter", criterion=option_criterion)
    # strategy_calculator = StrategyCalculatorProcessor(name="StrategyCalculator", strategies=Variables.Strategies)
    # valuation_calculator = ValuationCalculatorProcessor(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    # valuation_filter = ValuationFilterProcessor(name="ValuationFilter", criterion=valuation_criterion)
    # valuation_writer = ValuationWriterConsumer(name="ValuationWriter", table=acquisition_table, valuation=Variables.Valuations.ARBITRAGE, priority=valuation_priority)
    # valuation_reader = ValuationReaderProducer(name="ValuationReader", table=acquisition_table, valuation=Variables.Valuations.ARBITRAGE, query=Querys.Contract)
    # holding_calculator = HoldingCalculatorProcessor(name="HoldingCalculator", valuation=Variables.Valuations.ARBITRAGE)
    # holding_saver = HoldingSaverConsumer(name="HoldingSaver", file=holding_file, mode="a")

    option_directory = OptionDirectorySource(name="OptionDirectory", file=option_file, query=Querys.Contract, mode="r")
    option_filter = OptionFilterProcess(name="OptionFilter", criterion=option_criterion)
    strategy_calculator = StrategyCalculatorProcess(name="StrategyCalculator", strategies=Variables.Strategies)
    valuation_calculator = ValuationCalculatorProcess(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE, combination=valuation_combination)
    valuation_filter = ValuationFilterProcess(name="ValuationFilter", criterion=valuation_criterion)
    valuation_writer = ValuationWriterProcess(name="ValuationWriter", table=acquisition_table, valuation=Variables.Valuations.ARBITRAGE, priority=valuation_priority)
    valuation_reader = ValuationReaderSource(name="ValuationReader", table=acquisition_table, valuation=Variables.Valuations.ARBITRAGE, query=Querys.Contract)
    holding_calculator = HoldingCalculatorProcess(name="HoldingCalculator", valuation=Variables.Valuations.ARBITRAGE)
    holding_saver = HoldingSaverProcess(name="HoldingSaver", file=holding_file, mode="a")

    trading_process = TradingProcess(acquisition_table, discount=arguments["discount"], liquidity=arguments["liquidity"], capacity=arguments["capacity"])
    valuation_process = option_directory + option_filter + strategy_calculator + valuation_calculator + valuation_filter + valuation_writer
    acquisition_process = valuation_reader + holding_calculator + holding_saver
    trading_thread = RepeatingThread(trading_process, name="TradingThread", wait=10).setup(**parameters)
    valuation_thread = RoutineThread(valuation_process, name="ValuationThread").setup(**parameters)
    divestiture_thread = RepeatingThread(acquisition_process, name="DivestitureThread", wait=10).setup(**parameters)

    trading_thread.start()
    divestiture_thread.start()
    valuation_thread.start()
    while True: time.sleep(10)
    valuation_thread.join()
    divestiture_thread.join()
    trading_thread.join()


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
    sysArguments = sysArguments | dict(discount=2.00, liquidity=100, capacity=1)
    main(arguments=sysArguments, parameters=sysParameters)




