# -*- coding: utf-8 -*-
"""
Created on Weds Jun 13 2024
@name:   Divestiture Calculations
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
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
HISTORY = os.path.join(ROOT, "repository", "history")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Variables, Querys
from finance.technicals import TechnicalCalculator, BarsFile
from finance.exposures import ExposureCalculator, ExposureWriter, ExposureTable
from finance.securities import OptionCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationWriter, ValuationReader, ValuationTable
from finance.orders import OrderCalculator
from finance.stability import StabilityCalculator, StabilityFilter
from finance.holdings import HoldingCalculator, HoldingFile
from support.files import Directory, Loader, Saver, FileTypes, FileTimings
from support.synchronize import RepeatingThread
from support.filtering import Filter, Criterion
from support.processes import Source, Process

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class HoldingDirectorySource(Directory, Source, arguments=["contract", "holdings"]): pass
class BarsLoaderProcess(Loader, Process, domain=["contract"], arguments=["bars"]): pass
class StatisticCalculatorProcess(TechnicalCalculator, Process, domain=["contract", "bars"], arguments=["statistics"]): pass
class ExposureCalculatorProcess(ExposureCalculator, Process, domain=["contract", "holdings"], arguments=["exposures"]): pass
class ExposureWriterProcess(ExposureWriter, Process, domain=["contract", "exposures"]): pass
class OptionCalculatorProcess(OptionCalculator, Process, domain=["contract", "exposures", "statistics"], arguments=["options"]): pass
class OptionFilterOperation(Filter, Process, domain=["contract", "options"], arguments=["options"]): pass
class StrategyCalculatorProcess(StrategyCalculator, Process, domain=["contract", "options"], arguments=["strategies"]): pass
class ValuationCalculatorProcess(ValuationCalculator, Process, domain=["contract", "strategies"], arguments=["valuations"]): pass
class ValuationFilterProcess(Filter, Process, domain=["contract", "valuations"], arguments=["valuations"]): pass
class OrderCalculatorProcess(OrderCalculator, Process, domain=["contract", "valuations"], arguments=["orders"]): pass
class StabilityCalculatorProcess(StabilityCalculator, Process, domain=["contract", "orders", "exposures"], arguments=["stabilities"]): pass
class StabilityFilterProcess(StabilityFilter, Process, domain=["contract", "stabilities", "valuations"], arguments=["valuations"]): pass
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
            pursue = self.status(Variables.Status.PROSPECT) & (self.table[:, "priority"] >= self.discount)
            pursue = self.limit(pursue)
            abandon = self.status(Variables.Status.PROSPECT) & (self.table[:, "priority"] < self.discount)
            accepted = self.status(Variables.Status.PENDING) & (self.table[:, "size"] >= self.liquidity)
            rejected = self.status(Variables.Status.PENDING) & (self.table[:, "size"] < self.liquidity)
            self.table.change(rejected, "status", Variables.Status.REJECTED)
            self.table.change(accepted, "status", Variables.Status.ACCEPTED)
            self.table.change(abandon, "status", Variables.Status.ABANDONED)
            self.table.change(pursue, "status", Variables.Status.PENDING)

    def status(self, status): return self.table[:, "status"] == status
    def limit(self, mask): return (mask.cumsum() < self.capacity + 1) & mask


def main(*args, arguments, parameters, **kwargs):
    option_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    option_sizing = lambda cols: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}
    valuation_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    bars_file = BarsFile(name="BarsFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holding_file = HoldingFile(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    divestiture_table = ValuationTable(name="DivestitureTable", valuation=Variables.Valuations.ARBITRAGE)
    exposure_table = ExposureTable(name="ExposureTable")

    holding_directory = HoldingDirectorySource(name="HoldingDirectory", file=holding_file, query=Querys.Contract, mode="r")
    bars_loader = BarsLoaderProcess(name="BarsLoader", file=bars_file, query=Querys.Symbol, mode="r")
    statistic_calculator = StatisticCalculatorProcess(name="StatisticCalculator", technical=Variables.Technicals.STATISTIC)
    exposure_calculator = ExposureCalculatorProcess(name="ExposureCalculator")
    exposure_writer = ExposureWriterProcess(name="ExposureWriter", table=exposure_table)
    option_calculator = OptionCalculatorProcess(name="OptionCalculator", pricing=Variables.Pricing.BLACKSCHOLES, sizing=option_sizing)
    option_filter = OptionFilterOperation(name="OptionFilter", criterion=option_criterion)
    strategy_calculator = StrategyCalculatorProcess(name="StrategyCalculator", strategies=Variables.Strategies)
    valuation_calculator = ValuationCalculatorProcess(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE, combine=pd.concat)
    valuation_filter = ValuationFilterProcess(name="ValuationFilter", criterion=valuation_criterion)
    order_calculator = OrderCalculatorProcess(name="OrderCalculator")
    stability_calculator = StabilityCalculatorProcess(name="StabilityCalculator")
    stability_filter = StabilityFilterProcess(name="StabilityFilter")
    valuation_writer = ValuationWriterProcess(name="ValuationWriter", table=divestiture_table, valuation=Variables.Valuations.ARBITRAGE, priority=valuation_priority)
    valuation_reader = ValuationReaderSource(name="ValuationReader", table=divestiture_table, valuation=Variables.Valuations.ARBITRAGE, query=Querys.Contract)
    holding_calculator = HoldingCalculatorProcess(name="HoldingCalculator", valuation=Variables.Valuations.ARBITRAGE)
    holding_saver = HoldingSaverProcess(name="HoldingSaver", file=holding_file, mode="a")

    trading_process = TradingProcess(divestiture_table, discount=arguments["discount"], liquidity=arguments["liquidity"], capacity=arguments["capacity"])
    valuation_process = holding_directory + bars_loader + statistic_calculator + exposure_calculator
    valuation_process = valuation_process + option_calculator + option_filter + strategy_calculator + valuation_calculator + valuation_filter
    valuation_process = valuation_process + order_calculator + stability_calculator + stability_filter + valuation_writer
    divestiture_process = valuation_reader + holding_calculator + holding_saver
    trading_thread = RepeatingThread(trading_process, name="TradingThread", wait=10).setup(**parameters)
    valuation_thread = RepeatingThread(valuation_process, name="ValuationThread", wait=10).setup(**parameters)
    divestiture_thread = RepeatingThread(divestiture_process, name="DivestitureThread", wait=10).setup(**parameters)

    trading_thread.start()
    divestiture_thread.start()
    valuation_thread.start()
    while True:
        if bool(divestiture_table): print(divestiture_table)
        time.sleep(10)
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
    sysParameters = dict(current=sysCurrent, discount=0.00, fees=0.00, period=252)
    sysArguments = dict(apy=0.00, size=10, volume=100, interest=100)
    sysArguments = sysArguments | dict(discount=0.25, liquidity=25, capacity=2)
    main(arguments=sysArguments, parameters=sysParameters)




