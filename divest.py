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
import pandas as pd
import xarray as xr
from functools import update_wrapper
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
HISTORY = os.path.join(ROOT, "repository", "history")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Variables, Querys
from finance.exposures import ExposureCalculator, ExposureWriter, ExposureTable
from finance.securities import OptionCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationWriter, ValuationReader, ValuationTable
from finance.orders import OrderCalculator
from finance.stability import StabilityCalculator, StabilityFilter
from finance.holdings import HoldingCalculator, HoldingFile
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import RepeatingThread
from support.filtering import Filter, Criterion
from support.mixins import AttributeNode

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class Valuation(object):
    def __init__(self, options, exposures, valuations, *args, calculators, filters, **kwargs):
        self.calculators = AttributeNode(calculators)
        self.filters = AttributeNode(filters)
        self.valuations = valuations
        self.exposures = exposures
        self.options = options

    def __call__(self, *args, **kwargs):
        pass


class Divestiture(object):
    def __init__(self, valuations, holdings, *args, calculators, **kwargs):
        self.calculators = AttributeNode(calculators)
        self.valuations = valuations
        self.holdings = holdings

    def __call__(self, *args, **kwargs):
        pass


class Trading(object):
    def __new__(cls, *args, capacity=None, **kwargs):
        instance = super().__new__(cls)
        if capacity is not None:
            pursue = instance.capacity(instance.pursue, capacity)
            setattr(instance, "pursue", pursue)
        return instance

    def __init__(self, table, *args, discount, liquidity, **kwargs):
        self.liquidity = liquidity
        self.discount = discount
        self.table = table

    def __call__(self, *args, **kwargs):
        with self.table.mutex:
            if bool(self.table): print(self.table)
            self.table.change(self.rejected, "status", Variables.Status.REJECTED)
            self.table.change(self.accepted, "status", Variables.Status.ACCEPTED)
            self.table.change(self.abandon, "status", Variables.Status.ABANDONED)
            self.table.change(self.pursue, "status", Variables.Status.PENDING)
            if bool(self.table): print(self.table)

    def pursue(self, table): return (table[:, "status"] == Variables.Status.PROSPECT) & (table[:, "priority"] >= self.discount)
    def abandon(self, table): return (table[:, "status"] == Variables.Status.PROSPECT) & (table[:, "priority"] < self.discount)
    def accepted(self, table): return (table[:, "status"] == Variables.Status.PENDING) & (table[:, "size"] >= self.liquidity)
    def rejected(self, table): return (table[:, "status"] == Variables.Status.PENDING) & (table[:, "size"] < self.liquidity)

    @staticmethod
    def capacity(method, capacity):
        assert capacity is not None
        def wrapper(table): return (method(table).cumsum() < capacity + 1) & method(table)
        update_wrapper(wrapper, method)
        return wrapper


def main(*args, arguments, parameters, **kwargs):
    option_criterion = {Criterion.FLOOR: {"size": arguments["size"], "volume": arguments["volume"], "interest": arguments["interest"]}, Criterion.NULL: ["size", "volume", "interest"]}
    valuation_criterion = {Criterion.FLOOR: {("apy", Variables.Scenarios.MINIMUM): arguments["apy"], "size": arguments["size"]}, Criterion.NULL: [("apy", Variables.Scenarios.MINIMUM), "size"]}
    valuation_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    sizing_functions = {"volume": lambda cols: np.NaN, "size": lambda cols: np.NaN, "interest": lambda cols: np.NaN}
    timing_functions = {"current": lambda cols: np.NaN}
    holding_file = HoldingFile(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    divestiture_table = ValuationTable(name="DivestitureTable")
    exposure_table = ExposureTable(name="ExposureTable")

    holding_loader = Loader(name="HoldingLoader", file=holding_file, query=Querys.Contract, mode="r")
    exposure_calculator = ExposureCalculator(name="ExposureCalculator")
    exposure_writer = ExposureWriter(name="ExposureWriter", table=exposure_table)
    option_calculator = OptionCalculator(name="OptionCalculator", pricing=Variables.Pricing.BLACKSCHOLES, sizings=sizing_functions, timings=timing_functions)
    option_filter = Filter(name="OptionFilter", criterion=option_criterion)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=Variables.Strategies)
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    valuation_filter = Filter(name="ValuationFilter", criterion=valuation_criterion)
    order_calculator = OrderCalculator(name="OrderCalculator")
    stability_calculator = StabilityCalculator(name="StabilityCalculator")
    stability_filter = StabilityFilter(name="StabilityFilter")
    valuation_writer = ValuationWriter(name="ValuationWriter", table=divestiture_table, valuation=Variables.Valuations.ARBITRAGE, priority=valuation_priority)
    valuation_reader = ValuationReader(name="ValuationReader", table=divestiture_table, valuation=Variables.Valuations.ARBITRAGE, query=Querys.Contract)
    holding_calculator = HoldingCalculator(name="HoldingCalculator", valuation=Variables.Valuations.ARBITRAGE)
    holding_saver = Saver(name="HoldingSaver", file=holding_file, mode="a")

    calculators = dict(exposure=exposure_calculator, option=option_calculator, strategy=strategy_calculator, valuation=valuation_calculator, order=order_calculator, stability=stability_calculator, holding=holding_calculator)
    filters = dict(option=option_filter, valuation=valuation_filter, stability=stability_filter)

    valuation_routine = Valuation(holding_loader, exposure_writer, valuation_writer, calculators=calculators, filters=filters)
    acquisition_routine = Divestiture(valuation_reader, holding_saver, calculators=calculators, filters=filters)
    trading_routine = Trading(divestiture_table, discount=arguments["discount"], liquidity=arguments["liquidity"], capacity=arguments["capacity"])
    valuation_thread = RepeatingThread(valuation_routine).setup(**parameters)
    acquisition_thread = RepeatingThread(acquisition_routine, wait=10).setup(**parameters)
    trading_thread = RepeatingThread(trading_routine, wait=10).setup(**parameters)

    trading_thread.start()
    acquisition_thread.start()
    valuation_thread.start()
    while True: time.sleep(10)
    valuation_thread.join()
    acquisition_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysCurrent = Datetime(year=2024, month=10, day=9)
    sysParameters = dict(current=sysCurrent, discount=0.00, fees=0.00)
    sysArguments = dict(apy=0.50, size=10, volume=100, interest=100)
    sysArguments = sysArguments | dict(discount=0.75, liquidity=100, capacity=1)
    main(arguments=sysArguments, parameters=sysParameters)




