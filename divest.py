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
from support.filtering import Filter, Criterion
from support.synchronize import RepeatingThread
from support.mixins import AttributeNode

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class Valuation(object):
    def __init__(self, holdings, bars, exposures, valuations, *args, calculators, filters, **kwargs):
        self.calculators = AttributeNode(calculators)
        self.filters = AttributeNode(filters)
        self.valuations = valuations
        self.exposures = exposures
        self.holdings = holdings
        self.bars = bars

    def __call__(self, *args, **kwargs):
        for contract, holdings in self.holdings(*args, **kwargs):
            bars = self.bars(contract, *args, **kwargs)
            assert bars is not None
            statistics = self.calculators.statistics(contract, bars, *args, **kwargs)
            exposures = self.calculators.exposures(contract, holdings, *args, **kwargs)
            self.exposures(contract, exposures, *args, **kwargs)
            options = self.calculators.options(contract, exposures, statistics, *args, **kwargs)
            options = self.filters.options(contract, options, *args, **kwargs)
            if options is None: continue
            for strategies in self.calculators.strategies(contract, options, *args, **kwargs):
                for valuations in self.calculators.valuations(contract, strategies, *args, **kwargs):
                    valuations = self.filters.valuations(contract, valuations, *args, **kwargs)
                    if valuations is None: continue
                    orders = self.calculators.orders(contract, valuations, *args, **kwargs)
                    stabilities = self.calculators.stabilities(contract, orders, exposures, *args, **kwargs)
                    stabilities = self.filters.stablities(contract, valuations, stabilities, *args, **kwargs)
                    if not stabilities: continue
                    self.valuations(contract, valuations, *args, **kwargs)


class Divestiture(object):
    def __init__(self, valuations, holdings, *args, calculators, **kwargs):
        self.calculators = AttributeNode(calculators)
        self.valuations = valuations
        self.holdings = holdings

    def __call__(self, *args, **kwargs):
        for contract, valuations in self.valuations(*args, **kwargs):
            holdings = self.calculators.holdings(contract, valuations, *args, **kwargs)
            self.holdings(contract, holdings, *args, **kwargs)


class Trading(object):
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
    sizing_functions = {"volume": lambda cols: np.NaN, "size": lambda cols: np.NaN, "interest": lambda cols: np.NaN}
    timing_functions = {"current": lambda cols: np.NaN}
    bars_file = BarsFile(name="BarsFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holding_file = HoldingFile(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    divestiture_table = ValuationTable(name="DivestitureTable", valuation=Variables.Valuations.ARBITRAGE)
    exposure_table = ExposureTable(name="ExposureTable")

    bars_loader = Loader(name="BarsLoader", file=bars_file, query=Querys.Symbol, mode="r")
    statistic_calculator = TechnicalCalculator(name="StatisticCalculator", technical=Variables.Technicals.STATISTIC)
    holding_loader = Directory(name="HoldingLoader", file=holding_file, query=Querys.Contract, mode="r")
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

    calculators = dict(statistics=statistic_calculator, holdings=holding_calculator, orders=order_calculator, stabilities=stability_calculator)
    calculators = calculators | dict(exposures=exposure_calculator, options=option_calculator, strategies=strategy_calculator, valuations=valuation_calculator)
    filters = dict(options=option_filter, valuations=valuation_filter, stabilities=stability_filter)

    valuation_routine = Valuation(holding_loader, bars_loader, exposure_writer, valuation_writer, calculators=calculators, filters=filters)
    acquisition_routine = Divestiture(valuation_reader, holding_saver, calculators=calculators, filters=filters)
    trading_routine = Trading(divestiture_table, discount=arguments["discount"], liquidity=arguments["liquidity"], capacity=arguments["capacity"])
    valuation_thread = RepeatingThread(valuation_routine).setup(**parameters)
    acquisition_thread = RepeatingThread(acquisition_routine, wait=10).setup(**parameters)
    trading_thread = RepeatingThread(trading_routine, wait=10).setup(**parameters)

    valuation_thread.start()
    valuation_thread.join()


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




