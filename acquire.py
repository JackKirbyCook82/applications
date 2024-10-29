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
import pandas as pd
import xarray as xr
from functools import update_wrapper
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
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import RoutineThread, RepeatingThread
from support.filtering import Filter, Criterion
from support.mixins import AttributeNode

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class Valuation(object):
    def __init__(self, options, valuations, *args, calculators, filters, **kwargs):
        self.calculators = AttributeNode(calculators)
        self.filters = AttributeNode(filters)
        self.valuation = valuations
        self.options = options

    def __call__(self, *args, **kwargs):
        for (contract, options) in self.options(*args, **kwargs):
            source = (contract, options)
            options = self.filters.option(source, *args, **kwargs)
            if options is None: continue
            source = (contract, options)
            for strategies in self.calculators.strategy(source, *args, **kwargs):
                source = (contract, strategies)
                for valuations in self.calculators.valuation(source, *args, **kwargs):
                    source = (contract, valuations)
                    valuations = self.filters.valuation(source, *args, **kwargs)
                    if valuations is None: continue
                    source = (contract, valuations)
                    self.valuations(source, *args, **kwargs)


class Acquisition(object):
    def __init__(self, valuations, holdings, *args, calculators, **kwargs):
        self.calculators = AttributeNode(calculators)
        self.valuations = valuations
        self.holdings = holdings

    def __call__(self, *args, **kwargs):
        for (scope, valuations) in self.valuations(*args, **kwargs):
            contract = Querys.Contract(scope)
            source = (contract, valuations)
            holdings = self.calculators.holding(source, *args, **kwargs)
            source = (contract, holdings)
            self.holdings(source, *args, **kwargs)


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
    option_file = OptionFile(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    holding_file = HoldingFile(name="HoldingFile", repository=PORTFOLIO, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    acquisition_table = ValuationTable(name="AcquisitionTable", valuation=Variables.Valuations.ARBITRAGE)

    option_loader = Loader(name="OptionLoader", file=option_file, query=Querys.Contract, mode="r")
    option_filter = Filter(name="OptionFilter", criterion=option_criterion)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=Variables.Strategies)
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    valuation_filter = Filter(name="ValuationFilter", criterion=valuation_criterion)
    valuation_writer = ValuationWriter(name="ValuationWriter", table=acquisition_table, valuation=Variables.Valuations.ARBITRAGE, priority=valuation_priority)
    valuation_reader = ValuationReader(name="ValuationReader", table=acquisition_table, valuation=Variables.Valuations.ARBITRAGE, query=Querys.Contract)
    holding_calculator = HoldingCalculator(name="HoldingCalculator", valuation=Variables.Valuations.ARBITRAGE)
    holding_saver = Saver(name="HoldingSaver", file=holding_file, mode="w")

    calculators = dict(strategy=strategy_calculator, valuation=valuation_calculator, holding=holding_calculator)
    filters = dict(option=option_filter, valuation=valuation_filter)

    valuation_routine = Valuation(option_loader, valuation_writer, calculators=calculators, filters=filters)
    acquisition_routine = Acquisition(valuation_reader, holding_saver, calculators=calculators, filters=filters)
    trading_routine = Trading(acquisition_table, discount=arguments["discount"], liquidity=arguments["liquidity"], capacity=arguments["capacity"])
    valuation_thread = RoutineThread(valuation_routine).setup(**parameters)
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




