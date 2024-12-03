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
from datetime import timedelta as TimeDelta
from collections import namedtuple as ntuple
from collections import OrderedDict as ODict

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
from finance.prospects import ProspectCalculator, ProspectReader, ProspectDiscarding, ProspectAltering, ProspectWriter, ProspectTable, ProspectHeader, ProspectLayout
from finance.holdings import HoldingCalculator, HoldingFile
from support.pipelines import Routine, Producer, Processor, Consumer
from support.files import Directory, Saver, FileTypes, FileTimings
from support.synchronize import RoutineThread, RepeatingThread
from support.filtering import Filter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class OptionDirectoryProducer(Directory, Producer, query=Querys.Contract): pass
class OptionFilterProcessor(Filter, Processor, query=Querys.Contract): pass
class StrategyCalculatorProcessor(StrategyCalculator, Processor): pass
class ValuationCalculatorProcessor(ValuationCalculator, Processor): pass
class ValuationFilterProcessor(Filter, Processor, query=Querys.Contract): pass
class ProspectCalculatorProcessor(ProspectCalculator, Processor): pass
class ProspectWriterConsumer(ProspectWriter, Consumer, query=Querys.Contract): pass
class ProspectDiscardingRoutine(ProspectDiscarding, Routine, query=Querys.Contract): pass
class ProspectAlteringRoutine(ProspectAltering, Routine, query=Querys.Contract): pass
class ProspectReaderProducer(ProspectReader, Producer, query=Querys.Contract): pass
class HoldingCalculatorProcessor(HoldingCalculator, Processor): pass
class HoldingSaverConsumer(Saver, Consumer, query=Querys.Contract): pass


class AcquisitionCriterion(ntuple("Criterion", "sizing profit")):
    Sizing = ntuple("Sizing", "size volume interest")
    Profit = ntuple("Profit", "apy cost")

    def __new__(cls, *args, sizing, profit, **kwargs):
        sizing = cls.Sizing(*[sizing[field] for field in cls.Sizing._fields])
        profit = cls.Profit(*[profit[field] for field in cls.Profit._fields])
        return super().__new__(cls, sizing, profit)

    def options(self, table): return self.interest(table) & self.volume(table) & self.size(table)
    def valuations(self, table): return self.apy(table) & self.cost(table) & self.size(table)
    def apy(self, table): return table[("apy", Variables.Scenarios.MINIMUM)] >= self.profit.apy
    def cost(self, table): return table["cost"] <= self.profit.cost
    def interest(self, table): return table["interest"] >= self.sizing.interest
    def volume(self, table): return table["volume"] >= self.sizing.volume
    def size(self, table): return table["size"] >= self.sizing.size


class AcquisitionProtocol(ntuple("Protocol", "trading timing")):
    Trading = ntuple("Trading", "discount liquidity capacity")
    Timing = ntuple("Timing", "current tenure")

    def __new__(cls, *args, trading, timing, **kwargs):
        trading = cls.Sizing(*[trading[field] for field in cls.Trading._fields])
        timing = cls.Profit(*[timing[field] for field in cls.Timing._fields])
        return super().__new__(cls, trading, timing)

    def __iter__(self): return iter(self.functions.values())
    def __getitem__(self, name): return self.functions[name]
    def __init__(self, *args, **kwargs):
        Protocol = ntuple("Protocol", "name variable function")
        pursue = Protocol("pursue", Variables.Status.PROSPECT, lambda table: self.limited(self.pursue(table)))
        abandon = Protocol("abandon", Variables.Status.ABANDON, lambda table: self.abandon(table))
        reject = Protocol("reject", Variables.Status.REJECT, lambda table: self.reject(table))
        accept = Protocol("accept", Variables.Status.ACCEPT, lambda table: self.accept(table))
        self.functions = ODict([(protocol.name, protocol) for protocol in  (pursue, abandon, reject, accept)])

    def unattractive(self, table): return table["priority"] < self.trading.discount
    def attractive(self, table): return table["priority"] >= self.trading.discount
    def illiquid(self, table): return table["size"] < self.trading.liquidity
    def liquid(self, table): return table["size"] < self.trading.liquidity
    def pursue(self, table): return table["status"] == Variables.Status.PROSPECT & self.attractive(table)
    def abandon(self, table): return table["status"] == Variables.Status.PROSPECT & self.unattractive(table)
    def accept(self, table): return Variables.Status.PENDING & self.liquid(table)
    def reject(self, table): return Variables.Status.PENDING & self.illiquid(table)
    def limited(self, mask): return (mask.cumsum() < self.trading.capacity + 1) & mask


def main(*args, arguments={}, parameters={}, **kwargs):
    option_file = OptionFile(name="OptionFile", filetype=FileTypes.CSV, filetiming=FileTimings.EAGER, repository=MARKET)
    holding_file = HoldingFile(name="HoldingFile", filetype=FileTypes.CSV, filetiming=FileTimings.EAGER, repository=PORTFOLIO)
    acquisition_layout = ProspectLayout(name="AcquisitionLayout", valuation=Variables.Valuations.ARBITRAGE, rows=100)
    acquisition_header = ProspectHeader(name="AcquisitionHeader", valuation=Variables.Valuations.ARBITRAGE)
    acquisition_table = ProspectTable(name="AcquisitionTable", layout=acquisition_layout, header=acquisition_header)
    prospect_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    acquisition_criterion = AcquisitionCriterion(**arguments)
    acquisition_protocol = AcquisitionProtocol(**arguments)

    option_directory = OptionDirectoryProducer(name="OptionDirectory", file=option_file, mode="r")
    option_filter = OptionFilterProcessor(name="OptionFilter", criterion=acquisition_criterion.options)
    strategy_calculator = StrategyCalculatorProcessor(name="StrategyCalculator", strategies=Variables.Strategies)
    valuation_calculator = ValuationCalculatorProcessor(name="ValuationCalculator", header=acquisition_header)
    valuation_filter = ValuationFilterProcessor(name="ValuationFilter", criterion=acquisition_criterion.valuations)
    prospect_calculator = ProspectCalculatorProcessor(name="ProspectCalculator", header=acquisition_header, priority=prospect_priority)
    prospect_writer = ProspectWriterConsumer(name="ProspectWriter", table=acquisition_table, status=Variables.Status.PROSPECT)
    prospect_discarding = ProspectDiscardingRoutine(name="ProspectDiscarding", table=acquisition_table, status=[Variables.Status.OBSOLETE, Variables.Status.REJECTED, Variables.Status.ABANDONED])
    prospect_altering = ProspectAlteringRoutine(name="ProspectAltering", table=acquisition_table, protocol=acquisition_protocol["pursue"])
    prospect_reader = ProspectReaderProducer(name="ProspectReader", table=acquisition_table, status=[Variables.Status.ACCEPTED])
    holding_calculator = HoldingCalculatorProcessor(name="HoldingCalculator", header=acquisition_header)
    holding_saver = HoldingSaverConsumer(name="HoldingSaver", file=holding_file, mode="a")

    valuation_process = option_directory + option_filter + strategy_calculator + valuation_calculator + valuation_filter + prospect_calculator + prospect_writer
    acquisition_process = prospect_reader + holding_calculator + holding_saver
    valuation_thread = RoutineThread(valuation_process, name="ValuationThread").setup(**parameters)
    discarding_thread = RepeatingThread(prospect_discarding, name="DiscardingThread", wait=10).setup(**parameters)
    altering_thread = RepeatingThread(prospect_altering, name="AlteringThread", wait=10).setup(**parameters)
    acquisition_thread = RepeatingThread(acquisition_process, name="AcquisitionThread", wait=10).setup(**parameters)

#    acquisition_thread.start()
#    altering_thread.start()
#    discarding_thread.start()
    valuation_thread.start()
    while bool(valuation_thread) or bool(acquisition_table):
        if bool(acquisition_table): print(acquisition_table)
        time.sleep(10)
    valuation_thread.join()
#    discarding_thread.join()
#    altering_thread.join()
#    acquisition_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysCurrent = Datetime(year=2024, month=11, day=6)
    sysTenure = TimeDelta(days=1)
    sysTiming = dict(current=sysCurrent, tenure=sysTenure)
    sysTrading = dict(discount=2.00, liquidity=25, capacity=10)
    sysSizing = dict(size=10, volume=100, interest=100)
    sysProfit = dict(apy=1.00, cost=100000)
    sysArguments = dict(timing=sysTiming, trading=sysTrading, sizing=sysSizing, profit=sysProfit)
    sysParameters = dict(current=sysCurrent, discount=0.00, fees=0.00)
    main(arguments=sysArguments, parameters=sysParameters)




