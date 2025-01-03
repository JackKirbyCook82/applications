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

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path: sys.path.append(ROOT)

from finance.securities import OptionFile
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.prospects import ProspectCalculator, ProspectReader, ProspectWriter, ProspectDiscarding, ProspectProtocols, ProspectTable, ProspectHeader, ProspectLayout
from finance.holdings import HoldingCalculator, HoldingFile
from finance.variables import Variables, Querys
from support.pipelines import Routine, Producer, Processor, Consumer
from support.synchronize import RoutineThread, RepeatingThread
from support.files import Loader, Saver, Directory
from support.transforms import Pivot, Unpivot
from support.decorators import Decorator
from support.meta import NamingMeta
from support.filters import Filter
from support.mixins import Naming

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class OptionDirectoryProducer(Directory, Producer, query=Querys.Contract): pass
class OptionLoaderProcessor(Loader, Processor, query=Querys.Contract): pass
class OptionFilterProcessor(Filter, Processor, query=Querys.Contract): pass
class StrategyCalculatorProcessor(StrategyCalculator, Processor): pass
class ValuationCalculatorProcessor(ValuationCalculator, Processor): pass
class ValuationPivotProcessor(Pivot, Processor, query=Querys.Contract): pass
class ValuationFilterProcessor(Filter, Processor, query=Querys.Contract): pass
class ProspectCalculatorProcessor(ProspectCalculator, Processor): pass
class ProspectWriterConsumer(ProspectWriter, Consumer, query=Querys.Contract): pass
class ProspectDiscardingRoutine(ProspectDiscarding, Routine, query=Querys.Contract): pass
class ProspectProtocolsRoutine(ProspectProtocols, Routine, query=Querys.Contract): pass
class ProspectReaderProducer(ProspectReader, Producer, query=Querys.Contract): pass
class ProspectUnpivotProcessor(Unpivot, Processor, query=Querys.Contract): pass
class HoldingCalculatorProcessor(HoldingCalculator, Processor): pass
class HoldingSaverConsumer(Saver, Consumer, query=Querys.Contract): pass

class AcquisitionTrading(Naming, fields=["discount", "liquidity", "capacity"]): pass
class AcquisitionSizing(Naming, fields=["size", "volume", "interest"]): pass
class AcquisitionTiming(Naming, fields=["current", "tenure"]): pass
class AcquisitionProfit(Naming, fields=["apy", "cost"]): pass

class OptionCriterion(object, named={"sizing": AcquisitionSizing, "timing": AcquisitionTiming, "profit": AcquisitionProfit}, metaclass=NamingMeta):
    def __iter__(self): return iter([self.interest, self.volume, self.size, self.date])

    def date(self, table): return table["current"].dt.date == self.timing.current.date()
    def interest(self, table): return table["interest"] >= self.sizing.interest
    def volume(self, table): return table["volume"] >= self.sizing.volume
    def size(self, table): return table["size"] >= self.sizing.size

class ValuationCriterion(object, named={"sizing": AcquisitionSizing, "profit": AcquisitionProfit}, metaclass=NamingMeta):
    def __iter__(self): return iter([self.apy, self.cost, self.size])

    def apy(self, table): return table[("apy", Variables.Scenarios.MINIMUM)] >= self.profit.apy
    def cost(self, table): return table[("cost", Variables.Scenarios.MINIMUM)] <= self.profit.cost
    def size(self, table): return table[("size", "")] >= self.sizing.size

class AcquisitionProtocol(Decorator): pass
class AcquisitionProtocols(object, named={"trading": AcquisitionTrading, "timing": AcquisitionTiming}, metaclass=NamingMeta):
    def __iter__(self): return iter([(method, method["status"]) for method in [self.obsolete, self.abandon, self.pursue, self.reject, self.accept]])

    def limited(self, mask): return (mask.cumsum() < self.trading.capacity + 1) & mask
    def timeout(self, table): return (pd.to_datetime(self.timing.current) - table["current"]) >= self.timing.tenure
    def obsolete(self, table): return (table["status"] == Variables.Status.PROSPECT) & self.timeout(table)
    def unattractive(self, table): return table["priority"] < self.trading.discount
    def attractive(self, table): return table["priority"] >= self.trading.discount
    def illiquid(self, table): return table["size"] < self.trading.liquidity
    def liquid(self, table): return table["size"] >= self.trading.liquidity

    @AcquisitionProtocol(status=Variables.Status.OBSOLETE)
    def obsolete(self, table): return (table["status"] == Variables.Status.PROSPECT) & self.timeout(table)
    @AcquisitionProtocol(status=Variables.Status.ABANDONED)
    def abandon(self, table): return self.limited((table["status"] == Variables.Status.PROSPECT) & self.unattractive(table))
    @AcquisitionProtocol(status=Variables.Status.PENDING)
    def pursue(self, table): return self.limited((table["status"] == Variables.Status.PROSPECT) & self.attractive(table))
    @AcquisitionProtocol(status=Variables.Status.REJECTED)
    def reject(self, table): return self.limited((table["status"] == Variables.Status.PENDING) & self.illiquid(table))
    @AcquisitionProtocol(status=Variables.Status.ACCEPTED)
    def accept(self, table): return self.limited((table["status"] == Variables.Status.PENDING) & self.liquid(table))


def main(*args, parameters={}, namespace={}, **kwargs):
    acquisition_layout = ProspectLayout(name="AcquisitionLayout", valuation=Variables.Valuations.ARBITRAGE, rows=100)
    acquisition_header = ProspectHeader(name="AcquisitionHeader", valuation=Variables.Valuations.ARBITRAGE)
    acquisition_table = ProspectTable(name="AcquisitionTable", layout=acquisition_layout, header=acquisition_header)
    holding_file = HoldingFile(name="HoldingFile", repository=PORTFOLIO)
    option_file = OptionFile(name="OptionFile", repository=MARKET)
    acquisition_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    acquisition_protocols = AcquisitionProtocols(namespace)
    valuation_criterion = ValuationCriterion(namespace)
    option_criterion = OptionCriterion(namespace)

    option_directory = OptionDirectoryProducer(name="OptionDirectory", file=option_file, mode="r")
    option_loader = OptionLoaderProcessor(name="OptionLoader", file=option_file, mode="r")
    option_filter = OptionFilterProcessor(name="OptionFilter", criterion=list(option_criterion))
    strategy_calculator = StrategyCalculatorProcessor(name="StrategyCalculator", strategies=Variables.Strategies)
    valuation_calculator = ValuationCalculatorProcessor(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    valuation_pivot = ValuationPivotProcessor(name="ValuationPivot", header=tuple(acquisition_header.transform))
    valuation_filter = ValuationFilterProcessor(name="ValuationFilter", criterion=list(valuation_criterion))
    prospect_calculator = ProspectCalculatorProcessor(name="ProspectCalculator", header=acquisition_header, priority=acquisition_priority)
    prospect_writer = ProspectWriterConsumer(name="ProspectWriter", table=acquisition_table, status=Variables.Status.PROSPECT)
    prospect_discarding = ProspectDiscardingRoutine(name="ProspectDiscarding", table=acquisition_table, status=[Variables.Status.OBSOLETE, Variables.Status.REJECTED, Variables.Status.ABANDONED])
    prospect_protocol = ProspectProtocolsRoutine(name="ProspectAltering", table=acquisition_table, protocols=dict(acquisition_protocols))
    prospect_reader = ProspectReaderProducer(name="ProspectReader", table=acquisition_table, status=[Variables.Status.ACCEPTED])
    prospect_unpivot = ProspectUnpivotProcessor(name="ProspectUnpivot", header=tuple(acquisition_header.transform))
    holding_calculator = HoldingCalculatorProcessor(name="HoldingCalculator")
    holding_saver = HoldingSaverConsumer(name="HoldingSaver", file=holding_file, mode="a")

    valuation_process = option_directory + option_loader + option_filter + strategy_calculator + valuation_calculator + valuation_pivot + valuation_filter + prospect_calculator + prospect_writer
    acquisition_process = prospect_reader + prospect_unpivot + holding_calculator + holding_saver
    valuation_thread = RoutineThread(valuation_process, name="ValuationThread").setup(**parameters)
    discarding_thread = RepeatingThread(prospect_discarding, name="DiscardingThread", wait=10).setup(**parameters)
    protocol_thread = RepeatingThread(prospect_protocol, name="AlteringThread", wait=10).setup(**parameters)
    acquisition_thread = RepeatingThread(acquisition_process, name="AcquisitionThread", wait=10).setup(**parameters)

    acquisition_thread.start()
    protocol_thread.start()
    discarding_thread.start()
    valuation_thread.start()
    while bool(valuation_thread) or bool(acquisition_table):
        if bool(acquisition_table): print(acquisition_table)
        time.sleep(10)
    discarding_thread.cease()
    protocol_thread.cease()
    acquisition_thread.cease()
    valuation_thread.join()
    discarding_thread.join()
    protocol_thread.join()
    acquisition_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysCurrent = Datetime(year=2024, month=11, day=6, hour=21, minute=0)
    sysTenure = TimeDelta(days=1)
    sysTiming = dict(current=sysCurrent, tenure=sysTenure)
    sysTrading = dict(discount=2.00, liquidity=25, capacity=25)
    sysSizing = dict(size=10, volume=100, interest=100)
    sysProfit = dict(apy=1.00, cost=100000)
    sysParameters = dict(discount=0.00, fees=0.00)
    sysNamespace = dict(timing=sysTiming, sizing=sysSizing, profit=sysProfit, trading=sysTrading)
    main(parameters=sysParameters, namespace=sysNamespace)




