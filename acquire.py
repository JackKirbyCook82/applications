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

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path: sys.path.append(ROOT)

from finance.securities import OptionFile
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.prospects import ProspectCalculator, ProspectReader, ProspectWriter, ProspectDiscarding, ProspectProtocol, ProspectProtocols, ProspectTable, ProspectHeader, ProspectLayout
from finance.holdings import HoldingCalculator, HoldingFile
from finance.variables import Variables, Categories, Querys
from support.pipelines import Routine, Producer, Processor, Consumer
from support.synchronize import RoutineThread, RepeatingThread
from support.files import Loader, Saver, Directory
from support.transforms import Pivot, Unpivot
from support.filters import Filter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class OptionDirectoryProducer(Directory, Producer, query=Querys.Settlement): pass
class OptionLoaderProcessor(Loader, Processor, query=Querys.Settlement): pass
class OptionFilterProcessor(Filter, Processor, query=Querys.Settlement): pass
class StrategyCalculatorProcessor(StrategyCalculator, Processor, query=Querys.Settlement): pass
class ValuationCalculatorProcessor(ValuationCalculator, Processor, query=Querys.Settlement): pass
class ValuationPivotProcessor(Pivot, Processor, query=Querys.Settlement): pass
class ValuationFilterProcessor(Filter, Processor, query=Querys.Settlement): pass
class ProspectCalculatorProcessor(ProspectCalculator, Processor, query=Querys.Settlement): pass
class ProspectWriterConsumer(ProspectWriter, Consumer, query=Querys.Settlement): pass
class ProspectDiscardingRoutine(ProspectDiscarding, Routine, query=Querys.Settlement): pass
class ProspectProtocolsRoutine(ProspectProtocols, Routine, query=Querys.Settlement): pass
class ProspectReaderProducer(ProspectReader, Producer, query=Querys.Settlement): pass
class ProspectUnpivotProcessor(Unpivot, Processor, query=Querys.Settlement): pass
class HoldingCalculatorProcessor(HoldingCalculator, Processor, query=Querys.Settlement): pass
class HoldingSaverConsumer(Saver, Consumer, query=Querys.Settlement): pass

class AcquisitionTrading(ntuple("Trading", "discount liquidity capacity")): pass
class AcquisitionSizing(ntuple("Sizing", "size volume interest")): pass
class AcquisitionTiming(ntuple("Timing", "current tenure")): pass
class AcquisitionAccounting(ntuple("Accounting", "apy cost")): pass
class AcquisitionCodifying(ntuple("Codifying", "trading sizing timing")): pass
class AcquisitionCriterion(ntuple("Criterion", "sizing timing accounting")): pass


class ValuationCriterion(AcquisitionCriterion):
    def apy(self, table): return table[("apy", Variables.Scenarios.MINIMUM)] >= self.accounting.apy
    def cost(self, table): return table[("cost", Variables.Scenarios.MINIMUM)] <= self.accounting.cost
    def size(self, table): return table[("size", "")] >= self.sizing.size

class OptionCriterion(AcquisitionCriterion):
    def current(self, table): return table["current"].dt.date == self.timing.current.date()
    def interest(self, table): return table["interest"] >= self.sizing.interest
    def volume(self, table): return table["volume"] >= self.sizing.volume
    def size(self, table): return table["size"] >= self.sizing.size


class AcquisitionProtocol(ProspectProtocol): pass
class AcquisitionProtocols(AcquisitionCodifying):
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


def main(*args, arguments, parameters, **kwargs):
    acquisition_trading = AcquisitionTrading(**arguments["trading"])
    acquisition_sizing = AcquisitionSizing(**arguments["sizing"])
    acquisition_timing = AcquisitionTiming(**arguments["timing"])
    acquisition_accounting = AcquisitionAccounting(**arguments["accounting"])
    acquisition_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    acquisition_protocols = AcquisitionProtocols(**{"trading": acquisition_trading, "sizing": acquisition_sizing, "accounting": acquisition_accounting})
    valuation_criterion = ValuationCriterion(**{"sizing": acquisition_sizing, "timing": acquisition_timing, "accounting": acquisition_accounting})
    option_criterion = OptionCriterion(**{"sizing": acquisition_sizing, "timing": acquisition_timing, "accounting": acquisition_accounting})
    prospect_protocols = [getattr(acquisition_protocols, attribute) for attribute in ("obsolete", "abandon", "pursue", "reject", "accept")]

    acquisition_layout = ProspectLayout(name="AcquisitionLayout", valuation=Variables.Valuations.ARBITRAGE, rows=100)
    acquisition_header = ProspectHeader(name="AcquisitionHeader", valuation=Variables.Valuations.ARBITRAGE)
    acquisition_table = ProspectTable(name="AcquisitionTable", layout=acquisition_layout, header=acquisition_header)
    holding_file = HoldingFile(name="HoldingFile", repository=PORTFOLIO)
    option_file = OptionFile(name="OptionFile", repository=MARKET)

    option_directory = OptionDirectoryProducer(name="OptionDirectory", file=option_file, mode="r")
    option_loader = OptionLoaderProcessor(name="OptionLoader", file=option_file, mode="r")
    option_filter = OptionFilterProcessor(name="OptionFilter", criterion=[option_criterion.current, option_criterion.size])
    strategy_calculator = StrategyCalculatorProcessor(name="StrategyCalculator", strategies=list(Categories.Strategies))
    valuation_calculator = ValuationCalculatorProcessor(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    valuation_pivot = ValuationPivotProcessor(name="ValuationPivot", header=tuple(acquisition_header.transform))
    valuation_filter = ValuationFilterProcessor(name="ValuationFilter", criterion=[valuation_criterion.apy, valuation_criterion.size])
    prospect_calculator = ProspectCalculatorProcessor(name="ProspectCalculator", header=acquisition_header, priority=acquisition_priority)
    prospect_writer = ProspectWriterConsumer(name="ProspectWriter", table=acquisition_table, status=Variables.Status.PROSPECT)
    prospect_discarding = ProspectDiscardingRoutine(name="ProspectDiscarding", table=acquisition_table, status=[Variables.Status.OBSOLETE, Variables.Status.REJECTED, Variables.Status.ABANDONED])
    prospect_protocol = ProspectProtocolsRoutine(name="ProspectAltering", table=acquisition_table, protocols=list(prospect_protocols))
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
    sysCurrent = Datetime(year=2024, month=11, day=6, hour=18, minute=0)
    sysTenure = TimeDelta(days=1)
    sysTrading = dict(discount=2.00, liquidity=25, capacity=25)
    sysSizing = dict(size=10, volume=100, interest=100)
    sysTiming = dict(current=sysCurrent, tenure=sysTenure)
    sysAccounting = dict(apy=2.00, cost=1000)
    sysArguments = dict(trading=sysTrading, sizing=sysSizing, timing=sysTiming, accounting=sysAccounting)
    sysParameters = dict(discount=0.00, fees=0.00)
    main(arguments=sysArguments, parameters=sysParameters)




