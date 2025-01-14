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
from datetime import timedelta as TimeDelta

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
HISTORY = os.path.join(ROOT, "repository", "technical")
if ROOT not in sys.path: sys.path.append(ROOT)

from finance.technicals import StatisticCalculator, HistoryFile
from finance.exposures import ExposureCalculator
from finance.securities import OptionCalculator, OptionBasis
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.prospects import ProspectCalculator, ProspectReader, ProspectWriter, ProspectDiscarding, ProspectProtocols, ProspectTable, ProspectHeader, ProspectLayout
from finance.orders import OrderCalculator
from finance.stability import StabilityCalculator, StabilityFilter
from finance.holdings import HoldingCalculator, HoldingFile
from finance.variables import Variables, Categories, Querys
from support.pipelines import Routine, Producer, Processor, Consumer
from support.files import Loader, Saver, Directory
from support.algorithms import Source, Algorithm
from support.synchronize import RepeatingThread
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


class HoldingDirectorySource(Directory, Source, signature="->contract"): pass
class HoldingLoaderProcess(Loader, Algorithm, signature="contract->holdings"): pass
class ExposureCalculatorProcess(ExposureCalculator, Algorithm, signature="holdings->exposures"): pass
class HistoryLoaderProcess(Loader, Algorithm, query=Querys.Symbol, signature="contract->history"): pass
class StatisticCalculatorProcess(StatisticCalculator, Algorithm, signature="history->statistics"): pass
class OptionCalculatorProcess(OptionCalculator, Algorithm, signature="(exposures,statistics)->options"): pass
class OptionFilterOperation(Filter, Algorithm, signature="options->options"): pass
class StrategyCalculatorProcess(StrategyCalculator, Algorithm, signature="options->strategies", assemble=False): pass
class ValuationCalculatorProcess(ValuationCalculator, Algorithm, signature="strategies->valuations"): pass
class ValuationPivotProcessor(Pivot, Algorithm, signature="valuations->valuations"): pass
class ValuationFilterProcess(Filter, Algorithm, signature="valuations->valuations"): pass
class ProspectCalculatorProcess(ProspectCalculator, Algorithm, signature="valuations->prospects"): pass
class OrderCalculatorProcess(OrderCalculator, Algorithm, signature="prospects->orders"): pass
class StabilityCalculatorProcess(StabilityCalculator, Algorithm, signature="(orders,exposures)->stabilities"): pass
class StabilityFilterProcess(StabilityFilter, Algorithm, signature="(prospects,stabilities)->prospects"): pass
class ProspectUnpivotProcessor(Unpivot, Algorithm, signature="prospects->prospects"): pass
class ProspectWriterProcess(ProspectWriter, Algorithm, signature="prospects->"): pass
class ProspectDiscardingRoutine(ProspectDiscarding, Routine): pass
class ProspectProspectsRoutine(ProspectProtocols, Routine): pass
class ProspectReaderSource(ProspectReader, Producer): pass
class ProspectUnpivotProcessor(Unpivot, Processor): pass
class HoldingCalculatorProcess(HoldingCalculator, Processor): pass
class HoldingSaverProcess(Saver, Consumer): pass

class DivestitureTrading(Naming, fields=["discount", "liquidity", "capacity"]): pass
class DivestitureSizing(Naming, fields=["size", "volume", "interest"]): pass
class DivestitureTiming(Naming, fields=["current", "tenure"]): pass
class DivestitureProfit(Naming, fields=["apy", "cost"]): pass

class OptionAssumptions(Naming, fields=["pricing"], named={"sizing": DivestitureSizing, "timing": DivestitureTiming}): pass
class OptionCriterion(object, named={"sizing": DivestitureSizing}, metaclass=NamingMeta):
    def __iter__(self): return iter([self.interest, self.volume, self.size])

    def interest(self, table): return table["interest"] >= self.sizing.interest
    def volume(self, table): return table["volume"] >= self.sizing.volume
    def size(self, table): return table["size"] >= self.sizing.size

class ValuationCriterion(object, named={"sizing": DivestitureSizing, "profit": DivestitureProfit}, metaclass=NamingMeta):
    def __iter__(self): return iter([self.apy, self.cost, self.size])

    def apy(self, table): return table[("apy", Variables.Scenarios.MINIMUM)] >= self.profit.apy
    def cost(self, table): return table[("cost", Variables.Scenarios.MINIMUM)] <= self.profit.cost
    def size(self, table): return table[("size", "")] >= self.sizing.size

class DivestitureProtocol(Decorator): pass
class DivestitureProtocols(object, named={"trading": DivestitureTrading, "timing": DivestitureTiming}, metaclass=NamingMeta):
    def __iter__(self): return iter([(method, method["status"]) for method in [self.obsolete, self.abandon, self.pursue, self.reject, self.accept]])

    def limited(self, mask): return (mask.cumsum() < self.trading.capacity + 1) & mask
    def timeout(self, table): return (pd.to_datetime(self.timing.current) - table["current"]) >= self.timing.tenure
    def obsolete(self, table): return (table["status"] == Variables.Status.PROSPECT) & self.timeout(table)
    def unattractive(self, table): return table["priority"] < self.trading.discount
    def attractive(self, table): return table["priority"] >= self.trading.discount
    def illiquid(self, table): return table["size"] < self.trading.liquidity
    def liquid(self, table): return table["size"] >= self.trading.liquidity

    @DivestitureProtocol(status=Variables.Status.OBSOLETE)
    def obsolete(self, table): return (table["status"] == Variables.Status.PROSPECT) & self.timeout(table)
    @DivestitureProtocol(status=Variables.Status.ABANDONED)
    def abandon(self, table): return self.limited((table["status"] == Variables.Status.PROSPECT) & self.unattractive(table))
    @DivestitureProtocol(status=Variables.Status.PENDING)
    def pursue(self, table): return self.limited((table["status"] == Variables.Status.PROSPECT) & self.attractive(table))
    @DivestitureProtocol(status=Variables.Status.REJECTED)
    def reject(self, table): return self.limited((table["status"] == Variables.Status.PENDING) & self.illiquid(table))
    @DivestitureProtocol(status=Variables.Status.ACCEPTED)
    def accept(self, table): return self.limited((table["status"] == Variables.Status.PENDING) & self.liquid(table))


def main(*args, arguments={}, parameters={}, **kwargs):
    divestiture_layout = ProspectLayout(name="DivestitureLayout", valuation=Variables.Valuations.ARBITRAGE, rows=100)
    divestiture_header = ProspectHeader(name="DivestitureHeader", valuation=Variables.Valuations.ARBITRAGE)
    divestiture_table = ProspectTable(name="DivestitureTable", layout=divestiture_layout, header=divestiture_header)
    holding_file = HoldingFile(name="HoldingFile", repository=PORTFOLIO)
    history_file = HistoryFile(name="HistoryFile", repository=HISTORY)
    option_basis = OptionBasis(Variables.Pricing.BLACKSCHOLES, arguments["size"], arguments["current"])
    divestiture_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]

    holding_directory = HoldingDirectorySource(name="HoldingDirectory", file=holding_file, mode="r", query=Querys.Contract)
    holding_loader = HoldingLoaderProcess(name="HoldingLoader", file=holding_file, mode="r", query=Querys.Contract)
    history_loader = HistoryLoaderProcess(name="BarsLoader", file=history_file, mode="r", query=Querys.Contract)
    statistic_calculator = StatisticCalculatorProcess(name="StatisticCalculator", query=Querys.Contract)
    exposure_calculator = ExposureCalculatorProcess(name="ExposureCalculator", query=Querys.Contract)
    option_calculator = OptionCalculatorProcess(name="OptionCalculator", basis=option_basis, query=Querys.Contract)
    option_filter = OptionFilterOperation(name="OptionFilter", criterion=, query=Querys.Contract)
    strategy_calculator = StrategyCalculatorProcess(name="StrategyCalculator", strategies=list(Categories.Strategies), query=Querys.Contract)
    valuation_calculator = ValuationCalculatorProcess(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE, query=Querys.Contract)
    valuation_pivot = ValuationPivotProcessor(name="ValuationPivot", header=tuple(divestiture_header.transform), query=Querys.Contract)
    valuation_filter = ValuationFilterProcess(name="ValuationFilter", criterion=, query=Querys.Contract)
    prospect_calculator = ProspectCalculatorProcess(name="ProspectCalculator", header=divestiture_header, priority=divestiture_priority, query=Querys.Contract)
    order_calculator = OrderCalculatorProcess(name="OrderCalculator", query=Querys.Contract)
    stability_calculator = StabilityCalculatorProcess(name="StabilityCalculator", query=Querys.Contract)
    stability_filter = StabilityFilterProcess(name="StabilityFilter", query=Querys.Contract)
    prospect_writer = ProspectWriterProcess(name="ProspectWriter", table=divestiture_table, status=Variables.Status.PROSPECT, query=Querys.Contract)
    prospect_discarding = ProspectDiscardingRoutine(name="ProspectDiscarding", table=divestiture_table, status=[Variables.Status.OBSOLETE, Variables.Status.REJECTED, Variables.Status.ABANDONED], query=Querys.Contract)
    prospect_protocol = ProspectProspectsRoutine(name="ProspectAltering", table=divestiture_table, protocols=, query=Querys.Contract)
    prospect_reader = ProspectReaderSource(name="ProspectReader", table=divestiture_table, status=[Variables.Status.ACCEPTED], query=Querys.Contract)
    prospect_unpivot = ProspectUnpivotProcessor(name="ProspectUnpivot", header=tuple(divestiture_header.transform), query=Querys.Contract)
    holding_calculator = HoldingCalculatorProcess(name="HoldingCalculator", query=Querys.Contract)
    holding_saver = HoldingSaverProcess(name="HoldingSaver", file=holding_file, mode="a", query=Querys.Contract)

    valuation_process = holding_directory + holding_loader + exposure_calculator + history_loader + statistic_calculator + option_calculator + option_filter
    valuation_process = valuation_process + strategy_calculator + valuation_calculator + valuation_pivot + valuation_filter
    valuation_process = valuation_process + prospect_calculator + order_calculator + stability_calculator + stability_filter + prospect_writer
    divestiture_process = prospect_reader + prospect_unpivot + holding_calculator + holding_saver
    valuation_thread = RepeatingThread(valuation_process, name="ValuationThread", wait=10).setup(**parameters)
    discarding_thread = RepeatingThread(prospect_discarding, name="DiscardingThread", wait=10).setup(**parameters)
    protocol_thread = RepeatingThread(prospect_protocol, name="AlteringThread", wait=10).setup(**parameters)
    divestiture_thread = RepeatingThread(divestiture_process, name="AlteringThread", wait=10).setup(**parameters)

    divestiture_thread.start()
    protocol_thread.start()
    discarding_thread.start()
    valuation_thread.start()
    while bool(valuation_thread) or bool(divestiture_table):
        if bool(divestiture_table): print(divestiture_table)
        time.sleep(10)
    discarding_thread.cease()
    protocol_thread.cease()
    divestiture_thread.cease()
    valuation_thread.join()
    discarding_thread.join()
    protocol_thread.join()
    divestiture_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysCurrent = Datetime(year=2024, month=11, day=6, hour=18, minute=0)
    sysTenure = TimeDelta(days=1)
    sysPricing = Variables.Pricing.BLACKSCHOLES
    sysTiming = dict(current=sysCurrent, tenure=sysTenure)
    sysTrading = dict(discount=0.00, liquidity=10, capacity=1)
    sysSizing = dict(size=10, volume=100, interest=100)
    sysProfit = dict(apy=0.00, cost=1000)
    sysParameters = dict(discount=0.00, fees=0.00, period=252)
    sysNamespace = dict(pricing=sysPricing, timing=sysTiming, sizing=sysSizing, profit=sysProfit, trading=sysTrading)
    main(parameters=sysParameters, namespace=sysNamespace)




