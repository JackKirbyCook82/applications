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
HISTORY = os.path.join(ROOT, "repository", "history")
if ROOT not in sys.path: sys.path.append(ROOT)

from finance.technicals import TechnicalCalculator, HistoryFile
from finance.exposures import ExposureCalculator
from finance.securities import OptionCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.prospects import ProspectCalculator, ProspectReader, ProspectWriter, ProspectDiscarding, ProspectProtocols, ProspectTable, ProspectHeader, ProspectLayout
from finance.orders import OrderCalculator
from finance.stability import StabilityCalculator, StabilityFilter
from finance.holdings import HoldingCalculator, HoldingFile
from finance.variables import Variables, Querys
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


class HoldingDirectorySource(Directory, Source, query=Querys.Contract, signature="->contract"): pass
class HoldingLoaderProcess(Loader, Algorithm, query=Querys.Contract, signature="contract->holdings"): pass
class ExposureCalculatorProcess(ExposureCalculator, Algorithm, signature="holdings->exposures"): pass
class HistoryLoaderProcess(Loader, Algorithm, query=Querys.Symbol, signature="contract->history"): pass
class StatisticCalculatorProcess(TechnicalCalculator, Algorithm, signature="history->statistics"): pass
class OptionCalculatorProcess(OptionCalculator, Algorithm, signature="(exposures,statistics)->options"): pass
class OptionFilterOperation(Filter, Algorithm, query=Querys.Contract, signature="options->options"): pass
class StrategyCalculatorProcess(StrategyCalculator, Algorithm, signature="options->strategies", assemble=False): pass
class ValuationCalculatorProcess(ValuationCalculator, Algorithm, signature="strategies->valuations"): pass
class ValuationPivotProcessor(Pivot, Algorithm, query=Querys.Contract, signature="valuations->valuations"): pass
class ValuationFilterProcess(Filter, Algorithm, query=Querys.Contract, signature="valuations->valuations"): pass
class ProspectCalculatorProcess(ProspectCalculator, Algorithm, signature="valuations->prospects"): pass
class OrderCalculatorProcess(OrderCalculator, Algorithm, signature="prospects->orders"): pass
class StabilityCalculatorProcess(StabilityCalculator, Algorithm, signature="(orders,exposures)->stabilities"): pass
class StabilityFilterProcess(StabilityFilter, Algorithm, signature="(prospects,stabilities)->prospects"): pass
class ProspectUnpivotProcessor(Unpivot, Algorithm, query=Querys.Contract, signature="prospects->prospects"): pass
class ProspectWriterProcess(ProspectWriter, Algorithm, query=Querys.Contract, signature="prospects->"): pass
class ProspectDiscardingRoutine(ProspectDiscarding, Routine, query=Querys.Contract): pass
class ProspectProspectsRoutine(ProspectProtocols, Routine, query=Querys.Contract): pass
class ProspectReaderSource(ProspectReader, Producer, query=Querys.Contract): pass
class ProspectUnpivotProcessor(Unpivot, Processor, query=Querys.Contract): pass
class HoldingCalculatorProcess(HoldingCalculator, Processor): pass
class HoldingSaverProcess(Saver, Consumer, query=Querys.Contract): pass

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


def main(*args, parameters={}, namespace={}, **kwargs):
    divestiture_layout = ProspectLayout(name="DivestitureLayout", valuation=Variables.Valuations.ARBITRAGE, rows=100)
    divestiture_header = ProspectHeader(name="DivestitureHeader", valuation=Variables.Valuations.ARBITRAGE)
    divestiture_table = ProspectTable(name="DivestitureTable", layout=divestiture_layout, header=divestiture_header)
    holding_file = HoldingFile(name="HoldingFile", repository=PORTFOLIO)
    history_file = HistoryFile(name="HistoryFile", repository=HISTORY)
    divestiture_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    divestiture_protocols = DivestitureProtocols(namespace)
    valuation_criterion = ValuationCriterion(namespace)
    option_criterion = OptionCriterion(namespace)
    option_assumptions = OptionAssumptions(namespace)

    holding_directory = HoldingDirectorySource(name="HoldingDirectory", file=holding_file, mode="r")
    holding_loader = HoldingLoaderProcess(name="HoldingLoader", file=holding_file, mode="r")
    history_loader = HistoryLoaderProcess(name="BarsLoader", file=history_file, mode="r")
    statistic_calculator = StatisticCalculatorProcess(name="StatisticCalculator", technical=Variables.Technicals.STATISTIC)
    exposure_calculator = ExposureCalculatorProcess(name="ExposureCalculator")
    option_calculator = OptionCalculatorProcess(name="OptionCalculator", assumptions=option_assumptions)
    option_filter = OptionFilterOperation(name="OptionFilter", criterion=list(option_criterion))
    strategy_calculator = StrategyCalculatorProcess(name="StrategyCalculator", strategies=Variables.Strategies)
    valuation_calculator = ValuationCalculatorProcess(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    valuation_pivot = ValuationPivotProcessor(name="ValuationPivot", header=tuple(divestiture_header.transform))
    valuation_filter = ValuationFilterProcess(name="ValuationFilter", criterion=list(valuation_criterion))
    prospect_calculator = ProspectCalculatorProcess(name="ProspectCalculator", header=divestiture_header, priority=divestiture_priority)
    order_calculator = OrderCalculatorProcess(name="OrderCalculator")
    stability_calculator = StabilityCalculatorProcess(name="StabilityCalculator")
    stability_filter = StabilityFilterProcess(name="StabilityFilter")
    prospect_writer = ProspectWriterProcess(name="ProspectWriter", table=divestiture_table, status=Variables.Status.PROSPECT)
    prospect_discarding = ProspectDiscardingRoutine(name="ProspectDiscarding", table=divestiture_table, status=[Variables.Status.OBSOLETE, Variables.Status.REJECTED, Variables.Status.ABANDONED])
    prospect_protocol = ProspectProspectsRoutine(name="ProspectAltering", table=divestiture_table, protocols=dict(divestiture_protocols))
    prospect_reader = ProspectReaderSource(name="ProspectReader", table=divestiture_table, status=[Variables.Status.ACCEPTED])
    prospect_unpivot = ProspectUnpivotProcessor(name="ProspectUnpivot", header=tuple(divestiture_header.transform))
    holding_calculator = HoldingCalculatorProcess(name="HoldingCalculator")
    holding_saver = HoldingSaverProcess(name="HoldingSaver", file=holding_file, mode="a")

    valuations_process = holding_directory + holding_loader + exposure_calculator + history_loader + statistic_calculator + option_calculator + option_filter
    valuations_process = valuations_process + strategy_calculator + valuation_calculator + valuation_pivot + valuation_filter
    valuations_process = valuations_process + prospect_calculator + order_calculator + stability_calculator + stability_filter + prospect_writer
    divestiture_process = prospect_reader + prospect_unpivot + holding_calculator + holding_saver
    valuations_thread = RepeatingThread(valuations_process, name="ValuationThread", wait=10).setup(**parameters)
    discarding_thread = RepeatingThread(prospect_discarding, name="DiscardingThread", wait=10).setup(**parameters)
    protocol_thread = RepeatingThread(prospect_protocol, name="AlteringThread", wait=10).setup(**parameters)
    divestiture_thread = RepeatingThread(divestiture_process, name="AlteringThread", wait=10).setup(**parameters)

    valuations_thread.start()
    while True:
        if bool(divestiture_table): print(divestiture_table)
        time.sleep(10)
    valuations_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysCurrent = Datetime(year=2024, month=11, day=6, hour=21, minute=0)
    sysTenure = TimeDelta(days=1)
    sysPricing = Variables.Pricing.BLACKSCHOLES
    sysTiming = dict(current=sysCurrent, tenure=sysTenure)
    sysTrading = dict(discount=0.00, liquidity=25, capacity=1)
    sysSizing = dict(size=10, volume=100, interest=100)
    sysProfit = dict(apy=0.00, cost=0)
    sysParameters = dict(discount=0.00, fees=0.00, period=252)
    sysNamespace = dict(pricing=sysPricing, timing=sysTiming, sizing=sysSizing, profit=sysProfit, trading=sysTrading)
    main(parameters=sysParameters, namespace=sysNamespace)




