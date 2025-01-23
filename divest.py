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
from collections import namedtuple as ntuple

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
HISTORY = os.path.join(ROOT, "repository", "technical")
if ROOT not in sys.path: sys.path.append(ROOT)

from finance.technicals import StatisticCalculator, HistoryFile
from finance.exposures import ExposureCalculator
from finance.securities import OptionCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.prospects import ProspectCalculator, ProspectReader, ProspectWriter, ProspectDiscarding, ProspectProtocol, ProspectProtocols, ProspectTable, ProspectHeader, ProspectLayout
from finance.orders import OrderCalculator
from finance.stability import StabilityCalculator, StabilityFilter
from finance.holdings import HoldingCalculator, HoldingFile
from finance.variables import Variables, Categories, Querys
from support.pipelines import Routine, Producer, Processor, Consumer
from support.files import Loader, Saver, Directory
from support.algorithms import Source, Algorithm
from support.synchronize import RepeatingThread
from support.transforms import Pivot, Unpivot
from support.filters import Filter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class HoldingDirectorySource(Directory, Source, query=Querys.Settlement, signature="->contract"): pass
class HoldingLoaderProcess(Loader, Algorithm, query=Querys.Settlement, signature="contract->holdings"): pass
class ExposureCalculatorProcess(ExposureCalculator, Algorithm, query=Querys.Settlement, signature="holdings->exposures"): pass
class HistoryLoaderProcess(Loader, Algorithm, query=Querys.Symbol, signature="contract->history"): pass
class StatisticCalculatorProcess(StatisticCalculator, Algorithm, query=Querys.Settlement, signature="history->statistics"): pass
class OptionCalculatorProcess(OptionCalculator, Algorithm, query=Querys.Settlement, signature="(exposures,statistics)->options"): pass
class OptionFilterOperation(Filter, Algorithm, query=Querys.Settlement, signature="options->options"): pass
class StrategyCalculatorProcess(StrategyCalculator, Algorithm, query=Querys.Settlement, signature="options->strategies", assemble=False): pass
class ValuationCalculatorProcess(ValuationCalculator, Algorithm, query=Querys.Settlement, signature="strategies->valuations"): pass
class ValuationPivotProcessor(Pivot, Algorithm, query=Querys.Settlement, signature="valuations->valuations"): pass
class ValuationFilterProcess(Filter, Algorithm, query=Querys.Settlement, signature="valuations->valuations"): pass
class ProspectCalculatorProcess(ProspectCalculator, Algorithm, query=Querys.Settlement, signature="valuations->prospects"): pass
class OrderCalculatorProcess(OrderCalculator, Algorithm, query=Querys.Settlement, signature="prospects->orders"): pass
class StabilityCalculatorProcess(StabilityCalculator, Algorithm, query=Querys.Settlement, signature="(orders,exposures)->stabilities"): pass
class StabilityFilterProcess(StabilityFilter, Algorithm, query=Querys.Settlement, signature="(prospects,stabilities)->prospects"): pass
class ProspectUnpivotProcessor(Unpivot, Algorithm, query=Querys.Settlement, signature="prospects->prospects"): pass
class ProspectWriterProcess(ProspectWriter, Algorithm, query=Querys.Settlement, signature="prospects->"): pass
class ProspectDiscardingRoutine(ProspectDiscarding, Routine, query=Querys.Settlement): pass
class ProspectProspectsRoutine(ProspectProtocols, Routine, query=Querys.Settlement): pass
class ProspectReaderSource(ProspectReader, Producer, query=Querys.Settlement): pass
class ProspectUnpivotProcessor(Unpivot, Processor, query=Querys.Settlement): pass
class HoldingCalculatorProcess(HoldingCalculator, Processor, query=Querys.Settlement): pass
class HoldingSaverProcess(Saver, Consumer, query=Querys.Settlement): pass

class DivestitureTrading(ntuple("Trading", "discount liquidity capacity")): pass
class DivestitureSizing(ntuple("Sizing", "size volume interest")): pass
class DivestitureTiming(ntuple("Timing", "current tenure")): pass
class DivestitureAccounting(ntuple("Accounting", "apy cost")): pass
class DivestitureCodifying(ntuple("Codifying", "trading sizing timing")): pass
class DivestitureCriterion(ntuple("Criterion", "sizing timing accounting")): pass
class DivestitureCalculating(ntuple("Calculating", "pricing sizing timing")): pass


class ValuationCriterion(DivestitureCriterion):
    def apy(self, table): return table[("apy", Variables.Scenarios.MINIMUM)] >= self.profit.apy
    def cost(self, table): return table[("cost", Variables.Scenarios.MINIMUM)] <= self.profit.cost
    def size(self, table): return table[("size", "")] >= self.sizing.size

class OptionCriterion(DivestitureCriterion):
    def interest(self, table): return table["interest"] >= self.sizing.interest
    def volume(self, table): return table["volume"] >= self.sizing.volume
    def size(self, table): return table["size"] >= self.sizing.size


class DivestitureProtocol(ProspectProtocol): pass
class DivestitureProtocols(DivestitureCodifying):
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


def main(*args, arguments, parameters, **kwargs):
    divestiture_trading = DivestitureTrading(**arguments["trading"])
    divestiture_sizing = DivestitureSizing(**arguments["sizing"])
    divestiture_timing = DivestitureTiming(**arguments["timing"])
    divestiture_accounting = DivestitureAccounting(**arguments["accounting"])
    divestiture_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    divestiture_calculating = DivestitureCalculating(Variables.Pricing.BLACKSCHOLES, divestiture_sizing, divestiture_timing)
    divestiture_protocols = DivestitureProtocols(**{"trading": divestiture_trading, "sizing": divestiture_sizing, "accounting": divestiture_accounting})
    valuation_criterion = ValuationCriterion(**{"sizing": divestiture_sizing, "timing": divestiture_timing, "accounting": divestiture_accounting})
    option_criterion = OptionCriterion(**{"sizing": divestiture_sizing, "timing": divestiture_timing, "accounting": divestiture_accounting})
    prospect_protocols = [getattr(divestiture_protocols, attribute) for attribute in ("obsolete", "abandon", "pursue", "reject", "accept")]

    divestiture_layout = ProspectLayout(name="DivestitureLayout", valuation=Variables.Valuations.ARBITRAGE, rows=100)
    divestiture_header = ProspectHeader(name="DivestitureHeader", valuation=Variables.Valuations.ARBITRAGE)
    divestiture_table = ProspectTable(name="DivestitureTable", layout=divestiture_layout, header=divestiture_header)
    holding_file = HoldingFile(name="HoldingFile", repository=PORTFOLIO)
    history_file = HistoryFile(name="HistoryFile", repository=HISTORY)

    holding_directory = HoldingDirectorySource(name="HoldingDirectory", file=holding_file, mode="r")
    holding_loader = HoldingLoaderProcess(name="HoldingLoader", file=holding_file, mode="r")
    history_loader = HistoryLoaderProcess(name="BarsLoader", file=history_file, mode="r")
    statistic_calculator = StatisticCalculatorProcess(name="StatisticCalculator")
    exposure_calculator = ExposureCalculatorProcess(name="ExposureCalculator")
    option_calculator = OptionCalculatorProcess(name="OptionCalculator", calculation=divestiture_calculating)
    option_filter = OptionFilterOperation(name="OptionFilter", criterion=[option_criterion.size])
    strategy_calculator = StrategyCalculatorProcess(name="StrategyCalculator", strategies=list(Categories.Strategies))
    valuation_calculator = ValuationCalculatorProcess(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    valuation_pivot = ValuationPivotProcessor(name="ValuationPivot", header=tuple(divestiture_header.transform))
    valuation_filter = ValuationFilterProcess(name="ValuationFilter", criterion=[valuation_criterion.apy, valuation_criterion.size])
    prospect_calculator = ProspectCalculatorProcess(name="ProspectCalculator", header=divestiture_header, priority=divestiture_priority)
    order_calculator = OrderCalculatorProcess(name="OrderCalculator")
    stability_calculator = StabilityCalculatorProcess(name="StabilityCalculator")
    stability_filter = StabilityFilterProcess(name="StabilityFilter")
    prospect_writer = ProspectWriterProcess(name="ProspectWriter", table=divestiture_table, status=Variables.Status.PROSPECT)
    prospect_discarding = ProspectDiscardingRoutine(name="ProspectDiscarding", table=divestiture_table, status=[Variables.Status.OBSOLETE, Variables.Status.REJECTED, Variables.Status.ABANDONED])
    prospect_protocol = ProspectProspectsRoutine(name="ProspectAltering", table=divestiture_table, protocols=list(prospect_protocols))
    prospect_reader = ProspectReaderSource(name="ProspectReader", table=divestiture_table, status=[Variables.Status.ACCEPTED])
    prospect_unpivot = ProspectUnpivotProcessor(name="ProspectUnpivot", header=tuple(divestiture_header.transform))
    holding_calculator = HoldingCalculatorProcess(name="HoldingCalculator")
    holding_saver = HoldingSaverProcess(name="HoldingSaver", file=holding_file, mode="a")

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
    sysTrading = dict(discount=0.00, liquidity=10, capacity=1)
    sysSizing = dict(size=10, volume=100, interest=100)
    sysTiming = dict(current=sysCurrent, tenure=sysTenure)
    sysAccounting = dict(apy=0.00, cost=1000)
    sysArguments = dict(trading=sysTrading, sizing=sysSizing, timing=sysTiming, accounting=sysAccounting)
    sysParameters = dict(discount=0.00, fees=0.00, period=252)
    main(arguments=sysArguments, parameters=sysParameters)




