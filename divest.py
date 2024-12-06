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
from datetime import date as Date
from datetime import datetime as Datetime
from datetime import timedelta as TimeDelta

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
HISTORY = os.path.join(ROOT, "repository", "history")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Variables, Querys
from finance.technicals import TechnicalCalculator, BarsFile
from finance.exposures import ExposureCalculator
from finance.securities import OptionCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator
from finance.prospects import ProspectCalculator, ProspectReader, ProspectWriter, ProspectDiscarding, ProspectAltering, ProspectTable, ProspectHeader, ProspectLayout, ProspectProtocols, ProspectProtocol
from finance.orders import OrderCalculator
from finance.stability import StabilityCalculator, StabilityFilter
from finance.holdings import HoldingCalculator, HoldingFile
from support.files import Directory, Loader, Saver, FileTypes, FileTimings
from support.operations import Filter, Pivot, Unpivot
from support.synchronize import RepeatingThread
from support.meta import NamedMeta

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class HoldingDirectorySource(Directory, query=Querys.Contract): pass
class BarsLoaderProcess(Loader, query=Querys.Symbol): pass
class ExposureCalculatorProcess(ExposureCalculator): pass
class StatisticCalculatorProcess(TechnicalCalculator): pass
class OptionCalculatorProcess(OptionCalculator): pass
class OptionFilterOperation(Filter, query=Querys.Contract): pass
class StrategyCalculatorProcess(StrategyCalculator): pass
class ValuationCalculatorProcess(ValuationCalculator): pass
class ValuationPivotProcessor(Pivot, query=Querys.Contract): pass
class ValuationFilterProcess(Filter, query=Querys.Contract): pass
class ProspectCalculatorProcess(ProspectCalculator): pass
class OrderCalculatorProcess(OrderCalculator): pass
class StabilityCalculatorProcess(StabilityCalculator): pass
class StabilityFilterProcess(StabilityFilter): pass
class ProspectUnpivotProcessor(Unpivot, query=Querys.Contract): pass
class ProspectWriterProcess(ProspectWriter, query=Querys.Contract): pass
class ProspectDiscardingRoutine(ProspectDiscarding, query=Querys.Contract): pass
class ProspectAlteringRoutine(ProspectAltering, query=Querys.Contract): pass
class ProspectReaderSource(ProspectReader, query=Querys.Contract): pass
class ProspectUnpivotProcessor(Unpivot, query=Querys.Contract): pass
class HoldingCalculatorProcess(HoldingCalculator): pass
class HoldingSaverProcess(Saver, query=Querys.Contract): pass

class DivestitureTrading(object, fields=["discount", "liquidity", "capacity"], metaclass=NamedMeta): pass
class DivestitureSizing(object, fields=["size", "volume", "interest"], metaclass=NamedMeta): pass
class DivestitureTiming(object, fields=["date", "current", "tenure"], metaclass=NamedMeta): pass
class DivestitureProfit(object, fields=["apy", "cost"], metaclass=NamedMeta): pass

class DivestitureCriterion(object, named={"sizing": DivestitureSizing, "profit": DivestitureProfit}, metaclass=NamedMeta):
    def options(self, table): return self.interest(table) & self.volume(table) & self.size(table)
    def valuations(self, table): return self.apy(table) & self.cost(table) & (table[("size", "")] >= self.sizing.size)
    def interest(self, table): return table["interest"] >= self.sizing.interest
    def volume(self, table): return table["volume"] >= self.sizing.volume
    def size(self, table): return table["size"] >= self.sizing.size
    def apy(self, table): return table[("apy", Variables.Scenarios.MINIMUM)] >= self.profit.apy
    def cost(self, table): return table[("cost", Variables.Scenarios.MINIMUM)] <= self.profit.cost

class DivestitureProtocols(ProspectProtocols, named={"trading": DivestitureTrading, "timing": DivestitureTiming}, metaclass=NamedMeta):
    def limited(self, mask): return (mask.cumsum() < self.trading.capacity + 1) & mask
    def timeout(self, table): return (pd.to_datetime(self.timing.current) - table["current"]) >= self.timing.tenure
    def obsolete(self, table): return (table["status"] == Variables.Status.PROSPECT) & self.timeout(table)
    def unattractive(self, table): return table["priority"] < self.trading.discount
    def attractive(self, table): return table["priority"] >= self.trading.discount
    def illiquid(self, table): return table["size"] < self.trading.liquidity
    def liquid(self, table): return table["size"] >= self.trading.liquidity

    @ProspectProtocol(Variables.Status.OBSOLETE, order=1)
    def obsolete(self, table): return (table["status"] == Variables.Status.PROSPECT) & self.timeout(table)
    @ProspectProtocol(Variables.Status.PENDING, order=3)
    def pursue(self, table): return self.limited((table["status"] == Variables.Status.PROSPECT) & self.attractive(table))
    @ProspectProtocol(Variables.Status.ABANDONED, order=2)
    def abandon(self, table): return self.limited((table["status"] == Variables.Status.PROSPECT) & self.unattractive(table))
    @ProspectProtocol(Variables.Status.ACCEPTED, order=5)
    def accept(self, table): return self.limited((table["status"] == Variables.Status.PENDING) & self.liquid(table))
    @ProspectProtocol(Variables.Status.REJECTED, order=4)
    def reject(self, table): return self.limited((table["status"] == Variables.Status.PENDING) & self.illiquid(table))


def main(*args, protocol={}, criterion={}, parameters={}, **kwargs):
    bars_file = BarsFile(name="BarsFile", filetype=FileTypes.CSV, filetiming=FileTimings.EAGER, repository=HISTORY)
    holding_file = HoldingFile(name="HoldingFile", filetype=FileTypes.CSV, filetiming=FileTimings.EAGER, repository=PORTFOLIO)
    divestiture_layout = ProspectLayout(name="DivestitureLayout", valuation=Variables.Valuations.ARBITRAGE, rows=100)
    divestiture_header = ProspectHeader(name="DivestitureHeader", valuation=Variables.Valuations.ARBITRAGE)
    divestiture_table = ProspectTable(name="DivestitureTable", layout=divestiture_layout, header=divestiture_header)
    divestiture_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    divestiture_protocols = DivestitureProtocols(criterion)
    divestiture_criterion = DivestitureCriterion(protocol)

    holding_directory = HoldingDirectorySource(name="HoldingDirectory", file=holding_file, mode="r")
    bars_loader = BarsLoaderProcess(name="BarsLoader", file=bars_file, mode="r")
    statistic_calculator = StatisticCalculatorProcess(name="StatisticCalculator", technical=Variables.Technicals.STATISTIC)
    exposure_calculator = ExposureCalculatorProcess(name="ExposureCalculator")
#    option_calculator = OptionCalculatorProcess(name="OptionCalculator", pricing=, sizing=)
    option_filter = OptionFilterOperation(name="OptionFilter", criterion=divestiture_criterion.options)
    strategy_calculator = StrategyCalculatorProcess(name="StrategyCalculator", strategies=Variables.Strategies)
    valuation_calculator = ValuationCalculatorProcess(name="ValuationCalculator", valuation=Variables.Valuations.ARBITRAGE)
    valuation_pivot = ValuationPivotProcessor(name="ValuationPivot", pivot=("scenario", divestiture_header.variants))
    valuation_filter = ValuationFilterProcess(name="ValuationFilter", criterion=divestiture_criterion.valuations)
    prospect_calculator = ProspectCalculatorProcess(name="ProspectCalculator", header=divestiture_header, priority=divestiture_priority)
    order_calculator = OrderCalculatorProcess(name="OrderCalculator")
    stability_calculator = StabilityCalculatorProcess(name="StabilityCalculator")
    stability_filter = StabilityFilterProcess(name="StabilityFilter")
    prospect_writer = ProspectWriterProcess(name="ProspectWriter", table=divestiture_table)
    prospect_discarding = ProspectDiscardingRoutine(name="ProspectDiscarding", table=divestiture_table, status=[Variables.Status.OBSOLETE, Variables.Status.REJECTED, Variables.Status.ABANDONED])
    prospect_altering = ProspectAlteringRoutine(name="ProspectAltering", table=divestiture_table, protocols=list(divestiture_protocols))
    prospect_reader = ProspectReaderSource(name="ProspectReader", table=divestiture_table)
    prospect_unpivot = ProspectUnpivotProcessor(name="ProspectUnpivot", unpivot=("scenario", divestiture_header.variants))
    holding_calculator = HoldingCalculatorProcess(name="HoldingCalculator")
    holding_saver = HoldingSaverProcess(name="HoldingSaver", file=holding_file, mode="a")

    valuations_process = holding_directory + bars_loader + statistic_calculator + exposure_calculator + option_calculator + option_filter + strategy_calculator + valuation_calculator + valuation_pivot + valuation_filter
    valuations_process = valuations_process + prospect_calculator + order_calculator + stability_calculator + stability_filter + prospect_writer
    divestiture_process = prospect_reader + prospect_unpivot + holding_calculator + holding_saver
    valuations_thread = RepeatingThread(valuations_process, name="ValuationThread", wait=10).setup(**parameters)
    discarding_thread = RepeatingThread(prospect_discarding, name="DiscardingThread", wait=10).setup(**parameters)
    altering_thread = RepeatingThread(prospect_altering, name="AlteringThread", wait=10).setup(**parameters)
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
    sysDate = Date(year=2024, month=11, day=6)
    sysTenure = TimeDelta(days=1)
    sysTiming = dict(date=sysDate, current=sysCurrent, tenure=sysTenure)
    sysTrading = dict(discount=2.00, liquidity=25, capacity=1)
    sysSizing = dict(size=10, volume=100, interest=100)
    sysProfit = dict(apy=0.00, cost=0)
    sysCriterion = dict(timing=sysTiming, sizing=sysSizing, profit=sysProfit)
    sysProtocol = dict(timing=sysTiming, trading=sysTrading)
    sysParameters = dict(discount=0.00, fees=0.00)
    main(protocol=sysProtocol, criterion=sysCriterion, parameters=sysParameters)




