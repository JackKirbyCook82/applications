# -*- coding: utf-8 -*-
"""
Created on Fri Jan 1 2025
@name:   ETrade Acquisition Calculator
@author: Jack Kirby Cook

"""

import os
import sys
import time
import logging
import warnings
import pandas as pd
from collections import namedtuple as ntuple

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
REPOSITORY = os.path.join(ROOT, "repository")
if ROOT not in sys.path: sys.path.append(ROOT)

from finance.prospects import ProspectCalculator, ProspectRoutine, ProspectWriter, ProspectReader
from finance.prospects import ProspectTable, ProspectHeader, ProspectLayout
from finance.valuations import ValuationCalculator
from finance.strategies import StrategyCalculator
from finance.securities import SecurityCalculator
from finance.variables import Variables, Querys, Files, Strategies
from support.pipelines import Routine, Producer, Processor, Consumer
from support.synchronize import RoutineThread, RepeatingThread
from support.files import Directory, Loader
from support.filters import Filter, Criterion
from support.decorators import Decorator
from support.transforms import Pivoter
from support.mixins import Naming

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


class OptionDirectory(Directory, Producer, query=Querys.Settlement): pass
class OptionLoader(Loader, Processor, query=Querys.Settlement): pass
class OptionFilter(Filter, Processor, query=Querys.Settlement): pass
class SecurityCalculator(SecurityCalculator, Processor): pass
class SecurityFilter(Filter, Processor, query=Querys.Settlement): pass
class StrategyCalculator(StrategyCalculator, Processor): pass
class ValuationCalculator(ValuationCalculator, Processor): pass
class ValuationPivoter(Pivoter, Processor, query=Querys.Settlement): pass
class ValuationFilter(Filter, Processor, query=Querys.Settlement): pass
class ProspectCalculator(ProspectCalculator, Processor): pass
class ProspectWriter(ProspectWriter, Consumer, query=Querys.Settlement): pass
class ProspectRoutine(ProspectRoutine, Routine, query=Querys.Settlement): pass
class ProspectReader(ProspectReader, Producer, query=Querys.Settlement): pass

class Criterions(ntuple("Criterion", "security valuation")): pass
class Queues(ntuple("Queues", "trade")): pass

class SecurityCriterion(Criterion, fields=["size"]):
    def execute(self, table): return self.size(table)
    def size(self, table): return table["size"] >= self["size"]

class ValuationCriterion(Criterion, fields=["apy", "cost", "size"]):
    def execute(self, table): return self.apy(table) & self.cost(table) & self.size(table)
    def apy(self, table): return table[("apy", Variables.Valuations.Scenario.MINIMUM)] >= self["apy"]
    def cost(self, table): return (table[("cost", Variables.Valuations.Scenario.MINIMUM)] > 0) & (table[("cost", Variables.Valuations.Scenario.MINIMUM)] <= self["cost"])
    def size(self, table): return table[("size", "")] >= self["size"]

class AcquisitionProtocol(Naming, fields=["capacity", "discount", "liquidity"]):
    def limited(self, mask): return (mask.cumsum() < self["capacity"] + 1) & mask
    def obsolete(self, table): return (table["status"] == Variables.Status.PROSPECT) & self.timeout(table)
    def unattractive(self, table): return table["priority"] < self["discount"]
    def attractive(self, table): return table["priority"] >= self["discount"]
    def illiquid(self, table): return table["size"] < self["liquidity"]
    def liquid(self, table): return table["size"] >= self["liquidity"]

    @Decorator(status=Variables.Markets.Status.ABANDONED)
    def abandon(self, table): return self.limited((table["status"] == Variables.Markets.Status.PROSPECT) & self.unattractive(table))
    @Decorator(status=Variables.Markets.Status.PENDING)
    def pursue(self, table): return self.limited((table["status"] == Variables.Markets.Status.PROSPECT) & self.attractive(table))
    @Decorator(status=Variables.Markets.Status.REJECTED)
    def reject(self, table): return self.limited((table["status"] == Variables.Markets.Status.PENDING) & self.illiquid(table))
    @Decorator(status=Variables.Markets.Status.ACCEPTED)
    def accept(self, table): return self.limited((table["status"] == Variables.Markets.Status.PENDING) & self.liquid(table))


def acquisition(*args, table, header, priority, criterions, files, **kwargs):
    option_directory = OptionDirectory(name="OptionDirectory", file=files.option, mode="r")
    option_loader = OptionLoader(name="OptionLoader", file=files.option, mode="r")
    security_calculator = SecurityCalculator(name="SecurityCalculator", pricing=Variables.Markets.Pricing.CENTERED)
    security_filter = SecurityFilter(name="SecurityFilter", criterion=criterions.security)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies))
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    valuation_pivoter = ValuationPivoter(name="ValuationPivoter", header=header)
    valuation_filter = ValuationFilter(name="ValuationFilter", criterion=criterions.valuation)
    prospect_calculator = ProspectCalculator(name="ProspectCalculator", header=header, priority=priority)
    prospect_writer = ProspectWriter(name="ProspectWriter", table=table)
    acquisition_pipeline = option_directory + option_loader + security_calculator + security_filter + strategy_calculator
    acquisition_pipeline = acquisition_pipeline + valuation_calculator + valuation_pivoter + valuation_filter + prospect_calculator + prospect_writer
    return acquisition_pipeline


def main(*args, criterion={}, protocol={}, discount, fees, **kwargs):
    acquisition_layout = ProspectLayout(name="AcquisitionLayout", valuation=Variables.Valuations.Valuation.ARBITRAGE, rows=100)
    acquisition_header = ProspectHeader(name="AcquisitionHeader", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    acquisition_table = ProspectTable(name="AcquisitionTable", layout=acquisition_layout, header=acquisition_header)
    acquisition_priority = lambda cols: cols[("apy", Variables.Valuations.Scenario.MINIMUM)]
    acquisition_protocol = AcquisitionProtocol(**protocol)
    option_file = (Files.Options.Trade + Files.Options.Quote)(name="OptionFile", folder="option", repository=REPOSITORY)
    security_criterion = SecurityCriterion(**criterion)
    valuation_criterion = ValuationCriterion(**criterion)
    criterions = Criterions(security_criterion, valuation_criterion)
    files = Files(option_file)

    acquisition_parameters = dict(table=acquisition_table, header=acquisition_header, priority=acquisition_priority, criterions=criterions, files=files)
    acquisition_pipeline = acquisition(*args, **acquisition_parameters, **kwargs)
    acquisition_thread = RoutineThread(acquisition_pipeline, name="MarketThread").setup(discount=discount, fees=fees)
    failure_reader = ProspectReader(name="FailureReader", table=acquisition_table, status=[Variables.Markets.Status.OBSOLETE, Variables.Markets.Status.ABANDONED, Variables.Markets.Status.REJECTED])
    failure_thread = RepeatingThread(failure_reader, name="FailureThread", wait=10)
    success_routine = ProspectRoutine(name="SuccessRoutine", table=acquisition_table, protocol=acquisition_protocol)
    success_thread = RepeatingThread(success_routine, name="SuccessThread", wait=10)

    acquisition_thread.start()
    success_thread.start()
    failure_thread.start()
    while bool(acquisition_thread) or bool(acquisition_table):
        print(acquisition_table)
        time.sleep(10)
    acquisition_thread.cease()
    success_thread.cease()
    failure_thread.cease()
    acquisition_thread.join()
    success_thread.join()
    failure_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    sysProtocol = dict(capacity=100, liquidity=25, discount=3.50)
    sysCriterion = dict(apy=1.50, cost=1000, size=10)
    main(criterion=sysCriterion, protocol=sysProtocol, discount=0.00, fees=0.00)


