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

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
REPOSITORY = os.path.join(ROOT, "repository")
if ROOT not in sys.path: sys.path.append(ROOT)

from finance.prospects import ProspectCalculator, ProspectWriter, ProspectDiscarding, ProspectReader
from finance.prospects import ProspectTable, ProspectHeader, ProspectLayout
from finance.valuations import ValuationCalculator
from finance.strategies import StrategyCalculator
from finance.securities import SecurityCalculator
from finance.variables import Variables, Querys, Files, Strategies
from support.pipelines import Routine, Producer, Processor, Consumer
from support.filters import Filter, Criterion
from support.synchronize import RoutineThread
from support.files import Directory, Loader
from support.transforms import Pivoter

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
class ProspectRemover(ProspectDiscarding, Routine, query=Querys.Settlement): pass
class ProspectReader(ProspectReader, Producer, query=Querys.Settlement): pass

class AcquisitionFile(Files.Options.Trade + Files.Options.Quote):
    pass

class SecurityCriterion(Criterion):
    size = lambda table, size: table["size"] >= size

class ValuationCriterion(Criterion):
    apy = lambda table, apy: table[("apy", Variables.Valuations.Scenario.MINIMUM)] >= apy
    cost = lambda table, cost: (table[("cost", Variables.Valuations.Scenario.MINIMUM)] > 0) & (table[("cost", Variables.Valuations.Scenario.MINIMUM)] <= cost)
    size = lambda table, size: table[("size", "")] >= size


def main(*args, criterion={}, discount, fees, **kwargs):
    acquisition_file = AcquisitionFile(name="AcquisitionFile", folder="market", repository=REPOSITORY)
    acquisition_layout = ProspectLayout(name="AcquisitionLayout", valuation=Variables.Valuations.Valuation.ARBITRAGE, rows=100)
    acquisition_header = ProspectHeader(name="AcquisitionHeader", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    acquisition_table = ProspectTable(name="AcquisitionTable", layout=acquisition_layout, header=acquisition_header)
    acquisition_priority = lambda cols: cols[("apy", Variables.Valuations.Scenario.MINIMUM)]
    security_criterion = SecurityCriterion(**criterion)
    valuation_criterion = ValuationCriterion(**criterion)

    option_directory = OptionDirectory(name="OptionDirectory", file=acquisition_file, mode="r")
    option_loader = OptionLoader(name="OptionLoader", file=acquisition_file, mode="r")
    security_calculator = SecurityCalculator(name="SecurityCalculator", pricing=Variables.Markets.Pricing.CENTERED)
    security_filter = SecurityFilter(name="SecurityFilter", criterion=security_criterion)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=list(Strategies))
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Variables.Valuations.Valuation.ARBITRAGE)
    valuation_pivoter = ValuationPivoter(name="ValuationPivoter", header=acquisition_header)
    valuation_filter = ValuationFilter(name="ValuationFilter", criterion=valuation_criterion)
    prospect_calculator = ProspectCalculator(name="ProspectCalculator", header=acquisition_header, priority=acquisition_priority)
    prospect_writer = ProspectWriter(name="ProspectWriter", table=acquisition_table)
    acquisition_pipeline = option_directory + option_loader + security_calculator + security_filter + strategy_calculator + valuation_calculator + valuation_pivoter + valuation_filter + prospect_calculator + prospect_writer
    acquisition_thread = RoutineThread(acquisition_pipeline, name="AcquisitionThread").setup(discount=discount, fees=fees)

    acquisition_thread.start()
    while bool(acquisition_thread):
        print(acquisition_table)
        time.sleep(10)
    acquisition_thread.cease()
    acquisition_thread.join()
    print(acquisition_table)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    sysCriterion = dict(apy=1.00, cost=1000, size=10)
    main(criterion=sysCriterion, discount=0.00, fees=0.00)


