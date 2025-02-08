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

from finance.prospects import ProspectCalculator, ProspectWriter, ProspectTable, ProspectHeader, ProspectLayout
from finance.valuations import ValuationCalculator
from finance.strategies import StrategyCalculator
from finance.variables import Variables, Querys, Files
from support.pipelines import Producer, Processor, Consumer
from support.synchronize import RoutineThread
from support.files import Directory, Loader
from support.transforms import Pivoter
from support.filters import Filter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


class OptionDirectory(Directory, Producer, query=Querys.Settlement): pass
class OptionLoader(Loader, Processor, query=Querys.Settlement): pass
class OptionFilter(Filter, Processor, query=Querys.Settlement): pass
class StrategyCalculator(StrategyCalculator, Processor): pass
class ValuationCalculator(ValuationCalculator, Processor): pass
class ValuationPivoter(Pivoter, Processor, query=Querys.Settlement): pass
class ValuationFilter(Filter, Processor, query=Querys.Settlement): pass
class ProspectCalculator(ProspectCalculator, Processor): pass
class ProspectWriter(ProspectWriter, Consumer): pass
class AcquisitionFile(Files.Options.Trade + Files.Options.Quote): pass


def main(*args, criterion={}, discount, fees, **kwargs):
    acquisition_file = AcquisitionFile(name="AcquisitionFile", folder="market", repository=REPOSITORY)
    acquisition_layout = ProspectLayout(name="AcquisitionLayout", valuation=Variables.Valuations.ARBITRAGE, rows=100)
    acquisition_header = ProspectHeader(name="AcquisitionHeader", valuation=Variables.Valuations.ARBITRAGE)
    acquisition_table = ProspectTable(name="AcquisitionTable", layout=acquisition_layout, header=acquisition_header)
    acquisition_criterion = lambda table: table[("apy", Variables.Scenarios.MINIMUM)] >= criterion["apy"] & table[("size", Variables.Scenarios.MINIMUM)] >= criterion["size"]
    acquisition_priority = lambda columns: columns[("apy", Variables.Scenarios.MINIMUM)]
    acquisition_strategies = list(Variables.Strategies.Strategy)
    acquisition_valuation = Variables.Valuations.Valuation.ARBITRAGE

    option_directory = OptionDirectory(name="OptionDirectory", file=acquisition_file, mode="r")
    option_loader = OptionLoader(name="OptionLoader", file=acquisition_file, mode="r")
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", strategies=acquisition_strategies)
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=acquisition_valuation)
    valuation_pivoter = ValuationPivoter(name="ValuationPivoter", header=acquisition_header)
    valuation_filter = ValuationFilter(name="ValuationFilter", criterion=acquisition_criterion)
    prospect_calculator = ProspectCalculator(name="ProspectCalculator", header=acquisition_header, priority=acquisition_priority)
    prospect_writer = ProspectWriter(name="ProspectWriter", table=acquisition_table)
    acquisition_pipeline = option_directory + option_loader + strategy_calculator + valuation_calculator + valuation_pivoter + valuation_filter + prospect_calculator + prospect_writer
    acquisition_thread = RoutineThread(acquisition_pipeline, name="AcquisitionThread").setup(discount=discount, fees=fees)

    acquisition_thread.start()
    while bool(acquisition_thread):
        if bool(acquisition_table): print(acquisition_table)
        time.sleep(10)
    acquisition_thread.cease()
    acquisition_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    sysCriterion = dict(apy=2.00, size=10)
    main(criterion=sysCriterion, discount=0.00, fees=0.00)


