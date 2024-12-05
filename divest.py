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
from finance.prospects import ProspectCalculator, ProspectWriter, ProspectReader, ProspectTable, ProspectHeader, ProspectLayout
from finance.orders import OrderCalculator
from finance.stability import StabilityCalculator, StabilityFilter
from finance.holdings import HoldingCalculator, HoldingFile
from support.files import Directory, Loader, Saver, FileTypes, FileTimings
from support.synchronize import RepeatingThread
from support.processes import Source, Process
from support.filtering import Filter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class BarsLoaderProcess(Loader, Source, query=Querys.Symbol): pass
class ExposureCalculatorProcess(ExposureCalculator, Process): pass
class StatisticCalculatorProcess(TechnicalCalculator, Process): pass
class HoldingDirectorySource(Directory, Source, query=Querys.Contract): pass
class OptionCalculatorProcess(OptionCalculator, Process): pass
class OptionFilterOperation(Filter, Process): pass
class StrategyCalculatorProcess(StrategyCalculator, Process): pass
class ValuationCalculatorProcess(ValuationCalculator, Process, ): pass
class ValuationFilterProcess(Filter, Process): pass
class ProspectCalculatorProcess(ProspectCalculator, Process): pass
class OrderCalculatorProcess(OrderCalculator, Process): pass
class StabilityCalculatorProcess(StabilityCalculator, Process): pass
class StabilityFilterProcess(StabilityFilter, Process): pass
class ProspectWriterProcess(ProspectWriter, Process, query=Querys.Contract): pass
class ProspectReaderSource(ProspectReader, Source, query=Querys.Contract): pass
class HoldingCalculatorProcess(HoldingCalculator, Process, ): pass
class HoldingSaverProcess(Saver, Process, query=Querys.Contract): pass


def main(*args, arguments, parameters, **kwargs):
    bars_file = BarsFile(name="BarsFile", filetype=FileTypes.CSV, filetiming=FileTimings.EAGER, repository=HISTORY)
    holding_file = HoldingFile(name="HoldingFile", filetype=FileTypes.CSV, filetiming=FileTimings.EAGER, repository=PORTFOLIO)
    divestiture_layout = ProspectLayout(name="DivestitureLayout", valuation=Variables.Valuations.ARBITRAGE, rows=100)
    divestiture_header = ProspectHeader(name="DivestitureHeader", valuation=Variables.Valuations.ARBITRAGE)
    divestiture_table = ProspectTable(name="DivestitureTable", layout=divestiture_layout, header=divestiture_header)
    prospect_priority = lambda cols: cols[("apy", Variables.Scenarios.MINIMUM)]
    divestiture_criterion = DivestitureCriterion(**arguments)
    divestiture_protocol = DivestitureProtocol(**arguments)


    bars_loader = BarsLoaderProcess(name="BarsLoader", file=bars_file, mode="r")
    statistic_calculator = StatisticCalculatorProcess(name="StatisticCalculator", technical=Variables.Technicals.STATISTIC)
    holding_directory = HoldingDirectorySource(name="HoldingDirectory", file=holding_file, mode="r")
    exposure_calculator = ExposureCalculatorProcess(name="ExposureCalculator")
    option_calculator = OptionCalculatorProcess(name="OptionCalculator", pricing=Variables.Pricing.BLACKSCHOLES, sizing=option_sizing)
#    option_filter = OptionFilterOperation(name="OptionFilter", criterion=option_criterion)
    strategy_calculator = StrategyCalculatorProcess(name="StrategyCalculator", strategies=Variables.Strategies)
    valuation_calculator = ValuationCalculatorProcess(name="ValuationCalculator", header=divestiture_table.header, combine=pd.concat)
#    valuation_filter = ValuationFilterProcess(name="ValuationFilter", criterion=valuation_criterion)
    prospect_calculator = ProspectCalculatorProcess(name="ProspectCalculator", priority=valuation_priority)
    order_calculator = OrderCalculatorProcess(name="OrderCalculator")
    stability_calculator = StabilityCalculatorProcess(name="StabilityCalculator")
    stability_filter = StabilityFilterProcess(name="StabilityFilter")
    prospect_writer = ProspectWriterProcess(name="ProspectWriter", table=divestiture_table)
    prospect_reader = ProspectReaderSource(name="ProspectReader", table=divestiture_table)
    holding_calculator = HoldingCalculatorProcess(name="HoldingCalculator", header=divestiture_table.header)
    holding_saver = HoldingSaverProcess(name="HoldingSaver", file=holding_file, mode="a")


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysCurrent = Datetime(year=2024, month=11, day=6)
    sysDate = Date(year=2024, month=11, day=6)
    sysTenure = TimeDelta(days=1)
    sysTiming = dict(date=sysDate, current=sysCurrent, tenure=sysTenure)
    sysTrading = dict(discount=2.00, liquidity=25, capacity=1)
    sysSizing = dict(size=10, volume=100, interest=100)
    sysProfit = dict(apy=0.00, cost=0)
    sysArguments = dict(timing=sysTiming, trading=sysTrading, sizing=sysSizing, profit=sysProfit)
    sysParameters = dict(current=sysCurrent, discount=0.00, fees=0.00, period=252)
    main(arguments=sysArguments, parameters=sysParameters)




