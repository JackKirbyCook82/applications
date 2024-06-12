# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Trading Platform Valuation
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.valuations import ValuationCalculator, ValuationFilter, ValuationFiles
from finance.securities import SecurityFilter, SecurityFiles, SecurityHeaders
from finance.variables import Strategies, Valuations
from finance.strategies import StrategyCalculator
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import SideThread
from support.filtering import Criterion
from support.parsers import Parser

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


def valuation(*args, source, destination, headers, calculations, criterion, parameters, **kwargs):
    security_loader = Loader(name="SecurityLoader", source=source)
    security_filter = SecurityFilter(name="SecurityFilter", criterion=criterion["security"])
    security_parser = Parser(name="SecurityParser", headers=headers)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", calculations=calculations)
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", calculation=Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="ValuationFilter", criterion=criterion["valuation"])
    valuation_saver = Saver(name="ValuationSaver", destination=destination)
    valuation_pipeline = security_loader + security_filter + security_parser + strategy_calculator + valuation_calculator + valuation_filter + valuation_saver
    valuation_thread = SideThread(valuation_pipeline, name="ValuationThread")
    valuation_thread.setup(**parameters)
    return valuation_thread


def main(*args, **kwargs):
    security_criterion = {Criterion.FLOOR: {"volume": 25, "interest": 25, "size": 10}, Criterion.NULL: ["volume", "interest", "size"]}
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.0, "size": 10}, Criterion.NULL: ["apy", "size"]}
    criterion = dict(security=security_criterion, valuation=valuation_criterion)
    options_file = SecurityFiles.Options(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    options_header = SecurityHeaders.Options(name="OptionHeader", duplicates=False)
    arbitrage_file = ValuationFiles.Arbitrage(name="ArbitrageFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    strategy_calculations = [Strategies.Collar.Long, Strategies.Collar.Short, Strategies.Vertical.Put, Strategies.Vertical.Call]
    valuation_parameters = dict(source={options_file: "r"}, destination={arbitrage_file: "a"}, headers=[options_header], calculations=strategy_calculations, criterion=criterion)
    valuation_thread = valuation(*args, **valuation_parameters, **kwargs)
    valuation_thread.start()
    valuation_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    main(parameters={"discount": 0.0, "fees": 0.0})



