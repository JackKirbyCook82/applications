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
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.valuations import ValuationCalculator, ValuationFilter, ValuationFiles
from finance.securities import SecurityFilter, SecurityFiles
from finance.variables import Strategies, Querys, Variables
from finance.strategies import StrategyCalculator
from support.files import Loader, Saver, Directory, FileTypes, FileTimings
from support.synchronize import SideThread
from support.filtering import Criterion

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class ContractLoader(Loader, query=("contract", Querys.Contract)): pass
class ContractSaver(Saver, query=("contract", Querys.Contract)): pass
class ContractDirectory(Directory):
    @staticmethod
    def parser(filename):
        ticker, expire = str(filename).split("_")
        ticker = str(ticker).upper()
        expire = Datetime.strptime(expire, "%Y%m%d")
        return Querys.Contract(ticker, expire)


def valuation(*args, loading, saving, directory, calculations=[], criterions={}, parameters={}, **kwargs):
    security_loader = ContractLoader(name="MarketSecurityLoader", source=loading, directory=directory)
    security_filter = SecurityFilter(name="MarketSecurityFilter", criterion=criterions["security"])
    strategy_calculator = StrategyCalculator(name="MarketStrategyCalculator", calculations=calculations)
    valuation_calculator = ValuationCalculator(name="MarketValuationCalculator", calculation=Variables.Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="MarketValuationFilter", criterion=criterions["valuation"])
    valuation_saver = ContractSaver(name="MarketValuationSaver", destination=saving)
    valuation_pipeline = security_loader + security_filter + strategy_calculator + valuation_calculator + valuation_filter + valuation_saver
    valuation_thread = SideThread(valuation_pipeline, name="MarketValuationThread")
    valuation_thread.setup(**parameters)
    return valuation_thread


def main(*args, **kwargs):
    security_criterion = {Criterion.FLOOR: {"volume": 25, "interest": 25, "size": 10}, Criterion.NULL: ["volume", "interest", "size"]}
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.0, "size": 10}, Criterion.NULL: ["apy", "size"]}
    criterions = dict(security=security_criterion, valuation=valuation_criterion)
    calculations = [Strategies.Collar.Long, Strategies.Collar.Short, Strategies.Vertical.Put, Strategies.Vertical.Call]
    option_file = SecurityFiles.Options(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    arbitrage_file = ValuationFiles.Arbitrage(name="ArbitrageFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    options_directory = ContractDirectory(name="OptionsDirectory", repository=MARKET, variable="option")
    valuation_parameters = dict(loading={option_file: "r"}, saving={arbitrage_file: "w"}, directory=options_directory, calculations=calculations, criterions=criterions)
    valuation_thread = valuation(*args, **valuation_parameters, **kwargs)
    valuation_thread.start()
    valuation_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    sysParameters = dict(discount=0.0, fees=0.0)
    main(parameters=sysParameters)



