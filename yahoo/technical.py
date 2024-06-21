# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   Yahoo Trading Platform Technicals
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
HISTORY = os.path.join(ROOT, "repository", "history")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.technicals import TechnicalCalculator, TechnicalFiles
from finance.variables import Querys, Variables
from support.files import Loader, Saver, Directory, FileTypes, FileTimings
from support.synchronize import SideThread

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class SymbolLoader(Loader, query=("symbol", Querys.Symbol)): pass
class SymbolSaver(Saver, query=("symbol", Querys.Symbol)): pass
class SymbolDirectory(Directory):
    @staticmethod
    def parser(filename): return Querys.Symbol(filename)


def technical(*args, loading, saving, directory, calculations=[], parameters={}, **kwargs):
    technical_loader = SymbolLoader(name="TechnicalLoader", source=loading, directory=directory)
    technical_calculator = TechnicalCalculator(name="TechnicalCalculator", calculations=calculations)
    technical_saver = SymbolSaver(name="TechnicalSaver", destination=saving)
    technical_pipeline = technical_loader + technical_calculator + technical_saver
    technical_thread = SideThread(technical_pipeline, name="TechnicalThread")
    technical_thread.setup(**parameters)
    return technical_thread


def main(*args, **kwargs):
    bars_file = TechnicalFiles.Bars(name="BarsFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    statistic_file = TechnicalFiles.Statistic(name="StatisticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    stochastic_file = TechnicalFiles.Stochastic(name="StochasticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    bars_directory = SymbolDirectory(name="BarsDirectory", repository=HISTORY, variable="bars")
    calculations = [Variables.Technicals.STATISTIC, Variables.Technicals.STOCHASTIC]
    technical_parameters = dict(loading={bars_file: "r"}, saving={statistic_file: "w", stochastic_file: "w"}, directory=bars_directory, calculations=calculations)
    technical_thread = technical(*args, **technical_parameters, **kwargs)
    technical_thread.start()
    technical_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    sysParameters = dict(period=252)
    main(parameters=sysParameters)



