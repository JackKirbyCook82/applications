# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   Trading Platform Technicals
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
HISTORY = os.path.join(ROOT, "repository", "history")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Variables, Symbol
from finance.technicals import TechnicalCalculator, TechnicalFiles
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import SideThread

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


loading_formatter = lambda self, *, results, elapsed, **kw: f"{str(self.title)}: {repr(self)}|{str(results[Variables.Querys.SYMBOL])}[{elapsed:.02f}s]"
saving_formatter = lambda self, *, elapsed, **kw: f"{str(self.title)}: {repr(self)}[{elapsed:.02f}s]"
class SymbolLoader(Loader, query=Variables.Querys.SYMBOL, create=Symbol.fromstr, formatter=loading_formatter): pass
class SymbolSaver(Saver, query=Variables.Querys.SYMBOL, formatter=saving_formatter): pass


def technical(*args, directory, loading, saving, parameters={}, functions={}, **kwargs):
    technical_loader = SymbolLoader(name="TechnicalLoader", source=loading, directory=directory, **functions)
    technical_calculator = TechnicalCalculator(name="TechnicalCalculator")
    technical_saver = SymbolSaver(name="TechnicalSaver", destination=saving)
    technical_pipeline = technical_loader + technical_calculator + technical_saver
    technical_thread = SideThread(technical_pipeline, name="TechnicalThread")
    technical_thread.setup(**parameters)
    return technical_thread


def main(*args, **kwargs):
    bars_file = TechnicalFiles.Bars(name="BarsFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    statistic_file = TechnicalFiles.Statistic(name="StatisticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    stochastic_file = TechnicalFiles.Stochastic(name="StochasticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    technical_parameters = dict(directory=bars_file, loading={bars_file: "r"}, saving={statistic_file: "w", stochastic_file: "w"})
    technical_thread = technical(*args, **technical_parameters, **kwargs)
    technical_thread.start()
    technical_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    sysParameters = dict(period=252)
    main(parameters=sysParameters)



