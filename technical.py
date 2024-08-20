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
import pandas as pd
import xarray as xr

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


class SymbolLoader(Loader, variable=Variables.Querys.SYMBOL, create=Symbol.fromstr): pass
class SymbolSaver(Saver, variable=Variables.Querys.SYMBOL): pass


def technical(*args, directory, loading, saving, parameters={}, functions={}, **kwargs):
    technical_loader = SymbolLoader(name="TechnicalLoader", source=loading, directory=directory, **functions)
    technical_calculator = TechnicalCalculator(name="TechnicalCalculator")
    technical_saver = SymbolSaver(name="TechnicalSaver", destination=saving)
    technical_pipeline = technical_loader + technical_calculator + technical_saver
    technical_thread = SideThread(technical_pipeline, name="TechnicalThread")
    technical_thread.setup(**parameters)
    return technical_thread


def main(*args, parameters, **kwargs):
    bars_file = TechnicalFiles.Bars(name="BarsFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    statistic_file = TechnicalFiles.Statistic(name="StatisticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    stochastic_file = TechnicalFiles.Stochastic(name="StochasticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)

    technical_parameters = dict(directory=bars_file, loading={bars_file: "r"}, saving={statistic_file: "w", stochastic_file: "w"}, parameters=parameters)
    technical_thread = technical(*args, **technical_parameters, **kwargs)
    technical_thread.start()
    technical_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    xr.set_options(display_width=250)
    sysParameters = dict(period=252)
    main(parameters=sysParameters)



