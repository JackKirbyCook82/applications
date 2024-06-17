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
from finance.variables import Variables
from support.files import Loader, Saver, FileTypes, FileTimings
from support.synchronize import SideThread

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


def technical(*args, source, destination, calculations, parameters={}, **kwargs):
    technical_loader = Loader(name="TechnicalLoader", source=source)
    technical_calculator = TechnicalCalculator(name="TechnicalCalculator", calculations=calculations)
    technical_saver = Saver(name="TechnicalSaver", destination=destination)
    technical_pipeline = technical_loader + technical_calculator + technical_saver
    technical_thread = SideThread(technical_pipeline, name="TechnicalThread")
    technical_thread.setup(**parameters)
    return technical_thread


def main(*args, **kwargs):
    bars_file = TechnicalFiles.Bars(name="BarsFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    statistics_file = TechnicalFiles.Statistic(name="StatisticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    stochastics_file = TechnicalFiles.Stochastic(name="StochasticFile", repository=HISTORY, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    technical_calculations = [Variables.echnicals.STATISTIC, Variables.Technicals.STOCHASTIC]
    technical_parameters = dict(source={bars_file: "r"}, destination={statistics_file: "w", stochastics_file: "w"}, calculations=technical_calculations)
    technical_thread = technical(*args, **technical_parameters, **kwargs)
    technical_thread.start()
    technical_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    main(parameters={"period": 252})



