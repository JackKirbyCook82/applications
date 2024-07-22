# -*- coding: utf-8 -*-
"""
Created on Fri Jul 19 2024
@name:   Trading Platform PaperTrading
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
from datetime import datetime as Datetime

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
PORTFOLIO = os.path.join(ROOT, "repository", "portfolio")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from etrade.window import PaperTradeApplication
from support.synchronize import MainThread
from finance.holdings import HoldingTable

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


def papertrade(*args, acquisitions, divestitures, parameters={}, **kwargs):
    papertrade_application = PaperTradeApplication(acquisitions=acquisitions, divestitures=divestitures, name="PaperTradeApplication")
    papertrade_thread = MainThread(papertrade_application, name="PaperTradeThread")
    papertrade_thread.setup(**parameters)
    return papertrade_thread


def main(*args, arguments, parameters, **kwargs):
    acquisition_table = HoldingTable(name="AcquisitionTable")
    divestiture_table = HoldingTable(name="DivestitureTable")
    papertrade_parameters = dict(acquisitions=acquisition_table, divestitures=divestiture_table, parameters=parameters)
    papertrade_thread = papertrade(*args, **papertrade_parameters, **kwargs)
    papertrade_thread.start()
    papertrade_thread.run()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    current = Datetime(year=2024, month=7, day=18)
    sysArguments = dict(apy=0.035, size=10)
    sysParameters = dict(current=current, discount=0.0, fees=0.0)
    main(arguments=sysArguments, parameters=sysParameters)

