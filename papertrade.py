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

from support.synchronize import MainThread
from etrade.window import PaperTradeApplication

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


def papertrade(*args, parameters={}, **kwargs):
    papertrade_application = PaperTradeApplication(name="PaperTradeApplication")
    papertrade_thread = MainThread(papertrade_application, name="PaperTradeThread")
    papertrade_thread.setup(**parameters)
    return papertrade_thread


def main(*args, arguments, parameters, **kwargs):
    papertrade_parameters = dict(parameters=parameters)
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

