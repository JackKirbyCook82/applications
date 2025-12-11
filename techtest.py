# -*- coding: utf-8 -*-
"""
Created on Thurs Dec 11 2025
@name:   Technical Analysis
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import pandas as pd
from enum import Enum
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from collections import namedtuple as ntuple

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
RESOURCES = os.path.join(ROOT, "resources")
if ROOT not in sys.path: sys.path.append(ROOT)
WEBAPI = os.path.join(RESOURCES, "webapi.txt")

from finance.concepts import Querys
from webscraping.webreaders import WebReader
from support.concepts import DateRange
from support.mixins import Delayer

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2025, Jack Kirby Cook"
__license__ = "MIT License"


Website = Enum("WebSite", "ALPACA ETRADE")


def main(*args, webapi, delayer, parameters={}, **kwargs):
    with WebReader(delayer=delayer) as source:
        pass


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    function = lambda contents: ntuple("WebApi", list(contents.keys()))(*contents.values())
    sysWebApi = pd.read_csv(WEBAPI, sep=" ", header=0, index_col=0, converters={0: lambda website: Website[str(website).upper()]})
    sysWebApi = function(sysWebApi.to_dict("index")[Website.ALPACA])
    sysHistory = DateRange([(Datetime.today() - Timedelta(weeks=52*5)).date(), (Datetime.today() - Timedelta(days=1)).date()])
    sysParameters = dict(current=Datetime.now().date(), symbol=Querys.Symbol("SPY"), history=sysHistory, period=252)
    main(webapi=sysWebApi, delayer=Delayer(3), parameters=sysParameters)

