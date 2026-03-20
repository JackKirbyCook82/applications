# -*- coding: utf-8 -*-
"""
Created on Weds Mar 18 2026
@name:   Trading Application
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
from types import SimpleNamespace

import pandas as pd
from enum import Enum
from attr.converters import to_bool
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from collections import namedtuple as ntuple

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
REPOSITORY = os.path.join(ROOT, "repository")
RESOURCES = os.path.join(ROOT, "resources")
if ROOT not in sys.path: sys.path.append(ROOT)
AUTHENTICATORS = os.path.join(RESOURCES, "authenticators.txt")
ACCOUNTS = os.path.join(RESOURCES, "accounts.txt")
TICKERS = os.path.join(RESOURCES, "tickers.txt")

from alpaca.market import AlpacaStockDownloader, AlpacaContractDownloader, AlpacaOptionDownloader
from webscraping.webreaders import WebReader
from support.concepts import DateRange
from support.queues import Queues

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


Website = Enum("Website", ["ETRADE", "ALPACA", "INTERACTIVE"])
Brokerage = ntuple("Brokerage", {"website": Website, "live": bool})


def load(file):
    key = lambda website, live: Brokerage(Website(website), to_bool(live))
    value = lambda header, body: SimpleNamespace(**dict(zip(header, body)))
    contents = [str(line).split(" ") for line in open(file, "r").read().splitlines()]
    mapping = {key(*line[:2]): value(contents[0][2:], line[2:]) for line in contents[1:]}
    return mapping


def main(*args, tickers, expires, interest, discount, fees, **kwargs):
    authenticators = load(AUTHENTICATORS)
    accounts = load(ACCOUNTS)
    tickers = Queues.FIFO(contents=tickers, capacity=None, timeout=None)

    with WebReader() as source:
        stock_downloader = AlpacaStockDownloader(source=source, authenticator=authenticators[], name="StockDownloader")
        contract_downloader = AlpacaContractDownloader(source=source, authenticator=authenticators[], name="ContractDownloader")
        option_downloader = AlpacaOptionDownloader(source=source, authenticator=authenticators[], name="OptionDownloader")

        while bool(tickers):
            ticker = ticker.read()
            stock = stock_downloader(ticker=[ticker]).squeeze()
            contracts = contract_downloader(ticker=[ticker], expires=expires, strikes=None)
            options = option_downloader(contracts=contracts)




if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    arguments, parameters = list(), dict()
    parameters["tickers"] = open(TICKERS, "r").read().splitlines()
    parameters["expires"] = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52*2)).date()])
    parameters.update({"interest": 0.05, "discount": 0.05, "fees": 3.00})
    main(*arguments, **parameters)

