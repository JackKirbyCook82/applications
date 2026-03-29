# -*- coding: utf-8 -*-
"""
Created on Weds Mar 18 2026
@name:   Trading Application
@author: Jack Kirby Cook

"""

import os
import sys
import random
import logging
import warnings
import pandas as pd
from enum import Enum
from types import SimpleNamespace
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
from alpaca.history import AlpacaBarsDownloader
from finance.technicals import TechnicalCalculator
from finance.options import SanityFilter, ViabilityFilter, OptionCalculator
from finance.greeks import GreekCalculator
from finance.implied import ImpliedCalculator
from finance.concepts import Concepts, Querys
from webscraping.webreaders import WebReader
from support.concepts import DateRange, NumRange
from support.queues import Queues

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


Website = Enum("Website", ["ETRADE", "ALPACA", "INTERACTIVE"])
Brokerage = ntuple("Brokerage", {"website": Website, "live": bool})


def load(file):
    key = lambda website, live: Brokerage(Website[str(website).upper()], to_bool(live))
    value = lambda header, body: SimpleNamespace(**dict(zip(header, body)))
    contents = [str(line).split(" ") for line in open(file, "r").read().splitlines()]
    mapping = {key(*line[:2]): value(contents[0][2:], line[2:]) for line in contents[1:]}
    return mapping


def main(*args, tickers, history, expires, strikes, interest, discount, fees, period, **kwargs):
    authenticators, accounts = load(AUTHENTICATORS), load(ACCOUNTS)
    symbols = list(map(Querys.Symbol, tickers))
    random.shuffle(symbols)
    symbols = Queues.FIFO(contents=symbols, capacity=None, timeout=None)
    technicals = [Concepts.Technicals.State.STATS, Concepts.Technicals.Trend.MACD, Concepts.Technicals.Volatility.ATR]

    with WebReader(delay=3) as source:
        bars_downloader = AlpacaBarsDownloader(name="BarsDownloader", source=source, authenticator=authenticators[Website.ALPACA, False])
        stock_downloader = AlpacaStockDownloader(name="StockDownloader", source=source, authenticator=authenticators[Website.ALPACA, False])
        contract_downloader = AlpacaContractDownloader(name="ContractDownloader", source=source, authenticator=authenticators[Website.ALPACA, False])
        option_downloader = AlpacaOptionDownloader(name="OptionDownloader", source=source, authenticator=authenticators[Website.ALPACA, False])
        technical_calculator = TechnicalCalculator(name="TechnicalCalculator", technicals=technicals)
        sanity_filter = SanityFilter(name="SanityFilter")
        viability_filter = ViabilityFilter(name="ViabilityFilter")
        option_calculator = OptionCalculator(name="OptionCalculator")
        greek_calculator = GreekCalculator(name="GreekCalculator")
        implied_calculator = ImpliedCalculator(name="ImpliedCalculator", low=1e-4, high=5.0, tol=1e-10, iters=100)

        while bool(symbols):
            symbol = symbols.read()
            bars = bars_downloader([symbol], history=history)
            stock = stock_downloader([symbol]).squeeze()
            stock["mean"] = (stock["bid"] * stock["demand"] + stock["ask"] * stock["supply"]) / (stock["demand"] + stock["supply"])
            stock["median"] = (stock["bid"] + stock["ask"]) / 2
            strikes = NumRange.create([stock["last"] * strikes.minimum, stock["last"] * strikes.maximum])
            contracts = contract_downloader([symbol], expires=expires, strikes=strikes)
            options = option_downloader(contracts)
            options["underlying"] = stock["median"]
            technicals = technical_calculator(bars, period=period)
            options = sanity_filter(options)
            options = viability_filter(options, spread=0.25, size=2)
            options = option_calculator(options, interest=interest)

            print(options)
            raise Exception()

            options = greek_calculator(options=options, interest=interest)
            options = implied_calculator(options=options, interest=interest)

            print(options)
            raise Exception()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    arguments, parameters = list(), dict()
    parameters["tickers"] = open(TICKERS, "r").read().splitlines()
    parameters["expires"] = DateRange.create([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52*1/12)).date()])
    parameters["history"] = DateRange.create([(Datetime.today() - Timedelta(weeks=52*1)).date(), (Datetime.today() - Timedelta(days=1)).date()])
    parameters["strikes"] = NumRange.create([0.95, 1.05])
    parameters.update({"interest": 0.05, "discount": 0.05, "fees": 3.00, "period": 252})
    main(*arguments, **parameters)



