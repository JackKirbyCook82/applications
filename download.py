# -*- coding: utf-8 -*-
"""
Created on Weds Oct 28 2026
@name:   Acquisition Downloading Application
@author: Jack Kirby Cook
@file:   applications/download.py

"""

import sys
import logging
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Callable
from dataclasses import dataclass
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path: sys.path.append(str(ROOT))
REPOSITORY = ROOT / "repository"
RESOURCES = ROOT / "resources"

from ibkr.ticks import IbkrTradeOptionTicksDownloader, IbkrQuoteOptionTicksDownloader
from ibkr.contracts import IbkrContractDownloader
from ibkr.website import IbkrSocket, IbkrSubscription
from finance.querys import Symbol
from support.custom import NumberRange, DateRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


@dataclass(frozen=False)
class Downloader:
    contracts: Callable; ticks: Callable; quotes: Callable

    def __init__(self, *args, source, **kwargs):
        self.contracts = IbkrContractDownloader(name="ContractDownloader", source=source)
        self.quotes = IbkrQuoteOptionTicksDownloader(name="QuotesDownloader", source=source)
        self.ticks = IbkrTradeOptionTicksDownloader(name="TicksDownloader", source=source)


def main(*args, ticker, expires, history, zscore, interest, **kwargs):
    product = Symbol(ticker)
    today = Datetime.today().date()
    with IbkrSocket(host="127.0.0.1", port=7497, delay=1) as source:
        downloader = Downloader(source=source)
        with IbkrSubscription(source, product=product) as subscription:
            stock = subscription[product]
            spot = stock.price.trade
            volatility = stock.volatility.implied
            dividends = np.log1p(stock.dividends.predicted / spot)
        tau = (max(expires) + history) / 365
        forward = spot * np.exp(np.subtract(interest, dividends) * tau)
        strikes = NumberRange([forward * np.exp(zscore * index * volatility * np.sqrt(tau)) for index in (-1, +1)])
        expires = DateRange([today + Timedelta(days=days) for days in expires])
        history = DateRange([today - Timedelta(days=1), today - Timedelta(days=history)])
        contracts = downloader.contracts(product, expires=expires, strikes=strikes)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    main(ticker="SPY", expires=(7, 7*4), history=20, zscore=1.5, interest=np.log1p(0.05))



