# -*- coding: utf-8 -*-
"""
Created on Mon Jul 6 2026
@name:   Acquisition Application
@author: Jack Kirby Cook

"""

import sys
import logging
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path: sys.path.append(str(ROOT))
REPOSITORY = ROOT / "repository"
RESOURCES = ROOT / "resources"
AUTHENTICATORS = RESOURCES / "authenticators.txt"
ACCOUNTS = RESOURCES / "accounts.txt"
TICKERS = RESOURCES / "tickers.txt"
ASSETS = REPOSITORY / "assets.txt"

from alpaca.market import AlpacaStockDownloader, AlpacaContractDownloader, AlpacaOptionDownloader
from alpaca.history import AlpacaBarsDownloader
from stocks import StockCalculator
from stocks.technicals import TechnicalCalculator
from options import OptionCalculator, SanityFilter, ViabilityFilter
from finance.brokers import Authenticator, Brokerage, Asset
from finance.variables import Enumerations, Querys
from webscraping.webreaders import WebReader
from support.custom import NumRange, DateRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


def main(*args, tickers, expires, history, strikes, term, tenure, period, interest, dividends, **kwargs):
    symbols = list(map(Querys.Symbol, tickers))
    brokerage = Brokerage(Enumerations.Website.ALPACA, False)
    authenticator = Authenticator.load(AUTHENTICATORS)[brokerage]
    intent = Enumerations.Intents.OPEN
    assets = Asset.load(ASSETS)

    with WebReader(delay=1) as source:
        bars_downloader = AlpacaBarsDownloader(name="BarsDownloader", source=source, authenticator=authenticator)
        stock_downloader = AlpacaStockDownloader(name="StockDownloader", source=source, authenticator=authenticator)
        contract_downloader = AlpacaContractDownloader(name="ContractDownloader", source=source, authenticator=authenticator)
        option_downloader = AlpacaOptionDownloader(name="OptionDownloader", source=source, authenticator=authenticator)
        sanity_filter = SanityFilter(name="SanityFilter", size=5)
        viability_filter = ViabilityFilter(name="ViabilityFilter", active=0.30, money=0.15, tight=0.15)
        technical_calculator = TechnicalCalculator(name="TechnicalCalculator", technicals=[Enumerations.Technical.STATS])
        stock_calculator = StockCalculator(name="StockCalculator")
        option_calculator = OptionCalculator(name="OptionCalculator")

        for symbol in symbols:
            bars = bars_downloader([symbol], history=history)
            technicals = technical_calculator(bars, period=period)
            stocks = stock_downloader([symbol])
            stock = stock_calculator(stocks, technicals).squeeze()
            strikes = NumRange.create([stock["last"] * strikes.minimum, stock["last"] * strikes.maximum])
            contracts = contract_downloader([symbol], expires=expires, strikes=strikes)
            options = option_downloader(contracts)
            options["volatility"] = stock["volatility"]
            options["spot"] = stock["median"]
            options = sanity_filter(options)
            options = option_calculator(options)
            options = viability_filter(options)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    arguments, parameters = list(), dict()
    parameters["tickers"] = TICKERS.read_text().splitlines()
    parameters["expires"] = DateRange.create([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52*1/12)).date()])
    parameters["history"] = DateRange.create([(Datetime.today() - Timedelta(weeks=52*2)).date(), (Datetime.today() - Timedelta(days=1)).date()])
    parameters["strikes"] = NumRange.create([0.95, 1.05])
    parameters.update({"term": Enumerations.Terms.LIMIT, "tenure": Enumerations.Tenure.DAY})
    parameters.update({"period": 252, "interest": np.log10(1 + 0.05), "dividends": np.log10(1 + 0.00)})
    main(*arguments, **parameters)


