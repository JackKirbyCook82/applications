# -*- coding: utf-8 -*-
"""
Created on Mon Jul 6 2026
@name:   Divestiture Application
@author: Jack Kirby Cook

"""

import sys
import logging
import warnings
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

from alpaca.portfolio import AlpacaPortfolioDownloader
from alpaca.market import AlpacaStockDownloader, AlpacaOptionDownloader
from alpaca.history import AlpacaBarsDownloader
from stocks import StockCalculator
from stocks.technicals import TechnicalCalculator
from finance.brokers import Authenticator, Brokerage
from finance.variables import Enumerations, Querys
from webscraping.webreaders import WebReader
from support.custom import DateRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


def main(*args, history, term, tenure, period, **kwargs):
    brokerage = Brokerage(Enumerations.Website.ALPACA, False)
    authenticator = Authenticator.load(AUTHENTICATORS)[brokerage]
    intent = Enumerations.Intents.CLOSE

    with WebReader(delay=1) as source:
        portfolio_downloader = AlpacaPortfolioDownloader(name="PortfolioDownloader", source=source, authenticator=authenticator)
        bars_downloader = AlpacaBarsDownloader(name="BarsDownloader", source=source, authenticator=authenticator)
        stock_downloader = AlpacaStockDownloader(name="StockDownloader", source=source, authenticator=authenticator)
        option_downloader = AlpacaOptionDownloader(name="OptionDownloader", source=source, authenticator=authenticator)
        technical_calculator = TechnicalCalculator(name="TechnicalCalculator", technicals=[Enumerations.Technical.STATS])
        stock_calculator = StockCalculator(name="StockCalculator")

        portfolio = portfolio_downloader()
        symbols = portfolio[list(Querys.Symbol)].apply(Querys.Symbol, axis=1).to_list()
        bars = bars_downloader(symbols, history=history)
        technicals = technical_calculator(bars, period=period)
        technicals = technicals[technicals["date"] <= pd.Timestamp.today()]
        technicals = technicals.sort_values(["ticker", "date"]).groupby("ticker", as_index=False).last()
        stocks = stock_downloader(symbols)
        stocks = stocks.merge(technicals, on=["ticker", "date"], how="left", validate="many_to_one")
        stocks = stock_calculator(stocks)

        contracts = portfolio[list(Querys.Contract)].apply(Querys.Contract, axis=1).to_list()
        options = option_downloader(contracts)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    arguments, parameters = list(), dict()
    parameters["history"] = DateRange.create([(Datetime.today() - Timedelta(weeks=52*2)).date(), (Datetime.today() - Timedelta(days=1)).date()])
    parameters.update({"term": Enumerations.Terms.LIMIT, "tenure": Enumerations.Tenure.DAY})
    parameters.update({"period": 252})
    main(*arguments, **parameters)
