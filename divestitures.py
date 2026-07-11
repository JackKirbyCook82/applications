# -*- coding: utf-8 -*-
"""
Created on Mon Jul 6 2026
@name:   Divestiture Application
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

from alpaca.portfolio import AlpacaPortfolioDownloader
from alpaca.market import AlpacaStockDownloader, AlpacaOptionDownloader
from alpaca.history import AlpacaBarsDownloader
from stocks import StockCalculator
from stocks.technicals import TechnicalCalculator
from options import OptionCalculator, SanityFilter, ViabilityFilter
from options.volatility import VolatilityCalculator
from options.valuations import ValuationCalculator
from options.greeks import GreekCalculator
from finance.brokers import Authenticator, Brokerage
from finance.enumerations import Website, Technical, Terms, Tenure
from finance.querys import Contract, Symbol
from webscraping.webreaders import WebReader
from support.custom import DateRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


def main(*args, history, term, tenure, period, interest, dividends, **kwargs):
    brokerage = Brokerage(Website.ALPACA, False)
    authenticator = Authenticator.load(AUTHENTICATORS)[brokerage]

    with WebReader(delay=1) as source:
        portfolio_downloader = AlpacaPortfolioDownloader(name="PortfolioDownloader", source=source, authenticator=authenticator)
        bars_downloader = AlpacaBarsDownloader(name="BarsDownloader", source=source, authenticator=authenticator)
        stock_downloader = AlpacaStockDownloader(name="StockDownloader", source=source, authenticator=authenticator)
        option_downloader = AlpacaOptionDownloader(name="OptionDownloader", source=source, authenticator=authenticator)
        sanity_filter = SanityFilter(name="SanityFilter", size=5)
        viability_filter = ViabilityFilter(name="ViabilityFilter", active=0.30, money=0.15, tight=0.15)
        technical_calculator = TechnicalCalculator(name="TechnicalCalculator", technicals=[Technical.STATS])
        stock_calculator = StockCalculator(name="StockCalculator")
        option_calculator = OptionCalculator(name="OptionCalculator")
        volatility_calculator = VolatilityCalculator(name="VolatilityCalculator", low=1e-4, high=5.0, tol=1e-10, iters=100)
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        greek_calculator = GreekCalculator(name="GreekCalculator")

        portfolio = portfolio_downloader()
        contracts = portfolio[list(Contract)].apply(lambda series: Contract(series.to_dict()), axis=1)
        options = option_downloader(contracts)
        portfolio = options.merge(options, on=list(Contract), how="left", validate="many_to_one", sort=False)
        symbols = list(map(Symbol, portfolio["ticker"].to_list()))
        bars = bars_downloader(symbols, history=history)
        technicals = technical_calculator(bars, period=period)
        technicals = technicals[technicals["date"] <= pd.Timestamp.today()]
        technicals = technicals.sort_values(["ticker", "date"]).groupby("ticker", as_index=False).last()
        stocks = stock_downloader(symbols)
        stocks = stocks.merge(technicals[["ticker", "volatility", "trend"]], on="ticker", how="left", validate="many_to_one", sort=False)
        stocks = stock_calculator(stocks)
        portfolio = portfolio.merge(stocks[["ticker", "volatility", "trend"]], on="ticker", how="left", validate="many_to_one", sort=False)
        portfolio = portfolio.merge(stocks.rename(columns={"median": "spot"})[["ticker", "spot"]], on="ticker", how="left", validate="many_to_one", sort=False)
        portfolio = sanity_filter(portfolio)
        portfolio = option_calculator(portfolio)
        portfolio = viability_filter(portfolio)
        portfolio = valuation_calculator(portfolio, interest=interest, dividends=dividends)
        portfolio = volatility_calculator(portfolio, interest=interest, dividends=dividends)
        portfolio = greek_calculator(portfolio, interest=interest, dividends=dividends)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    arguments, parameters = list(), dict()
    parameters["history"] = DateRange.create([(Datetime.today() - Timedelta(weeks=52*2)).date(), (Datetime.today() - Timedelta(days=1)).date()])
    parameters.update({"term": Terms.LIMIT, "tenure": Tenure.DAY})
    parameters.update({"period": 252, "interest": np.log10(1 + 0.05), "dividends": np.log10(1 + 0.00)})
    main(*arguments, **parameters)



