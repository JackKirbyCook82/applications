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

from alpaca.market import AlpacaStockDownloader, AlpacaContractDownloader, AlpacaOptionDownloader
from alpaca.history import AlpacaBarsDownloader
from stocks import StockCalculator
from stocks.technicals import TechnicalCalculator
from options import OptionCalculator, SanityFilter, ViabilityFilter
from options.variances import VarianceCalculator, StandardizingCalculator
from options.localizing import PartitioningCalculator, Localizing
from options.volatility import VolatilityCalculator
from options.valuations import ValuationCalculator
from options.forwards import ForwardCalculator
from options.greeks import GreekCalculator
from finance.brokers import Authenticator, Brokerage
from finance.enumerations import Website, Technical, Terms, Tenure
from finance.querys import Symbol
from webscraping.webreaders import WebReader
from support.custom import NumRange, DateRange
from support.surface import SurfaceCreator

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


def main(*args, tickers, expires, history, strikes, term, tenure, period, interest, dividends, **kwargs):
    localizing = Localizing.create(radius=(0.05, 0.12, 0.01), window=(1, 3, 1), coverage=(3, 10), limit=45/365)
    brokerage = Brokerage(Website.ALPACA, False)
    authenticator = Authenticator.load(AUTHENTICATORS)[brokerage]
    symbols = list(map(Symbol, tickers))

    with WebReader(delay=1) as source:
        bars_downloader = AlpacaBarsDownloader(name="BarsDownloader", source=source, authenticator=authenticator)
        stock_downloader = AlpacaStockDownloader(name="StockDownloader", source=source, authenticator=authenticator)
        contract_downloader = AlpacaContractDownloader(name="ContractDownloader", source=source, authenticator=authenticator)
        option_downloader = AlpacaOptionDownloader(name="OptionDownloader", source=source, authenticator=authenticator)
        sanity_filter = SanityFilter(name="SanityFilter", size=5)
        viability_filter = ViabilityFilter(name="ViabilityFilter", active=0.30, money=0.15, tight=0.15)
        technical_calculator = TechnicalCalculator(name="TechnicalCalculator", technicals=[Technical.STATS])
        stock_calculator = StockCalculator(name="StockCalculator")
        option_calculator = OptionCalculator(name="OptionCalculator")
        forward_calculator = ForwardCalculator(name="ForwardCalculator", samplesize=5, tight=0.15)
        volatility_calculator = VolatilityCalculator(name="VolatilityCalculator", low=1e-4, high=5.0, tol=1e-10, iters=100)
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        greek_calculator = GreekCalculator(name="GreekCalculator")
        variance_calculator = VarianceCalculator(name="VarianceCalculator", neighbors=25, quantile=0.95, multiple=2.5)
        partitioning_calculator = PartitioningCalculator(name="PartitioningCalculator", localizing=localizing, samples=35, overlap=0.80)
        standardizing_calculator = StandardizingCalculator(name="StandardizingCalculator", neighbors=25)
        surface_creator = SurfaceCreator(name="SurfaceCreator", columns="tau|mae|tiv", quantity=35, gridsize=100, samplesize=5)

        for symbol in symbols:
            bars = bars_downloader(symbol, history=history)
            technicals = technical_calculator(bars, period=period)
            technicals = technicals[technicals["date"] <= pd.Timestamp.today()]
            technicals = technicals.sort_values(["ticker", "date"]).groupby("ticker", as_index=False).last()
            stocks = stock_downloader(symbol)
            stocks = stocks.merge(technicals[["ticker", "volatility", "trend"]], on="ticker", how="left", validate="many_to_one", sort=False)
            stocks = stock_calculator(stocks)
            stock = stocks[stocks["ticker"] == symbol.ticker].squeeze()
            strikes = NumRange.create([stock["last"] * strikes.minimum, stock["last"] * strikes.maximum])
            contracts = contract_downloader([symbol], expires=expires, strikes=strikes)
            options = option_downloader(contracts)
            options = options.merge(stocks[["ticker", "volatility", "trend"]], on="ticker", how="left", validate="many_to_one", sort=False)
            options = options.merge(stocks.rename(columns={"median": "spot"})[["ticker", "spot"]], on="ticker", how="left", validate="many_to_one", sort=False)
            options = sanity_filter(options)
            options = option_calculator(options)
            options = viability_filter(options)
            options = forward_calculator(options, interest=interest, dividends=dividends)
            options = valuation_calculator(options, interest=interest, dividends=dividends)
            options = volatility_calculator(options, interest=interest, dividends=dividends)
            options = greek_calculator(options, interest=interest, dividends=dividends)
            options = variance_calculator(options)
            for localized in partitioning_calculator(options):
                surface = surface_creator(localized, method="regression", smoothing=1/10, weights=None)
                localized = standardizing_calculator(localized, surface)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    arguments, parameters = list(), dict()
    parameters["tickers"] = ["SPY", "QQQ", "TSLA", "NVDA"]
    parameters["expires"] = DateRange.create([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52*1/12)).date()])
    parameters["history"] = DateRange.create([(Datetime.today() - Timedelta(weeks=52*2)).date(), (Datetime.today() - Timedelta(days=1)).date()])
    parameters["strikes"] = NumRange.create([0.95, 1.05])
    parameters.update({"term": Terms.LIMIT, "tenure": Tenure.DAY})
    parameters.update({"period": 252, "interest": np.log10(1 + 0.05), "dividends": np.log10(1 + 0.00)})
    main(*arguments, **parameters)



