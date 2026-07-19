# -*- coding: utf-8 -*-
"""
Created on Mon Jul 6 2026
@name:   Trading Acquisitions Application
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

from solutions.trading import OptionDownloading, OptionFiltering, OptionCalculating, OptionLocalizing
from alpaca.market import AlpacaStockDownloader, AlpacaContractDownloader, AlpacaOptionDownloader
from alpaca.portfolio import AlpacaPortfolioDownloader
from options import OptionCalculator, SanityFilter, ViabilityFilter
from options.localizing import PartitionCalculator, ProximityCalculator, Localizing
from options.variances import VarianceCalculator, VarianceScreener, VarianceStandardizer
from options.volatility import VolatilityCalculator
from options.forwards import ForwardCalculator
from options.greeks import GreekCalculator
from finance.brokers import Authenticator, Brokerage
from finance.enumerations import Website, Terms, Tenure
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
        portfolio_downloader = AlpacaPortfolioDownloader(name="PortfolioDownloader", source=source, authenticator=authenticator)
        stocks = AlpacaStockDownloader(name="StockDownloader", source=source, authenticator=authenticator)
        contracts = AlpacaContractDownloader(name="ContractDownloader", source=source, authenticator=authenticator)
        options = AlpacaOptionDownloader(name="OptionDownloader", source=source, authenticator=authenticator)
        downloading = OptionDownloading(stocks=stocks, contracts=contracts, options=options)

        sanity = SanityFilter(name="SanityFilter", size=5)
        options = OptionCalculator(name="OptionCalculator")
        viability = ViabilityFilter(name="ViabilityFilter", active=0.30, money=0.15, tight=0.15)
        filtering = OptionFiltering(sanity=sanity, options=options, viability=viability)

        forward = ForwardCalculator(name="ForwardCalculator", samplesize=5, tightness=0.15)
        volatility = VolatilityCalculator(name="VolatilityCalculator", low=1e-4, high=5.0, tol=1e-10, iters=100)
        variance = VarianceCalculator(name="VarianceCalculator")
        screener = VarianceScreener(name="VarianceScreener", neighbors=25, quantile=0.95, multiple=2.5)
        greeks = GreekCalculator(name="GreekCalculator")
        computing = OptionCalculating(forward=forward, volatility=volatility, variance=variance, screener=screener, greeks=greeks)

        surface = SurfaceCreator(name="SurfaceCreator", columns="tau|mae|tiv", quantity=35, gridsize=100, samplesize=5)
        partitions = PartitionCalculator(name="PartitionCalculator", localizing=localizing, samples=35, overlap=0.80)
        proximity = ProximityCalculator(name="ProximityCalculator", localizing=localizing, samples=35, overlap=0.80)
        localizing = OptionLocalizing(surface=surface, partitions=partitions, proximity=proximity)

        standardize = VarianceStandardizer(name="VarianceStandardizer", neighbors=25)

        portfolio = portfolio_downloader()
        for symbol in symbols:
            options = downloading(symbol, expires=expires, strikes=strikes)
            options = filtering(options)
            options = computing(options, interest=interest, dividends=dividends)


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



