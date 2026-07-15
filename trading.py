# -*- coding: utf-8 -*-
"""
Created on Mon Jul 6 2026
@name:   Market Application
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

from solutions.markets import MarketDownloaders, MarketCalculators, MarketFilters, StocksDownloader, OptionsDownloader
from solutions.finance import PricingCalculators, ImpliedCalculators, FinanceComputation
from solutions.localizing import LocalizingComputation
from alpaca.market import AlpacaStockDownloader, AlpacaContractDownloader, AlpacaOptionDownloader
from alpaca.portfolio import AlpacaPortfolioDownloader
from alpaca.history import AlpacaBarsDownloader
from stocks import StockCalculator
from stocks.technicals import TechnicalCalculator
from options import OptionCalculator, SanityFilter, ViabilityFilter
from options.localizing import PartitionCalculator, ProximityCalculator, Localizing
from options.variances import VarianceCalculator, StandardizationCalculator
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
        bars = AlpacaBarsDownloader(name="BarsDownloader", source=source, authenticator=authenticator)
        stocks = AlpacaStockDownloader(name="StockDownloader", source=source, authenticator=authenticator)
        contracts = AlpacaContractDownloader(name="ContractDownloader", source=source, authenticator=authenticator)
        options = AlpacaOptionDownloader(name="OptionDownloader", source=source, authenticator=authenticator)
        downloaders = MarketDownloaders(bars=bars, stocks=stocks, contracts=contracts, options=options)

        technicals = TechnicalCalculator(name="TechnicalCalculator", technicals=[Technical.STATS])
        stocks = StockCalculator(name="StockCalculator")
        options = OptionCalculator(name="OptionCalculator")
        calculators = MarketCalculators(technicals=technicals, stocks=stocks, options=options)

        sanity = SanityFilter(name="SanityFilter", size=5)
        viability = ViabilityFilter(name="ViabilityFilter", active=0.30, money=0.15, tight=0.15)
        filters = MarketFilters(sanity=sanity, viability=viability)

        valuation = ValuationCalculator(name="ValuationCalculator")
        greeks = GreekCalculator(name="GreekCalculator")
        pricing = PricingCalculators(valuation=valuation, greeks=greeks)

        forward = ForwardCalculator(name="ForwardCalculator", samplesize=5, tightness=0.15)
        volatility = VolatilityCalculator(name="VolatilityCalculator", low=1e-4, high=5.0, tol=1e-10, iters=100)
        variance = VarianceCalculator(name="VarianceCalculator", neighbors=25, quantile=0.95, multiple=2.5)
        implied = ImpliedCalculators(forward=forward, volatility=volatility, variance=variance)

        surface = SurfaceCreator(name="SurfaceCreator", columns="tau|mae|tiv", quantity=35, gridsize=100, samplesize=5)
        partitions = PartitionCalculator(name="PartitionCalculator", localizing=localizing, samples=35, overlap=0.80)
        proximity = ProximityCalculator(name="ProximityCalculator", localizing=localizing, samples=35, overlap=0.80)
        standardization = StandardizationCalculator(name="StandardizationCalculator", neighbors=25)

        portfolio_downloader = AlpacaPortfolioDownloader(name="PortfolioDownloader", source=source, authenticator=authenticator)
        stock_downloader = StocksDownloader(downloaders=downloaders, calculators=calculators)
        option_downloader = OptionsDownloader(downloaders=downloaders, calculators=calculators, filters=filters)
        finance_computation = FinanceComputation(pricing=pricing, implied=implied)
        localizing_computation = LocalizingComputation(surface=surface, partitions=partitions, proximity=proximity, standardization=standardization)

        portfolio = portfolio_downloader()
        for symbol in symbols:
            stock = stock_downloader(symbol, history=history, period=period)
            options = option_downloader(stock, expires=expires, strikes=strikes)
            options = finance_computation(options, interest=interest, dividends=dividends)
            localizing = localizing_computation(options, method="regression", smoothing=1/10, weights=None)
            for localized in localizing:
                pass


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



