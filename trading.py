# -*- coding: utf-8 -*-
"""
Created on Weds Mar 18 2026
@name:   Trading Application
@author: Jack Kirby Cook

"""

import sys
import queue
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
PORTFOLIO = REPOSITORY / "portfolio.csv"
AUTHENTICATORS = RESOURCES / "authenticators.txt"
ACCOUNTS = RESOURCES / "accounts.txt"
TICKERS = RESOURCES / "tickers.txt"

from alpaca.market import AlpacaStockDownloader, AlpacaContractDownloader, AlpacaOptionDownloader
from alpaca.history import AlpacaBarsDownloader
from alpaca.orders import AlpacaSpreadUploader
from stocks import StockCalculator
from stocks.technicals import TechnicalCalculator
from options import OptionCalculator, SanityFilter, ViabilityFilter
from options.volatility import VolatilityCalculator
from options.valuations import ValuationCalculator
from options.forwards import ForwardCalculator
from options.greeks import GreekCalculator
from options.variances import VarianceCalculator, StandardizingCalculator
from options.localizing import LocalizingCalculator, LocalizingVariables
from options.spreads import SpreadCalculator, SpreadMetrics
from options.prospects import ProspectCalculator, PriorityCalculator
from finance.brokers import Authenticator, Account
from finance.variables import Enumerations, Querys
from webscraping.webreaders import WebReader
from support.custom import NumRange, DateRange
from support.surface import SurfaceCreator

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


def main(*args, tickers, history, expires, strikes, period, interest, dividends, term, tenure, **kwargs):
    authenticators, accounts, symbols = Authenticator.load(AUTHENTICATORS), Account.load(ACCOUNTS), queue.Queue()
    for ticker in tickers: symbols.put(Querys.Symbol(ticker))
    spreads = [Enumerations.Spread.FLY, Enumerations.Spread.CALENDAR]
    technicals = [Enumerations.Technical.STATS]
    variables = LocalizingVariables.create(radius=(0.05, 0.12, 0.01), window=(1, 3, 1), coverage=(3, 10), limit=45/365)
    calendar = SpreadMetrics.create(ratios={"gap": +0.50, "theta": -0.35, "vega": +0.00}, zscore=0.50, profit=0.00)
    fly = SpreadMetrics.create(ratios={"gap": +0.50, "theta": -0.25}, zscore=0.75, profit=0.00)
    metrics = dict(calendar=calendar, fly=fly)

    with WebReader(delay=1) as source:
        spread_uploader = AlpacaSpreadUploader(name="SpreadUploader", source=source, authenticator=authenticators[Enumerations.Website.ALPACA, False], file=PORTFOLIO)
        bars_downloader = AlpacaBarsDownloader(name="BarsDownloader", source=source, authenticator=authenticators[Enumerations.Website.ALPACA, False])
        stock_downloader = AlpacaStockDownloader(name="StockDownloader", source=source, authenticator=authenticators[Enumerations.Website.ALPACA, False])
        contract_downloader = AlpacaContractDownloader(name="ContractDownloader", source=source, authenticator=authenticators[Enumerations.Website.ALPACA, False])
        option_downloader = AlpacaOptionDownloader(name="OptionDownloader", source=source, authenticator=authenticators[Enumerations.Website.ALPACA, False])
        sanity_filter = SanityFilter(name="SanityFilter", size=5)
        viability_filter = ViabilityFilter(name="ViabilityFilter", active=0.30, money=0.15, tight=0.15)
        surface_creator = SurfaceCreator(name="SurfaceCreator", columns="tau|mae|tiv", quantity=35, gridsize=100, samplesize=5)
        stock_calculator = StockCalculator(name="StockCalculator")
        technical_calculator = TechnicalCalculator(name="TechnicalCalculator", technicals=technicals)
        option_calculator = OptionCalculator(name="OptionCalculator")
        forward_calculator = ForwardCalculator(name="ForwardCalculator", samplesize=5, tight=0.15)
        volatility_calculator = VolatilityCalculator(name="VolatilityCalculator", low=1e-4, high=5.0, tol=1e-10, iters=100)
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        greek_calculator = GreekCalculator(name="GreekCalculator")
        variance_calculator = VarianceCalculator(name="VarianceCalculator", neighbors=25, quantile=0.95, multiple=2.5)
        localizing_calculator = LocalizingCalculator(name="LocalizingCalculator", variables=variables, samples=35, overlap=0.80)
        standardizing_calculator = StandardizingCalculator(name="StandardizingCalculator", neighbors=25)
        spread_calculator = SpreadCalculator(name="SpreadCalculator", spreads=spreads, limit=1)
        prospect_calculator = ProspectCalculator(name="ProspectCalculator", metrics=metrics)
        priority_calculator = PriorityCalculator(name="PriorityCalculator")

        while not symbols.empty():
            symbol = symbols.get()
            bars = bars_downloader([symbol], history=history)
            stocks = stock_downloader([symbol])
            technicals = technical_calculator(bars, period=period)
            stock = stock_calculator(stocks, technicals).squeeze()
            strikes = NumRange.create([stock["last"] * strikes.minimum, stock["last"] * strikes.maximum])
            contracts = contract_downloader([symbol], expires=expires, strikes=strikes)
            options = option_downloader(contracts)
            options["volatility"] = stock["volatility"]
            options["spot"] = stock["median"]
            options = sanity_filter(options)
            options = option_calculator(options)
            options = viability_filter(options)
            options = forward_calculator(options, interest=interest, dividends=dividends)
            options = valuation_calculator(options, interest=interest, dividends=dividends)
            options = volatility_calculator(options, interest=interest, dividends=dividends)
            options = greek_calculator(options, interest=interest, dividends=dividends)
            options = variance_calculator(options)
            localizer = localizing_calculator(options)
            for localized in localizer:
                surface = surface_creator(localized, method="regression", smoothing=1/10, weights=None)
                localized = standardizing_calculator(localized, surface)
                spreads = spread_calculator(localized)
                spreads = prospect_calculator(spreads)
                spreads = priority_calculator(spreads)
                spread_uploader(spreads, term=term, tenure=tenure)


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
    parameters.update({"term": Enumerations.Terms.LIMIT, "tenure": Enumerations.Tenure.DAY, "intent": Enumerations.Intents.OPEN})
    parameters.update({"period": 252, "interest": np.log10(1 + 0.05), "dividends": np.log10(1 + 0.00)})
    main(*arguments, **parameters)



