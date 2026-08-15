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
from typing import Callable
from dataclasses import dataclass
from datetime import timedelta as Timedelta

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path: sys.path.append(str(ROOT))
REPOSITORY = ROOT / "repository"
RESOURCES = ROOT / "resources"
AUTHENTICATORS = RESOURCES / "authenticators.txt"
ACCOUNTS = RESOURCES / "accounts.txt"
ORDERS = REPOSITORY / "orders"

from solutions.options import OptionDownloading, OptionFiltering, OptionMarketing, OptionSurfacer, OptionForecasting
from alpaca.market import AlpacaStockDownloader, AlpacaContractDownloader, AlpacaOptionDownloader
from alpaca.orders import AlpacaOrderUploader, AlpacaOrderUploadingFile
from options import OptionCalculator, SanityFilter, ViabilityFilter
from options.localizing import PartitionCalculator, Variables
from options.variances import VarianceCalculator, VarianceScreener, VarianceStandardizer
from options.acquisitions import AcquisitionCalculator, Metrics, Weights, Targets, Priority
from options.volatility import VolatilityCalculator
from options.valuations import ValuationCalculator
from options.forwards import ForwardCalculator
from options.greeks import GreekCalculator
from options.prospects import Slippage, Costing
from finance.brokers import Authenticator, Brokerage
from finance.enumerations import Website, Terms, Tenure, Spread
from finance.querys import Symbol
from webscraping.webreaders import WebReader
from support.custom import DateRange, NumberRange
from support.surface import SurfaceCreator

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


@dataclass(frozen=True)
class Marketing:
    downloading: Callable; filtering: Callable; marketing: Callable

    def __call__(self, symbols, /, expires, strikes, interest, dividends):
        for symbol in symbols:
            options = self.downloading(symbol, expires=expires, strikes=strikes)
            options = self.filtering(options)
            options = self.marketing(options, interest=interest, dividends=dividends)
            yield options


@dataclass(frozen=True)
class Localizing:
    partitions: Callable; surfacing: Callable; forecasting: Callable

    def __call__(self, options, /, interest, dividends):
        for localized in self.partitions(options):
            surface = self.surfacing(localized, method="regression", smoothing=1 / 10, weights=None)
            localized = self.forecasting(localized, surface, interest=interest, dividends=dividends)
            yield localized


def main(*args, tickers, expires, strikes, term, tenure, interest, dividends, **kwargs):
    variables = Variables(radius=(0.05, 0.12, 0.01), window=(1, 3, 1), coverage=(3, 10), limit=45/365)
    slippage = Slippage(entry=0.25, exit=0.35)
    costing = Costing(slippage=slippage, commissions=0.65)
    metrics = Metrics(zspread=2.0, multiple=3.0, ratio=10.0)
    targets = Targets(zspread=3.0, multiple=5.0, ratio=20.0)
    weights = Weights(zspread=0.30, multiple=0.30, ratio=0.40)
    priority = Priority(targets=targets, weights=weights)
    file = AlpacaOrderUploadingFile(file=ORDERS)
    brokerage = Brokerage(Website.ALPACA, False)
    authenticator = Authenticator.load(AUTHENTICATORS)[brokerage]
    spreads = [Spread.FLY, Spread.CALENDAR]
    symbols = list(map(Symbol, tickers))

    with WebReader(delay=1) as source:
        stock_downloader = AlpacaStockDownloader(name="StockDownloader", source=source, authenticator=authenticator)
        contract_downloader = AlpacaContractDownloader(name="ContractDownloader", source=source, authenticator=authenticator)
        option_downloader = AlpacaOptionDownloader(name="OptionDownloader", source=source, authenticator=authenticator)
        sanity_filter = SanityFilter(name="SanityFilter", size=5)
        option_calculator = OptionCalculator(name="OptionCalculator")
        viability_filter = ViabilityFilter(name="ViabilityFilter", active=0.30, money=0.15, tight=0.15)
        volatility_calculator = VolatilityCalculator(name="VolatilityCalculator", low=1e-4, high=5.0, tol=1e-10, iters=100)
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        greek_calculator = GreekCalculator(name="GreekCalculator")
        forward_calculator = ForwardCalculator(name="ForwardCalculator", samplesize=5, tightness=0.15)
        variance_calculator = VarianceCalculator(name="VarianceCalculator")
        variance_screener = VarianceScreener(name="VarianceScreener", neighbors=25, quantile=0.95, multiple=2.5)
        variance_standardizer = VarianceStandardizer(name="VarianceStandardizer", neighbors=25)
        surface_creator = SurfaceCreator(name="SurfaceCreator", columns="tau|mae|tiv", quantity=35, gridsize=100, samplesize=5)
        partition_calculator = PartitionCalculator(name="PartitionCalculator", variables=variables, samples=35, overlap=0.80)
        acquisition_calculator = AcquisitionCalculator(name="AcquisitionCalculator", spreads=spreads, costing=costing, metrics=metrics, priority=priority, limit=1)
        acquisition_uploader = AlpacaOrderUploader(name="AlpacaOrderUploader", source=source, file=file, authenticator=authenticator)

        downloading = OptionDownloading(stocks=stock_downloader, contracts=contract_downloader, options=option_downloader)
        filtering = OptionFiltering(sanity=sanity_filter, options=option_calculator, viability=viability_filter)
        marketing = OptionMarketing(volatility=volatility_calculator, greeks=greek_calculator, forward=forward_calculator, variance=variance_calculator)
        surfacing = OptionSurfacer(screener=variance_screener, surface=surface_creator)
        forecasting = OptionForecasting(standardize=variance_standardizer, valuation=valuation_calculator)
        marketing = Marketing(downloading, filtering, marketing)
        localizing = Localizing(partition_calculator, surfacing, forecasting)

        for options in marketing(symbols, expires=expires, strikes=strikes, interest=interest, dividends=dividends):
            for localized in localizing(options, interest=interest, dividends=dividends):
                acquisitions = acquisition_calculator(localized)
                orders = acquisition_uploader(acquisitions, term=term, tenure=tenure)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    arguments, parameters = list(), dict()
    parameters["tickers"] = ["SPY", "QQQ", "TSLA", "NVDA"]
    parameters["expires"] = lambda tomorrow: DateRange(tomorrow + Timedelta(weeks=1), tomorrow + Timedelta(weeks=52))
    parameters["strikes"] = lambda underlying: NumberRange(0.95 * underlying, 1.05 * underlying)
    parameters.update({"term": Terms.LIMIT, "tenure": Tenure.DAY})
    parameters.update({"interest": np.log10(1 + 0.05), "dividends": np.log10(1 + 0.00)})
    main(*arguments, **parameters)



