# -*- coding: utf-8 -*-
"""
Created on Mon Jul 6 2026
@name:   Trading Acquisitions Application
@author: Jack Kirby Cook
@file:   applications/acquisitions.py

"""

import sys
import logging
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import timedelta as Timedelta

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path: sys.path.append(str(ROOT))
REPOSITORY = ROOT / "repository"
RESOURCES = ROOT / "resources"
AUTHENTICATORS = RESOURCES / "authenticators.txt"
ACCOUNTS = RESOURCES / "accounts.txt"
ORDERS = REPOSITORY / "orders"

from solutions.options import OptionDownloading, OptionFiltering, OptionPricing, OptionValuing
from alpaca.market import AlpacaStockDownloader, AlpacaContractDownloader, AlpacaOptionDownloader
from alpaca.orders import AlpacaOrderUploader, AlpacaOrderFile
from options import OptionCalculator, SanityFilter, ViabilityFilter, ViabilityMetric
from options.localizing import PartitionCalculator, LocalizingVariables
from options.variances import VarianceCalculator, VarianceScreener, VarianceStandardizer
from options.acquisitions import AcquisitionCalculator, AcquisitionMetric
from options.volatility import VolatilityCalculator
from options.valuations import ValuationCalculator
from options.forwards import ForwardCalculator
from options.greeks import GreekCalculator
from options.prospects import ProspectSlippage, ProspectCosting
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


def main(*args, tickers, expires, strikes, term, tenure, interest, dividends, **kwargs):
    localizing = LocalizingVariables.create(radius=(0.05, 0.12, 0.01), window=(1, 3, 1), coverage=(3, 10), limit=45/365)
    slippage = ProspectSlippage(entry=0.25, exit=0.35)
    costing = ProspectCosting(slippage=slippage, commissions=0.65 / 100)
    acquiring = AcquisitionMetric(zspread=1.50, multiple=2.00, ratio=3.00)
    viability = ViabilityMetric(moneyness=0.15, tightness=0.15, activity=0.30)
    valuing = dict(method="regression", smoothing=1/10, weights=None)
    brokerage = Brokerage(Website.ALPACA, False)
    authenticator = Authenticator.load(AUTHENTICATORS)[brokerage]
    spreads = [Spread.FLY, Spread.CALENDAR]

    with WebReader(delay=1) as source:
        stock_downloader = AlpacaStockDownloader(name="StockDownloader", source=source, authenticator=authenticator)
        contract_downloader = AlpacaContractDownloader(name="ContractDownloader", source=source, authenticator=authenticator)
        option_downloader = AlpacaOptionDownloader(name="OptionDownloader", source=source, authenticator=authenticator)
        sanity_filter = SanityFilter(name="SanityFilter", size=5)
        option_calculator = OptionCalculator(name="OptionCalculator")
        viability_filter = ViabilityFilter(name="ViabilityFilter", metric=viability)
        volatility_calculator = VolatilityCalculator(name="VolatilityCalculator", low=1e-4, high=5.0, tol=1e-10, iters=100)
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        greek_calculator = GreekCalculator(name="GreekCalculator")
        forward_calculator = ForwardCalculator(name="ForwardCalculator", samplesize=5, tightness=0.15)
        variance_calculator = VarianceCalculator(name="VarianceCalculator")
        variance_screener = VarianceScreener(name="VarianceScreener", neighbors=25, quantile=0.95, multiple=2.5)
        variance_standardizer = VarianceStandardizer(name="VarianceStandardizer", neighbors=25)
        surface_creator = SurfaceCreator(name="SurfaceCreator", columns="tau|mae|tiv", quantity=35, gridsize=100, samplesize=5)
        partition_calculator = PartitionCalculator(name="PartitionCalculator", localizing=localizing, samples=35, overlap=0.80)
        acquisition_calculator = AcquisitionCalculator(name="AcquisitionCalculator", spreads=spreads, costing=costing, metric=acquiring, limit=1)
        order_uploader = AlpacaOrderUploader(name="AlpacaOrderUploader", source=source, authenticator=authenticator)
        orders_file = AlpacaOrderFile(name="AlpacaOrderFile", file=ORDERS)

        option_downloading = OptionDownloading(stocks=stock_downloader, contracts=contract_downloader, options=option_downloader)
        option_filtering = OptionFiltering(sanity=sanity_filter, options=option_calculator, viability=viability_filter)
        option_pricing = OptionPricing(volatility=volatility_calculator, greeks=greek_calculator, forward=forward_calculator, variance=variance_calculator)
        option_valuing = OptionValuing(screen=variance_screener, surface=surface_creator, standardize=variance_standardizer, valuation=valuation_calculator)

        symbols = list(map(Symbol, tickers))
        for symbol in symbols:
            options = option_downloading(symbol, expires=expires, strikes=strikes)
            options = option_filtering(options)
            options = option_pricing(options, interest=interest, dividends=dividends)
            for partition in partition_calculator(options):
                partition = option_valuing(partition, interest=interest, dividends=dividends, **valuing)
                acquisitions = acquisition_calculator(partition)
                if not bool(acquisitions): continue
                orders = order_uploader(acquisitions, term=term, tenure=tenure)
                orders_file.save(orders, mode="a")
                return


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



