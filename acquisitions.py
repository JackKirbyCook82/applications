# -*- coding: utf-8 -*-
"""
Created on Sun Oct 4 2026
@name:   Trading Acquisition Application
@author: Jack Kirby Cook
@file:   applications/acquisitions.py

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

from ibkr.website import IbkrSocket, IbkrSubscription
from ibkr.contracts import IbkrContractDownloader
from ibkr.spot import IbkrOptionSpotDownloader
from options import OptionCalculator, SanityFilter, ViabilityFilter, ViabilityMetrics
from options.settlements import SettlementCalculator
from options.forwards import ForwardCalculator
from options.volatility import ImpliedVolatilityCalculator
from options.greeks import GreekCalculator
from options.variances import VarianceCalculator, VarianceScreener, VarianceStandardizer
from options.localizing import PartitionCalculator, Localizing
from options.valuations import ValuationCalculator
from finance.querys import Symbol
from support.surface import SurfaceCreator, SurfaceError
from support.custom import DateRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


def main(*args, ticker, expires, today, zscore, interest, **kwargs):
    localizing = Localizing.create(radius=(0.05, 0.12, 0.01), window=(1, 3, 1), coverage=(3, 10), limit=45/252)
    viability = ViabilityMetrics(moneyness=0.15, tightness=0.15, activity=0.30)
    product = Symbol(ticker)

    with IbkrSocket(host="127.0.0.1", port=7497, delay=1) as source:
        settlement_calculator = SettlementCalculator(name="SettlementCalculator", today=today, zscore=zscore)
        contract_downloader = IbkrContractDownloader(name="ContractDownloader", source=source)
        option_downloader = IbkrOptionSpotDownloader(name="OptionDownloader", source=source)
        sanity_filter = SanityFilter(name="SanityFilter", size=5)
        option_calculator = OptionCalculator(name="OptionCalculator", today=today)
        viability_filter = ViabilityFilter(name="ViabilityFilter", metrics=viability)
        forward_calculator = ForwardCalculator(name="ForwardCalculator", samplesize=5, tightness=0.15)
        implied_calculator = ImpliedVolatilityCalculator(name="ImpliedVolatilityCalculator", low=1e-4, high=5.0, tol=1e-10, iters=100)
        greek_calculator = GreekCalculator(name="GreekCalculator")
        variance_calculator = VarianceCalculator(name="VarianceCalculator", today=today)
        partition_calculator = PartitionCalculator(name="PartitionCalculator", localizing=localizing, quality=50, overlap=0.80)
        variance_screener = VarianceScreener(name="VarianceScreener", neighbors=25, quantile=0.95, multiple=2.5)
        surface_creator = SurfaceCreator(name="SurfaceCreator", columns="tau|mae|tiv", quantity=35, gridsize=100, samplesize=5)
        variance_standardizer = VarianceStandardizer(name="VarianceStandardizer", neighbors=25)
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")

        with IbkrSubscription(source, product=product) as subscription:
            stock = subscription[product]
            underlying = (stock.price.bid + stock.price.ask) / 2
            volatility = stock.volatility.implied
            dividends = np.log1p(stock.dividends.predicted / underlying)
            parameters = dict(underlying=underlying, volatility=volatility, dividends=dividends)

            settlements = settlement_calculator(expires, interest=interest, **parameters)
            contracts = contract_downloader(product, settlements=settlements)
            options = option_downloader(contracts)
            options["underlying"] = underlying
            options["predicted"] = volatility
            options = sanity_filter(options)
            options = option_calculator(options)
            options = viability_filter(options)
            options = forward_calculator(options, interest=interest, dividends=dividends)
            options = implied_calculator(options, interest=interest, dividends=dividends, price="market")
            options = greek_calculator(options, interest=interest, dividends=dividends, volatility="implied")
            options = variance_calculator(options)
            for partition in partition_calculator(options):
                screened = variance_screener(partition)
                try: surface = surface_creator(screened, method="regression", smoothing=1/10, weights=None)
                except SurfaceError: continue
                partition = variance_standardizer(partition, surface)
                partition["tsv"] = surface(partition["tau"], partition["mae"])
                partition = partition.where(partition["tsv"] >= 0).dropna(how="all", inplace=False)
                partition["surfaced"] = np.sqrt(partition["tsv"] / partition["tau"])
                partition = valuation_calculator(partition, interest=interest, dividends=dividends, volatility="surfaced", valuation="forecast")
                if bool(partition.empty): continue


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    sysToday = Datetime.today().date()
    sysExpires = DateRange([sysToday + Timedelta(weeks=1), sysToday + Timedelta(weeks=52)])
    main(ticker="SPY", expires=sysExpires, today=sysToday, zscore=1.5, interest=np.log1p(0.05))

