import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

pd.set_option("display.width", None)
pd.set_option("display.max_columns", None)

from factor import compute_pool_factors, FACTOR_NAMES
from filter import get_universe_pool, UNIVERSE_SIZE

START_DATE = "2017-01-01"
END_DATE = "2026-04-07"

pool_df = get_universe_pool(
    start_date=START_DATE,
    end_date=END_DATE,
    universe_size=UNIVERSE_SIZE,
)
factor_df = compute_pool_factors(
    pool_name=f"smallcap{UNIVERSE_SIZE}",
    pool_df=pool_df[["date", "instrument"]],
    start_date=START_DATE,
    end_date=END_DATE,
    factor_names=FACTOR_NAMES,
)
factor_df["year"] = factor_df["date"].dt.year

for factor_name in FACTOR_NAMES:
    print(f"\n{'='*60}\n{factor_name}\n{'='*60}")
    print(factor_df.groupby("year")[factor_name].describe())
    
    plt.figure(figsize=(10, 6))
    years = sorted(factor_df["year"].unique())
    for year in years:
        year_data = factor_df.loc[factor_df["year"] == year, factor_name].dropna()
        if len(year_data) < 100:
            continue
        plt.hist(year_data, bins=50, alpha=0.5, label=str(year), density=True)
    plt.title(factor_name)
    plt.xlabel("value")
    plt.ylabel("density")
    plt.legend(loc="upper right", fontsize=8)
    plt.tight_layout()
    plt.show()
