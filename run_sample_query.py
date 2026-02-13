"""
Run the provided permit query against a synthetic sample dataset and write charts.

Outputs (written to charts/):
- jurisdiction_totals.png
- jurisdiction_type_stacked.png
- type_totals.png
- permit_counts_sample.csv
"""

import numpy as np
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
from pathlib import Path


def main() -> None:
    np.random.seed(42)

    jurisdictions = [
        "San Jose",
        "San Francisco",
        "Oakland",
        "Berkeley",
        "Palo Alto",
        "San Mateo",
    ]

    types = [
        "Residential building",
        "Residential multi-family",
        "Building addition",
        "Construction fire",
        "Construction combo",
        "General permit residential",
        "Encroachment permit residential",  # should be filtered out
        "Commercial building",  # filtered out
        "Solar PV residential",  # filtered out
    ]

    rows = []
    start = pd.Timestamp("2025-05-01")
    end = pd.Timestamp("2026-01-15")
    for _ in range(300):
        file_date = start + (end - start) * np.random.rand()
        jurisdiction = np.random.choice(
            jurisdictions, p=[0.22, 0.18, 0.18, 0.15, 0.15, 0.12]
        )
        t = np.random.choice(
            types, p=[0.2, 0.18, 0.18, 0.08, 0.08, 0.08, 0.05, 0.08, 0.07]
        )
        rows.append((file_date.date(), jurisdiction, t))

    df = pd.DataFrame(rows, columns=["file_date", "jurisdiction", "type"])

    con = duckdb.connect(database=":memory:")
    con.register("reporting_db_combined_data", df)
    query = """
    select
    COUNT(*) as type_count,
    jurisdiction,
    type
    FROM reporting_db_combined_data
    where file_date >= '2025-06-01'
      and (type ilike '%residential%' or type ilike '%building%' or type ilike '%multi%' or type IN ('Construction fire', 'Construction combo') or type ilike '%general permit%')
      and type not in ('Photovoltaic residential', 'Encroachment permit residential')
      and type not ilike '%commercial%' and type not ilike '%solar%' and type not ilike '%sewer%' and type not ilike '%hvac%' and type not ilike '%water heater%' and type not ilike '%pool%' and type not ilike '%battery%'
      and type not ilike '%furnace%' and type not ilike '%plumbing%' and type not ilike '%electric%' and type not ilike '%fire alarm%' and type not ilike '%fire sprinklers%'
    group by all
    order by type_count desc;
    """
    result = con.execute(query).fetchdf()

    charts_dir = Path("charts")
    charts_dir.mkdir(exist_ok=True)

    agg_juris = result.groupby("jurisdiction")["type_count"].sum().sort_values(ascending=False)
    plt.figure(figsize=(8, 4))
    agg_juris.plot(kind="bar", color="#1f77b4")
    plt.title("Permits by Jurisdiction (filtered)")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(charts_dir / "jurisdiction_totals.png", dpi=150)
    plt.close()

    pivot = result.pivot_table(index="jurisdiction", columns="type", values="type_count", fill_value=0)
    pivot = pivot.loc[agg_juris.index]  # align order
    pivot.plot(kind="bar", stacked=True, figsize=(10, 5))
    plt.title("Permits by Jurisdiction and Type")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(charts_dir / "jurisdiction_type_stacked.png", dpi=150)
    plt.close()

    agg_type = result.groupby("type")["type_count"].sum().sort_values(ascending=False)
    plt.figure(figsize=(8, 4))
    agg_type.plot(kind="barh", color="#ff7f0e")
    plt.title("Permits by Type (overall)")
    plt.xlabel("Count")
    plt.tight_layout()
    plt.savefig(charts_dir / "type_totals.png", dpi=150)
    plt.close()

    result.to_csv(charts_dir / "permit_counts_sample.csv", index=False)
    print("Rows returned:", len(result))
    print(result.head())
    print("Charts written to", charts_dir)


if __name__ == "__main__":
    main()
