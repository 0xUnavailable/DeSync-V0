#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load CSVs
u = pd.read_csv("scaling_unconstrained.csv")
c = pd.read_csv("scaling_constrained.csv")

# Ensure TPS is numeric
for df in [u, c]:
    df["TPS"] = pd.to_numeric(df["TPS"], errors="coerce")

# Group + aggregate (mean + std)
def agg(df, prefix):
    return df.groupby("totalTx")["TPS"].agg(["mean", "std"]).rename(
        columns={"mean": f"{prefix}_mean", "std": f"{prefix}_std"}
    ).reset_index()

u_agg = agg(u, "u")   # unconstrained
c_agg = agg(c, "c")   # constrained

# Merge both into one DataFrame
df = u_agg.merge(c_agg, on="totalTx", how="outer") \
          .fillna(0.0).sort_values("totalTx").reset_index(drop=True)

# Plotting
x = np.arange(len(df))  # positions
width = 0.35            # bar width

fig, ax = plt.subplots(figsize=(10, 6))

# Bars
bars_u = ax.bar(x - width/2, df["u_mean"], width, label="Unconstrained", color="steelblue", zorder=2)
bars_c = ax.bar(x + width/2, df["c_mean"], width, label="Constrained", color="darkorange", zorder=2)

# Error bars
ax.errorbar(x - width/2, df["u_mean"], yerr=df["u_std"], fmt='none',
            ecolor='gray', elinewidth=1, alpha=0.5, capsize=3, zorder=1)
ax.errorbar(x + width/2, df["c_mean"], yerr=df["c_std"], fmt='none',
            ecolor='gray', elinewidth=1, alpha=0.5, capsize=3, zorder=1)

# Labels
ax.set_xticks(x)
ax.set_xticklabels([f"{int(v):,}" for v in df["totalTx"]])
ax.set_xlabel("Total Transactions per Block")
ax.set_ylabel("Throughput (TPS)")
ax.set_title("Scaling Benchmark: Constrained vs Unconstrained")
ax.grid(axis="y", linestyle="--", linewidth=0.5, zorder=0)
ax.legend()

# Annotate bars
def autolabel(bars, fmt="{:.0f}"):
    for bar in bars:
        height = bar.get_height()
        ax.annotate(fmt.format(height),
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 5),
                    textcoords="offset points",
                    ha="center", va="bottom", fontsize=8)

autolabel(bars_u)
autolabel(bars_c)

plt.tight_layout()
plt.savefig("scaling_comparison_uc.png", dpi=300)
plt.show()

# Print aggregated table
print("\nAggregated results:")
print(df[["totalTx", "u_mean", "u_std", "c_mean", "c_std"]])
