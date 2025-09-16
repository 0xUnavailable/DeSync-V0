#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load CSVs
u_worker = pd.read_csv("scaling_unconstrained_worker.csv")
u_intra  = pd.read_csv("scaling_unconstrained_intra.csv")
c_worker = pd.read_csv("scaling_constrained_worker.csv")
c_intra  = pd.read_csv("scaling_constrained_intra.csv")

# Ensure TPS is numeric
for df in [u_worker, u_intra, c_worker, c_intra]:
    df["TPS"] = pd.to_numeric(df["TPS"], errors="coerce")

# Group + aggregate (mean + std)
def agg(df, prefix):
    return df.groupby("totalTx")["TPS"].agg(["mean", "std"]).rename(
        columns={"mean":f"{prefix}_mean","std":f"{prefix}_std"}
    ).reset_index()

u_worker_agg = agg(u_worker, "uw")
u_intra_agg  = agg(u_intra, "ui")
c_worker_agg = agg(c_worker, "cw")
c_intra_agg  = agg(c_intra, "ci")

# Merge all into one DataFrame
df = u_worker_agg.merge(u_intra_agg, on="totalTx", how="outer") \
                 .merge(c_worker_agg, on="totalTx", how="outer") \
                 .merge(c_intra_agg, on="totalTx", how="outer") \
                 .fillna(0.0).sort_values("totalTx").reset_index(drop=True)

# Plotting
x = np.arange(len(df))  # positions
width = 0.2             # bar width

fig, ax = plt.subplots(figsize=(12,6))

# Bars
bars_uw = ax.bar(x - 1.5*width, df["uw_mean"], width, label="Unconstrained Worker", color="steelblue", zorder=2)
bars_ui = ax.bar(x - 0.5*width, df["ui_mean"], width, label="Unconstrained Intra", color="skyblue", zorder=2)
bars_cw = ax.bar(x + 0.5*width, df["cw_mean"], width, label="Constrained Worker", color="darkorange", zorder=2)
bars_ci = ax.bar(x + 1.5*width, df["ci_mean"], width, label="Constrained Intra", color="navajowhite", zorder=2)

# Error bars
ax.errorbar(x - 1.5*width, df["uw_mean"], yerr=df["uw_std"], fmt='none',
            ecolor='gray', elinewidth=1, alpha=0.5, capsize=3, zorder=1)
ax.errorbar(x - 0.5*width, df["ui_mean"], yerr=df["ui_std"], fmt='none',
            ecolor='gray', elinewidth=1, alpha=0.5, capsize=3, zorder=1)
ax.errorbar(x + 0.5*width, df["cw_mean"], yerr=df["cw_std"], fmt='none',
            ecolor='gray', elinewidth=1, alpha=0.5, capsize=3, zorder=1)
ax.errorbar(x + 1.5*width, df["ci_mean"], yerr=df["ci_std"], fmt='none',
            ecolor='gray', elinewidth=1, alpha=0.5, capsize=3, zorder=1)

# Labels
ax.set_xticks(x)
ax.set_xticklabels([f"{int(v):,}" for v in df["totalTx"]])
ax.set_xlabel("Total Transactions per Block")
ax.set_ylabel("Throughput (TPS)")
ax.set_title("Scaling Benchmark: Worker vs Intra Execution (Constrained & Unconstrained)")
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

for bars in [bars_uw, bars_ui, bars_cw, bars_ci]:
    autolabel(bars)

plt.tight_layout()
plt.savefig("scaling_comparison_all.png", dpi=300)
plt.show()

# Print aggregated table
print("\nAggregated results:")
print(df[["totalTx","uw_mean","uw_std","ui_mean","ui_std","cw_mean","cw_std","ci_mean","ci_std"]])
