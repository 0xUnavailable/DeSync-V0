#!/usr/bin/env python3
import pandas as pd
import numpy as np

# Parameters
TPS1 = 10000      # baseline TPS at 1 core (replace with your measurement)
p_values = [0.1 , 0.9, 0.3, 0.5, 0.99]  # parallelizable fractions to explore
cores = [1, 2, 4, 8, 16, 32, 64]

rows = []
for p in p_values:
    for n in cores:
        speedup = 1 / ((1 - p) + p / n)
        tps = TPS1 * speedup
        rows.append({
            "p": p,
            "cores": n,
            "speedup": speedup,
            "TPS": tps
        })

df = pd.DataFrame(rows)
df.to_csv("amdahl_projection.csv", index=False)
print("Saved amdahl_projection.csv")
print(df.head(len(rows)))
