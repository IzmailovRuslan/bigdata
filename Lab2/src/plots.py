import pandas as pd
import matplotlib.pyplot as plt

not_opt_1 = pd.read_csv("results/not_optimized_1_nodes.csv")
not_opt_3 = pd.read_csv("results/not_optimized_3_nodes.csv")
opt_1 = pd.read_csv("results/optimized_1_nodes.csv")
opt_3 = pd.read_csv("results/optimized_3_nodes.csv")

plt.figure(figsize=(14, 6))

plt.subplot(1, 2, 1)
plt.boxplot([not_opt_1["Time"], not_opt_3["Time"], opt_1["Time"], opt_3["Time"]])
plt.xticks([1,2,3,4], ["Not opt, 1 node", "Not opt, 3 nodes", "Opt, 1 node", "Opt, 3 nodes"])
plt.ylabel('Time (s)')
plt.title(f'Time distribution')
plt.grid(True)

plt.subplot(1, 2, 2)
plt.boxplot([not_opt_1["RAM"], not_opt_3["RAM"], opt_1["RAM"], opt_3["RAM"]])
plt.xticks([1,2,3,4], ["Not opt, 1 node", "Not opt, 3 nodes", "Opt, 1 node", "Opt, 3 nodes"])
plt.ylabel('RAM(MB)')
plt.title(f'RAM distribution')
plt.grid(True)

plt.savefig(f'results/result_distributions.png')