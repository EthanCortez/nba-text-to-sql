"""
visualization.py

Purpose:
    Generate visualization plots for the NBA Text-to-SQL project evaluation results.
    This script creates bar charts comparing the baseline and advanced models on:

        • Execution Accuracy
        • SQL Execution Failure Rate
        • Average Edit Distance

    The figures are saved as PNG files and displayed for inspection.

Authors:
    Sabrina Park

Notes:
    - This script was written by Sabrina Park for the project report visualizations.
    - Uses matplotlib for plotting.
    - The values are taken from the evaluation pipeline output (execution accuracy,
      SQL failure rate, and edit distance metrics).
"""


import matplotlib.pyplot as plt
# ----------------------
# Data
# ----------------------
models = ["Baseline", "Advanced"]
execution_accuracy = [0.4386, 0.0512]
failure_rate = [0.2982, 0.9084]
edit_distance = [58.4737, 19.7547]

# ----------------------
# Global styling
# ----------------------
plt.style.use("seaborn-v0_8-whitegrid")  # built-in matplotlib style
plt.rcParams.update({
    "figure.dpi": 150,
    "savefig.dpi": 300,
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.labelsize": 11,
})

COLORS = ["#6B7280", "#2563EB"]  # Baseline gray, Advanced blue

def barplot(title, ylabel, values, filename, ylim=None):
    fig, ax = plt.subplots(figsize=(6, 4))

    bars = ax.bar(models, values, color=COLORS, width=0.55)

    ax.set_title(title, pad=10)
    ax.set_ylabel(ylabel)

    # Subtle grid only on y
    ax.grid(axis="y", linestyle="-", linewidth=0.8, alpha=0.35)
    ax.grid(axis="x", visible=False)

    # Remove extra borders
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Y limits (either provided or auto with headroom)
    if ylim is not None:
        ax.set_ylim(*ylim)
    else:
        ymax = max(values)
        ax.set_ylim(0, ymax * 1.18)

    # Value labels with a little padding
    for b in bars:
        h = b.get_height()
        ax.annotate(
            f"{h:.2f}",
            (b.get_x() + b.get_width() / 2, h),
            xytext=(0, 5),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=10
        )

    fig.tight_layout()
    fig.savefig(filename, bbox_inches="tight")
    plt.show()

# 1) Execution Accuracy (tighter y-axis so bars look less tiny)
barplot(
    "Execution Accuracy",
    "Accuracy",
    execution_accuracy,
    "execution_accuracy.png",
    ylim=(0, 0.5)
)

# 2) SQL Failure Rate (keep full 0–1 since one value is ~0.91)
barplot(
    "SQL Execution Failure Rate",
    "Failure Rate",
    failure_rate,
    "failure_rate.png",
    ylim=(0, 1.0)
)

# 3) Average Edit Distance (auto ylim with headroom)
barplot(
    "Average Edit Distance",
    "Edit Distance",
    edit_distance,
    "edit_distance.png"
)
