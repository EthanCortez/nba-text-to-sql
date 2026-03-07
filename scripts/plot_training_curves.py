import json, os
import pandas as pd
import matplotlib.pyplot as plt

RESULTS_DIR = "results"  # <-- change to desired output_dir
STATE_PATH = os.path.join(RESULTS_DIR, "trainer_state.json")

if not os.path.exists(STATE_PATH):
    raise FileNotFoundError(f"Missing {STATE_PATH}. Check your output_dir or enable logging in TrainingArguments.")

with open(STATE_PATH, "r") as f:
    state = json.load(f)

df = pd.DataFrame(state.get("log_history", []))

train_df = df[df.get("loss").notna()] if "loss" in df.columns else pd.DataFrame()
eval_df  = df[df.get("eval_loss").notna()] if "eval_loss" in df.columns else pd.DataFrame()

# Train loss curve
if not train_df.empty:
    plt.figure(figsize=(6,4))
    plt.plot(train_df["step"], train_df["loss"])
    plt.title("Baseline Training Loss")
    plt.xlabel("Step")
    plt.ylabel("Loss")
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, "baseline_train_loss.png"))
    plt.show()
else:
    print("No train loss found in log_history.")

# Eval loss curve
if not eval_df.empty:
    plt.figure(figsize=(6,4))
    plt.plot(eval_df["step"], eval_df["eval_loss"])
    plt.title("Baseline Validation Loss")
    plt.xlabel("Step")
    plt.ylabel("Eval Loss")
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, "baseline_eval_loss.png"))
    plt.show()
else:
    print("No eval_loss found. Make sure eval_strategy is enabled and eval_dataset is provided.")

# Train vs eval on same plot
if not train_df.empty and not eval_df.empty:
    plt.figure(figsize=(6,4))
    plt.plot(train_df["step"], train_df["loss"], label="train loss")
    plt.plot(eval_df["step"], eval_df["eval_loss"], label="val loss")
    plt.title("Baseline Train vs Validation Loss")
    plt.xlabel("Step")
    plt.ylabel("Loss")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, "baseline_train_vs_eval_loss.png"))
    plt.show()
    