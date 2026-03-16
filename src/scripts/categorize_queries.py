"""
categorize_queries.py

Purpose:
    Add heuristic labels to the NBA Text-to-SQL dataset for analysis.
    This script reads a CSV of natural-language questions and gold SQL queries,
    then assigns:
        1. word_count   -> number of words in the question
        2. query_type   -> rough category of the question
        3. difficulty   -> heuristic difficulty label (easy, medium, hard)

    The output is a new CSV that can be used for evaluation breakdowns,
    such as accuracy by difficulty level or query type.

Authors:
    Sabrina Park

Source / modification notes:
    - This script was written by Sabrina Park for the NBA Text-to-SQL project.
    - Uses pandas for CSV processing.
    - Difficulty labels are heuristic-based, not human-annotated.
"""

import pandas as pd
import re

# load your CSV
df = pd.read_csv("./data/actual_data1.csv")

# -------------------------
# Heuristic keyword groups
# -------------------------
vague_words = {
    "best", "better", "greatest", "hardest", "impactful", "efficient",
    "goat", "ballin", "balled", "cooked", "eating", "wet", "sickest",
    "absolute", "elite", "deserved", "most valuable"
}

comparison_words = {
    "compare", "vs", "or", "between"
}

trend_words = {
    "over time", "changed", "improved", "increase", "decrease", "years"
}

count_words = {
    "how many", "count", "number of"
}

# -------------------------
# SQL feature helpers
# -------------------------
def sql_feature_count(sql: str) -> int:
    s = sql.lower()
    score = 0
    
    if " join " in s:
        score += s.count(" join ")
    if " group by " in s:
        score += 2
    if " having " in s:
        score += 2
    if " order by " in s:
        score += 1
    if " limit " in s:
        score += 1
    if " where " in s:
        score += 1
    if " in (" in s or "select max" in s or "select min" in s:
        score += 2
    if " avg(" in s or " sum(" in s or " count(" in s or " max(" in s or " min(" in s:
        score += 1
        
    return score

def count_conditions(sql: str) -> int:
    s = sql.lower()
    if " where " not in s:
        return 0
    where_part = s.split(" where ", 1)[1]
    where_part = where_part.split(" group by ")[0].split(" order by ")[0].split(" limit ")[0]
    return where_part.count(" and ") + where_part.count(" or ") + 1

# -------------------------
# Query type labeling
# -------------------------
def classify_query_type(question: str) -> str:
    q = question.lower().strip()

    if any(x in q for x in comparison_words):
        return "comparison"
    if any(x in q for x in trend_words):
        return "trend"
    if any(x in q for x in count_words):
        return "count"
    if q.startswith("top ") or "top " in q:
        return "ranking"
    if q.startswith("who ") or q.startswith("which player") or q.startswith("which team"):
        return "lookup"
    if "all players" in q or "list all" in q or "show me all" in q:
        return "list"
    return "other"

# -------------------------
# Difficulty scoring
# -------------------------
def categorize_difficulty(question: str, sql: str) -> str:
    q = question.lower().strip()
    word_count = len(q.split())
    sql_score = sql_feature_count(sql)
    condition_count = count_conditions(sql)

    difficulty_score = 0

    # vague / underspecified language
    if any(word in q for word in vague_words):
        difficulty_score += 2

    # comparisons are usually harder
    if any(word in q for word in comparison_words):
        difficulty_score += 2

    # trend / over time questions
    if any(word in q for word in trend_words):
        difficulty_score += 2

    # longer questions = harder
    if word_count >= 12:
        difficulty_score += 2
    elif word_count >= 8:
        difficulty_score += 1

    # SQL complexity
    difficulty_score += sql_score

    # multiple filters
    if condition_count >= 3:
        difficulty_score += 2
    elif condition_count == 2:
        difficulty_score += 1

    # final label
    if difficulty_score <= 3:
        return "easy"
    elif difficulty_score <= 7:
        return "medium"
    else:
        return "hard"

# -------------------------
# Apply to dataset
# -------------------------
df["word_count"] = df["input"].apply(lambda x: len(str(x).split()))
df["query_type"] = df["input"].apply(classify_query_type)
df["difficulty"] = df.apply(lambda row: categorize_difficulty(str(row["input"]), str(row["output"])), axis=1)

# save
df.to_csv("actual_data_with_difficulty.csv", index=False)

# quick summary
print(df["difficulty"].value_counts())
print(df["query_type"].value_counts())

# optional: inspect examples
print(df[["input", "query_type", "difficulty"]].head(20))
