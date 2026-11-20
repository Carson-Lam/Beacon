"""
Trains beacon-direction w an XGBoost classifier on mart_model_features, labelled by
mart_model_labels, validated with walk_forward against two baselines.

Runs:
- One parent run that compares the child runs
- Three child runs:
    Price 
    Price + Structure
    Price + Structure + Sentiment

Baselines:
  always_up - every row gets the training up-rate as its probability and is
              predicted up. Its accuracy is the test fold's base rate

  logreg    - logistic regression on the same features

Fixed hyperparameters to prevent leaking 
Walk forward.csv holds the full table
"""

import os

import duckdb
import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

from ml.tracking import require_tracking_server
from ml.walk_forward import MIN_TRAIN_YEARS, PURGE_SESSIONS, split, walk_forward_folds

GOLD_DIR = os.environ.get("BEACON_GOLD_DIR", "/opt/airflow/data/gold")
EXPERIMENT = "beacon-direction"
SEED = 42
METRICS = ["accuracy", "roc_auc", "log_loss", "brier"]

FEATURE_GROUPS = {
    "price": ["ret_1d", "ret_5d", "ret_20d", "close_vs_sma20", "close_vs_sma50", "rsi_14",
              "macd_hist_norm", "bb_pct_b", "realized_vol_20d", "volume_z_20d", "spy_ret_5d"],
    "structure": ["dist_prev_high", "dist_prev_low", "dist_20d_high", "dist_20d_low",
                  "higher_highs_10d", "higher_lows_10d", "gap_pct"],
    "sentiment": ["mention_count", "bullish_ratio", "velocity_vs_prior_7"],  
}
NOT_NULL = FEATURE_GROUPS["price"] + FEATURE_GROUPS["structure"]

# Each variant adds one group to the one before it so each comparison isolates one group
VARIANTS = {
    "price": ["price"],
    "price_structure": ["price", "structure"],
    "price_structure_sentiment": ["price", "structure", "sentiment"],
}

XGB_PARAMS = {
    "n_estimators": 300, "max_depth": 3, "learning_rate": 0.05, "subsample": 0.8,
    "colsample_bytree": 1.0, "min_child_weight": 50, "tree_method": "hist",
}


def load_training_set(gold_dir=GOLD_DIR) -> pd.DataFrame:
    """Features joined to labels; rows without a label (the last 6 sessions) are dropped."""
    df = duckdb.connect().execute(f"""
        select f.*, l.label
        from read_parquet('{gold_dir}/mart_model_features.parquet') f
        join read_parquet('{gold_dir}/mart_model_labels.parquet') l using (ticker, trade_date)
        where l.label is not null
        order by trade_date, ticker
    """).df()
    # Every feature as float64, a nullable count
    features = [c for g in FEATURE_GROUPS.values() for c in g]
    df[features] = df[features].astype("float64")
    return df


def xgb():
    return XGBClassifier(**XGB_PARAMS, random_state=SEED, n_jobs=-1)


def logreg():
    return make_pipeline(SimpleImputer(strategy="median", add_indicator=True), StandardScaler(),
                         LogisticRegression(max_iter=1000))


def score(y, p, predicted_up) -> dict:
    return {
        "accuracy": accuracy_score(y, predicted_up),
        "roc_auc": roc_auc_score(y, p),
        "log_loss": log_loss(y, p, labels=[0, 1]),
        "brier": brier_score_loss(y, p),
    }


def walk_forward(df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    rows = []
    for fold in walk_forward_folds(df.trade_date):
        train, test = split(df, fold)
        X_tr, y_tr, X_te, y_te = train[features], train.label, test[features], test.label
        base = {"test_year": fold.test_year, "n_train": len(train), "n_test": len(test),
                "base_rate": y_te.mean()}

        up_rate = np.full(len(test), y_tr.mean())
        rows.append({**base, "model": "always_up", **score(y_te, up_rate, np.ones(len(test)))})
        for name, model in (("logreg", logreg()), ("xgb", xgb())):
            p = model.fit(X_tr, y_tr).predict_proba(X_te)[:, 1]
            rows.append({**base, "model": name, **score(y_te, p, p >= 0.5)})
    return pd.DataFrame(rows)


def print_folds(folds: pd.DataFrame):
    """Every fold, every model - shown in the Airflow task log"""
    table = folds.pivot(index="test_year", columns="model", values=["accuracy", "roc_auc", "log_loss"])
    table.columns = [f"{model}_{metric}" for metric, model in table.columns]
    table.insert(0, "base_rate", folds.groupby("test_year").base_rate.first())
    print(table.round(4).to_string())


def train_variant(df: pd.DataFrame, variant: str, groups: list[str]) -> dict:
    features = [c for g in groups for c in FEATURE_GROUPS[g]]
    with mlflow.start_run(run_name=variant, nested=True):
        mlflow.set_tag("variant", variant)
        mlflow.log_params({"variant": variant, "feature_groups": ",".join(groups),
                           "features": ",".join(features), "n_features": len(features)})

        folds = walk_forward(df, features)
        print(f"\n== {variant}")
        print_folds(folds)
        for r in folds.itertuples():
            for m in METRICS:
                mlflow.log_metric(f"{r.model}_{m}", getattr(r, m), step=r.test_year)
            mlflow.log_metric("base_rate", r.base_rate, step=r.test_year)
        for (model, m), v in folds.groupby("model")[METRICS].mean().stack().items():
            mlflow.log_metric(f"{model}_{m}_mean", v)
        mlflow.log_text(folds.to_csv(index=False), "walk_forward.csv")

        final = xgb().fit(df[features], df.label)
        gain = final.get_booster().get_score(importance_type="gain")
        mlflow.log_dict(dict(sorted(gain.items(), key=lambda kv: -kv[1])), "feature_importance.json")
        mlflow.xgboost.log_model(final, name="model", input_example=df[features].head(5))

    latest = folds[folds.test_year == folds.test_year.max()].set_index("model").log_loss
    return {
        "variant": variant,
        "n_features": len(features),
        # Share of rows where every feature is present
        "complete_rows_share": round(df[features].notna().all(axis=1).mean(), 4),
        "xgb_log_loss_mean": folds[folds.model == "xgb"].log_loss.mean(),
        "latest_test_year": int(folds.test_year.max()),
        "xgb_log_loss_latest": latest["xgb"],
        "always_up_log_loss_latest": latest["always_up"],
    }


def main():
    require_tracking_server()  # before anything else

    df = load_training_set()
    missing = df[NOT_NULL].columns[df[NOT_NULL].isna().any()].tolist()
    if missing:
        raise ValueError(f"Null values in model features {missing}; mart_model_features should filter these")

    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name="train"):
        mlflow.log_params({
            "n_rows": len(df), "n_tickers": df.ticker.nunique(),
            "first_date": str(df.trade_date.min().date()), "last_date": str(df.trade_date.max().date()),
            "purge_sessions": PURGE_SESSIONS, "min_train_years": MIN_TRAIN_YEARS, "seed": SEED,
            **{f"xgb_{k}": v for k, v in XGB_PARAMS.items()},
        })
        variants = pd.DataFrame([train_variant(df, name, groups) for name, groups in VARIANTS.items()])
        mlflow.log_text(variants.to_csv(index=False), "variants.csv")
        print("\n== variants")
        print(variants.round(4).to_string(index=False))


if __name__ == "__main__":
    main()