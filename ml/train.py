"""
Trains beacon-direction w an XGBoost classifier on mart_model_features, labelled by
mart_model_labels, validated with walk_forward against two baselines, then registered and gated for promotion

Runs:
- One parent run that compares the child runs
- Three child runs:
    Price 
    Price + Structure
    Price + Structure + Sentiment

Models:
  always_up - every row gets the training up-rate as its probability and is
              predicted up. Its accuracy is the test fold's base rate

  logreg    - logistic regression on the same features

  xgb       - XGBoost trained on the training window minus its last year with no calibration

  xgb_cal   - XGBoost trained on the window minus its last year, with sigmoid
              calibration fitted on that year
    

Fixed hyperparameters to prevent leaking 
Walk forward.csv holds the full table

Registry registers one model as a "champion". The first run registers the baseline as "champion". Each run picks one candidatae,
registers it, then promote it only if it beats the baseline and current champion on walk-forward log loss. Decisions and reasons
are logged to the run.
"""

import argparse
import json
import os

import duckdb
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from sklearn.calibration import CalibratedClassifierCV
from sklearn.dummy import DummyClassifier
from sklearn.frozen import FrozenEstimator
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

from ml.promotion import decide, pick_candidate
from ml.tracking import require_tracking_server
from ml.walk_forward import (CALIBRATION_SESSIONS, MIN_TRAIN_YEARS, PURGE_SESSIONS, calibration_split,
                             split, walk_forward_folds)

GOLD_DIR = os.environ.get("BEACON_GOLD_DIR", "/opt/airflow/data/gold")
EXPERIMENT = "beacon-direction"
MODEL_NAME = "beacon-direction"
CHAMPION = "champion"
SEED = 42
METRICS = ["accuracy", "roc_auc", "log_loss", "brier"]
RELIABILITY_BINS = 10
CALIBRATION_METHOD = "sigmoid"  # Sigmoid > isotonic, more robust to small calibration sets and less likely to overfit.

FEATURE_GROUPS = {
    "price": ["ret_1d", "ret_5d", "ret_20d", "close_vs_sma20", "close_vs_sma50", "rsi_14",
              "macd_hist_norm", "bb_pct_b", "realized_vol_20d", "volume_z_20d", "spy_ret_5d"],
    "structure": ["dist_prev_high", "dist_prev_low", "dist_20d_high", "dist_20d_low",
                  "higher_highs_10d", "higher_lows_10d", "gap_pct"],
    "sentiment": ["mention_count", "bullish_ratio", "velocity_vs_prior_7"],  # null where no coverage
}
NOT_NULL = FEATURE_GROUPS["price"] + FEATURE_GROUPS["structure"]

# Each variant adds one group to the one before it, so each comparison isolates one group
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
    # Every feature as float64: a nullable count (mention_count) arrives as float in a
    # batch with nulls and int in one without, and the model schema needs one dtype
    features = [c for g in FEATURE_GROUPS.values() for c in g]
    df[features] = df[features].astype("float64")
    return df


def xgb():
    return XGBClassifier(**XGB_PARAMS, random_state=SEED, n_jobs=-1)


def logreg():
    return make_pipeline(SimpleImputer(strategy="median", add_indicator=True), StandardScaler(),
                         LogisticRegression(max_iter=1000))


def calibrated_xgb(train: pd.DataFrame, features: list[str]):
    """XGBoost on the training window minus its last year; calibration fitted on that year."""
    classifier_rows, cal = calibration_split(train)
    base = xgb().fit(classifier_rows[features], classifier_rows.label)
    calibrated = CalibratedClassifierCV(FrozenEstimator(base), method=CALIBRATION_METHOD)
    return base, calibrated.fit(cal[features], cal.label)


def log_sklearn_model(model, example: pd.DataFrame):
    # cloudpickle not MLflow's skops default because we don't load any untrusted files
    return mlflow.sklearn.log_model(model, name="model", input_example=example.head(5),
                                    serialization_format=mlflow.sklearn.SERIALIZATION_FORMAT_CLOUDPICKLE)


def score(y, p, predicted_up) -> dict:
    return {
        "accuracy": accuracy_score(y, predicted_up),
        "roc_auc": roc_auc_score(y, p),
        "log_loss": log_loss(y, p, labels=[0, 1]),
        "brier": brier_score_loss(y, p),
    }


def walk_forward(df: pd.DataFrame, features: list[str]):
    """Per-fold metrics, plus every out-of-sample probability for the reliability curve."""
    rows, preds = [], []
    for fold in walk_forward_folds(df.trade_date):
        train, test = split(df, fold)
        X_tr, y_tr, X_te, y_te = train[features], train.label, test[features], test.label
        base = {"test_year": fold.test_year, "n_train": len(train), "n_test": len(test),
                "base_rate": y_te.mean()}

        p = {
            "always_up": np.full(len(test), y_tr.mean()),
            "logreg": logreg().fit(X_tr, y_tr).predict_proba(X_te)[:, 1],
            "xgb": xgb().fit(X_tr, y_tr).predict_proba(X_te)[:, 1],
            "xgb_cal": calibrated_xgb(train, features)[1].predict_proba(X_te)[:, 1],
        }
        for name, prob in p.items():
            predicted_up = np.ones(len(test)) if name == "always_up" else prob >= 0.5
            rows.append({**base, "model": name, **score(y_te, prob, predicted_up)})
        preds.append(pd.DataFrame({"test_year": fold.test_year, "label": y_te.to_numpy(), **p}))
    return pd.DataFrame(rows), pd.concat(preds, ignore_index=True)


def reliability(preds: pd.DataFrame, models=("logreg", "xgb", "xgb_cal")) -> pd.DataFrame:
    """Predicted probability vs observed up-rate, in equal-count buckets of each model's output."""
    curves = []
    for m in models:
        bucket = pd.qcut(preds[m], RELIABILITY_BINS, labels=False, duplicates="drop")
        curve = preds.groupby(bucket).agg(predicted=(m, "mean"), observed=("label", "mean"), n=("label", "size"))
        curves.append(curve.reset_index(names="bucket").assign(model=m))
    return pd.concat(curves, ignore_index=True)[["model", "bucket", "predicted", "observed", "n"]]


def calibration_error(curve: pd.DataFrame) -> float:
    """Expected calibration error: count-weighted mean |predicted - observed| over buckets."""
    return float((curve.n * (curve.predicted - curve.observed).abs()).sum() / curve.n.sum())


def print_folds(folds: pd.DataFrame):
    """Every fold, every model - shown in the Airflow task log"""
    table = folds.pivot(index="test_year", columns="model", values=["accuracy", "roc_auc", "log_loss"])
    table.columns = [f"{model}_{metric}" for metric, model in table.columns]
    table.insert(0, "base_rate", folds.groupby("test_year").base_rate.first())
    print(table.round(4).to_string())


def train_variant(df: pd.DataFrame, variant: str, features: list[str], groups: list[str]) -> dict:
    with mlflow.start_run(run_name=variant, nested=True):
        mlflow.set_tag("variant", variant)
        mlflow.log_params({"variant": variant, "feature_groups": ",".join(groups),
                           "features": ",".join(features), "n_features": len(features)})

        folds, preds = walk_forward(df, features)
        print(f"\n== {variant}")
        print_folds(folds)
        for r in folds.itertuples():
            for m in METRICS:
                mlflow.log_metric(f"{r.model}_{m}", getattr(r, m), step=r.test_year)
            mlflow.log_metric("base_rate", r.base_rate, step=r.test_year)
        means = folds.groupby("model")[METRICS].mean()
        for (model, m), v in means.stack().items():
            mlflow.log_metric(f"{model}_{m}_mean", v)
        mlflow.log_text(folds.to_csv(index=False), "walk_forward.csv")

        curve = reliability(preds)
        mlflow.log_text(curve.to_csv(index=False), "reliability.csv")
        ece = {m: calibration_error(c) for m, c in curve.groupby("model")}
        for m, v in ece.items():
            mlflow.log_metric(f"{m}_calibration_error", v)
        mlflow.log_metrics({"xgb_cal_min_prob": preds.xgb_cal.min(), "xgb_cal_max_prob": preds.xgb_cal.max()})
        print(curve[curve.model.isin(["xgb", "xgb_cal"])].round(4).to_string(index=False))

        base, final = calibrated_xgb(df, features)
        gain = base.get_booster().get_score(importance_type="gain")
        mlflow.log_dict(dict(sorted(gain.items(), key=lambda kv: -kv[1])), "feature_importance.json")
        model_uri = log_sklearn_model(final, df[features]).model_uri

    latest = folds[folds.test_year == folds.test_year.max()].set_index("model").log_loss
    return {
        "variant": variant,
        "n_features": len(features),
        # Share of rows where every feature is present
        "complete_rows_share": round(df[features].notna().all(axis=1).mean(), 4),
        "xgb_cal_log_loss_mean": means.loc["xgb_cal", "log_loss"],
        "always_up_log_loss_mean": means.loc["always_up", "log_loss"],
        "latest_test_year": int(folds.test_year.max()),
        "xgb_cal_log_loss_latest": latest["xgb_cal"],
        "always_up_log_loss_latest": latest["always_up"],
        "xgb_cal_calibration_error": ece["xgb_cal"],
        "xgb_calibration_error": ece["xgb"],
        "features": ",".join(features),
        "model_uri": model_uri,
    }


def champion_version(client: MlflowClient):
    """The champion ModelVersion, or None if the registered model or alias does not exist yet."""
    try:
        aliases = client.get_registered_model(MODEL_NAME).aliases
    except MlflowException as e:
        if e.error_code == "RESOURCE_DOES_NOT_EXIST":
            return None
        raise
    return client.get_model_version(MODEL_NAME, aliases[CHAMPION]) if CHAMPION in aliases else None


def register(client: MlflowClient, model_uri: str, tags: dict):
    version = mlflow.register_model(model_uri, MODEL_NAME)
    for k, v in tags.items():
        client.set_model_version_tag(MODEL_NAME, version.version, k, str(v))
    return version


def bootstrap_baseline(client: MlflowClient, df: pd.DataFrame):
    """First run only: the always-up baseline becomes version 1 and champion."""
    features = FEATURE_GROUPS["price"]  # unused by the prediction; gives the model a schema
    with mlflow.start_run(run_name="baseline", nested=True):
        model = DummyClassifier(strategy="prior").fit(df[features], df.label)
        uri = log_sklearn_model(model, df[features]).model_uri
    version = register(client, uri, {"kind": "baseline", "variant": "always_up", "features": ",".join(features),
                                     "decision": "bootstrap: first champion"})
    client.set_registered_model_alias(MODEL_NAME, CHAMPION, version.version)
    print(f"Registered the always-up baseline as version {version.version} and champion")
    return client.get_model_version(MODEL_NAME, version.version)


def gate(client: MlflowClient, variants: pd.DataFrame, champion, degraded: bool) -> dict:
    """Registers the candidate variant and promotes it only if it passes ml/promotion.decide."""
    mean_ll = dict(zip(variants.variant, variants.xgb_cal_log_loss_mean))
    candidate = variants.set_index("variant").loc[pick_candidate(mean_ll, list(VARIANTS))]
    always_up = float(candidate.always_up_log_loss_mean)
    # A baseline champion has no score of its own to beat
    champion_ll = None if champion.tags.get("kind") == "baseline" else float(champion.tags["gate_log_loss"])
    decision = decide(float(candidate.xgb_cal_log_loss_mean), always_up, champion_ll)

    version = register(client, candidate.model_uri, {
        "kind": "xgb_cal", "variant": candidate.name, "features": candidate.features,
        "gate_log_loss": f"{candidate.xgb_cal_log_loss_mean:.6f}", "always_up_log_loss": f"{always_up:.6f}",
        "degraded": degraded, "decision": ("promoted: " if decision.promote else "not promoted: ") + decision.reason,
    })
    if decision.promote:
        client.set_registered_model_alias(MODEL_NAME, CHAMPION, version.version)
    return {
        "candidate_variant": candidate.name, "candidate_version": int(version.version),
        "promoted": decision.promote, "reason": decision.reason,
        "previous_champion_version": int(champion.version),
        "champion_version": int(version.version) if decision.promote else int(champion.version),
        "candidate_log_loss": float(candidate.xgb_cal_log_loss_mean), "always_up_log_loss": always_up,
    }


def main():
    require_tracking_server()  
    parser = argparse.ArgumentParser()
    parser.add_argument("--degrade", action="store_true", help="keep every other feature (gate verification)")
    degraded = parser.parse_args().degrade

    df = load_training_set()
    missing = df[NOT_NULL].columns[df[NOT_NULL].isna().any()].tolist()
    if missing:
        raise ValueError(f"Null values in model features {missing}; mart_model_features should filter these")
    groups = {g: cols[::2] if degraded else cols for g, cols in FEATURE_GROUPS.items()}

    client = MlflowClient()
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name="train-degraded" if degraded else "train"):
        mlflow.set_tag("degraded", degraded)
        mlflow.log_params({
            "n_rows": len(df), "n_tickers": df.ticker.nunique(),
            "first_date": str(df.trade_date.min().date()), "last_date": str(df.trade_date.max().date()),
            "purge_sessions": PURGE_SESSIONS, "min_train_years": MIN_TRAIN_YEARS,
            "calibration_sessions": CALIBRATION_SESSIONS, "seed": SEED,
            **{f"xgb_{k}": v for k, v in XGB_PARAMS.items()},
        })
        champion = champion_version(client) or bootstrap_baseline(client, df)

        variants = pd.DataFrame([
            train_variant(df, name, [c for g in gs for c in groups[g]], gs) for name, gs in VARIANTS.items()
        ])
        mlflow.log_text(variants.drop(columns="model_uri").to_csv(index=False), "variants.csv")
        print("\n== variants")
        print(variants.drop(columns=["features", "model_uri"]).round(4).to_string(index=False))

        result = gate(client, variants, champion, degraded)
        mlflow.log_dict(result, "promotion.json")
        mlflow.set_tags({"promoted": result["promoted"], "promotion_reason": result["reason"],
                         "champion_version": result["champion_version"]})
        # Positive = the candidate beats the base rate. The Pipeline Health eval trend reads this
        mlflow.log_metric("edge_vs_always_up", result["always_up_log_loss"] - result["candidate_log_loss"])
        print("\n== promotion")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

    