"""
NWDAF Congestion Predictor — TelcoMax Demo
Trains a RandomForestClassifier to predict tower congestion 15 minutes ahead.
Logs to MLflow and registers model in Unity Catalog.

Model: cmegdemos_catalog.dynamic_slicing_live_event.telcomax_congestion_predictor
"""

import os
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score,
    roc_auc_score, classification_report,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

CATALOG = "cmegdemos_catalog"
SCHEMA = "dynamic_slicing_live_event"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.telcomax_congestion_predictor"

FEATURE_COLS = [
    "avg_bandwidth_util",
    "peak_bandwidth_util",
    "avg_connections",
    "avg_congestion_score",
    "avg_latency_ms",
    "avg_packet_loss_pct",
    "bandwidth_trend_15min",
    "connection_trend_15min",
    "near_event_flag",
    "hour_of_day",
    "day_of_week",
]
LABEL_COL = "congestion_predicted_15min"


def get_spark():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.serverless(True).getOrCreate()


def load_training_data(spark) -> pd.DataFrame:
    print("Loading gold_congestion_features training data...")
    df = spark.table(f"{CATALOG}.{SCHEMA}.gold_congestion_features").toPandas()
    print(f"  Loaded {len(df):,} feature rows")
    return df


def prepare_features(df: pd.DataFrame):
    df = df.dropna(subset=FEATURE_COLS + [LABEL_COL])
    X = df[FEATURE_COLS].values
    y = df[LABEL_COL].values.astype(int)

    # Oversample positive class for better recall (congestion events are rare)
    pos_idx = np.where(y == 1)[0]
    neg_idx = np.where(y == 0)[0]
    if len(pos_idx) > 0 and len(neg_idx) > 0:
        n_oversample = min(len(neg_idx), len(pos_idx) * 5)
        pos_oversample = np.random.choice(pos_idx, size=n_oversample, replace=True)
        all_idx = np.concatenate([neg_idx, pos_oversample])
        np.random.shuffle(all_idx)
        X, y = X[all_idx], y[all_idx]

    return X, y


def train_model(X_train, y_train) -> RandomForestClassifier:
    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=12,
        min_samples_split=5,
        min_samples_leaf=2,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    return model


def main():
    spark = get_spark()
    print(f"Spark version: {spark.version}")

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment("/Shared/telcomax_nwdaf_experiments")

    df = load_training_data(spark)
    X, y = prepare_features(df)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    print(f"  Train: {len(X_train):,} | Test: {len(X_test):,} | Congestion rate: {y.mean():.2%}")

    with mlflow.start_run(run_name="telcomax_congestion_predictor_rf"):
        mlflow.log_params({
            "model_type": "RandomForestClassifier",
            "n_estimators": 200,
            "max_depth": 12,
            "class_weight": "balanced",
            "features": ",".join(FEATURE_COLS),
            "training_rows": len(X_train),
        })

        print("Training RandomForest congestion predictor...")
        model = train_model(X_train, y_train)

        y_pred = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]

        accuracy  = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, zero_division=0)
        recall    = recall_score(y_test, y_pred, zero_division=0)
        auc_roc   = roc_auc_score(y_test, y_proba)

        print(f"\n=== Congestion Predictor Metrics ===")
        print(f"  Accuracy:  {accuracy:.4f}")
        print(f"  Precision: {precision:.4f}")
        print(f"  Recall:    {recall:.4f}")
        print(f"  AUC-ROC:   {auc_roc:.4f}")
        print(f"\n{classification_report(y_test, y_pred)}")

        mlflow.log_metrics({
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "auc_roc": auc_roc,
        })

        # Feature importance
        importances = pd.Series(model.feature_importances_, index=FEATURE_COLS)
        print("\nFeature importances:")
        print(importances.sort_values(ascending=False).to_string())
        mlflow.log_dict(importances.to_dict(), "feature_importances.json")

        # Log and register model
        signature = mlflow.models.infer_signature(X_train, y_pred)
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            signature=signature,
            registered_model_name=MODEL_NAME,
            input_example=X_test[:5],
        )

        print(f"\nModel registered: {MODEL_NAME}")
        print(f"MLflow run: {mlflow.active_run().info.run_id}")


if __name__ == "__main__":
    main()
