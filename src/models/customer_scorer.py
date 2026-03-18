"""
NWDAF Customer Scorer — TelcoMax Demo
Trains a GradientBoostingClassifier to predict premium plan upgrade conversion.
Logs to MLflow and registers model in Unity Catalog.

Model: cmegdemos_catalog.dynamic_slicing_live_event.telcomax_customer_scorer
"""

import os
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score,
    roc_auc_score, classification_report,
)
from sklearn.model_selection import train_test_split

CATALOG = "cmegdemos_catalog"
SCHEMA = "dynamic_slicing_live_event"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.telcomax_customer_scorer"

FEATURE_COLS = [
    "monthly_revenue",
    "social_influence_score",
    "data_usage_gb_monthly",
    "churn_risk_score",
    "support_tickets_30d",
    "upgrade_propensity_score",
    "age",
    "avg_congestion_score",
    "congestion_predicted_15min",
    "near_event_flag",
]
LABEL_COL = "will_convert"


def get_spark():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.serverless(True).getOrCreate()


def load_training_data(spark) -> pd.DataFrame:
    """Join conversion opportunities with customer data to build training set."""
    print("Loading gold_conversion_opportunities for training data...")
    df = spark.table(f"{CATALOG}.{SCHEMA}.gold_conversion_opportunities").toPandas()
    print(f"  Loaded {len(df):,} opportunity rows")

    if len(df) == 0:
        raise ValueError("No conversion opportunities found. Run the pipeline first.")

    # Simulate ground-truth labels: customers with high conversion_score convert at 47%
    rng = np.random.default_rng(42)
    df[LABEL_COL] = (
        (df["conversion_score"] > 0.7) & (rng.random(len(df)) < 0.47)
    ).astype(int)

    # Add noise for lower-score customers
    low_score_mask = df["conversion_score"] <= 0.7
    df.loc[low_score_mask, LABEL_COL] = (
        rng.random(low_score_mask.sum()) < 0.05
    ).astype(int)

    print(f"  Label distribution: {df[LABEL_COL].value_counts().to_dict()}")
    print(f"  Conversion rate: {df[LABEL_COL].mean():.2%}")
    return df


def prepare_features(df: pd.DataFrame):
    available = [c for c in FEATURE_COLS if c in df.columns]
    df = df.dropna(subset=available + [LABEL_COL])
    X = df[available].values.astype(float)
    y = df[LABEL_COL].values.astype(int)
    return X, y, available


def train_model(X_train, y_train) -> GradientBoostingClassifier:
    model = GradientBoostingClassifier(
        n_estimators=150,
        learning_rate=0.08,
        max_depth=5,
        min_samples_split=10,
        subsample=0.8,
        random_state=42,
    )
    model.fit(X_train, y_train)
    return model


def main():
    spark = get_spark()
    print(f"Spark version: {spark.version}")

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment("/Shared/telcomax_nwdaf_experiments")

    df = load_training_data(spark)
    X, y, features_used = prepare_features(df)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    print(f"  Train: {len(X_train):,} | Test: {len(X_test):,}")

    with mlflow.start_run(run_name="telcomax_customer_scorer_gbm"):
        mlflow.log_params({
            "model_type": "GradientBoostingClassifier",
            "n_estimators": 150,
            "learning_rate": 0.08,
            "max_depth": 5,
            "features": ",".join(features_used),
            "training_rows": len(X_train),
        })

        print("Training GradientBoosting customer conversion scorer...")
        model = train_model(X_train, y_train)

        y_pred  = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]

        accuracy  = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, zero_division=0)
        recall    = recall_score(y_test, y_pred, zero_division=0)
        auc_roc   = roc_auc_score(y_test, y_proba) if len(np.unique(y_test)) > 1 else 0.0

        print(f"\n=== Customer Scorer Metrics ===")
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

        importances = pd.Series(model.feature_importances_, index=features_used)
        print("\nFeature importances:")
        print(importances.sort_values(ascending=False).to_string())
        mlflow.log_dict(importances.to_dict(), "feature_importances.json")

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
