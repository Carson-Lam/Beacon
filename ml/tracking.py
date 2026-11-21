"""
MLflow tracking guard to call require_tracking_server() before any training or inference.
"""

import os


def require_tracking_server() -> str:
    uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not uri.startswith(("http://", "https://")):
        raise RuntimeError(
            f"MLFLOW_TRACKING_URI must point at the MLflow server (http://mlflow:5000), got {uri!r}. "
            "Refusing to log to a local store inside this container."
        )
    return uri