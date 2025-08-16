import os
import logging
import numpy as np
import joblib
from tensorflow import keras

MODEL_PATH = "/app/saved_models/lstm_model.keras"
MODEL_WEIGHTS = "/app/saved_models/lstm_model.weights.h5"  # optional
SCALER_PATH = "/app/saved_models/feature_scaler.joblib"
FEATURES_PATH = "/app/saved_models/feature_columns.txt"

_lstm_model = None
_scaler = None
_feature_order = None


def _load_feature_order(path=FEATURES_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Feature list not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        cols = [line.strip() for line in f if line.strip()]
    if not cols:
        raise ValueError("Feature list file is empty.")
    return cols


def _load_scaler(path=SCALER_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Scaler not found: {path}")
    return joblib.load(path)


def _load_model():
    model = keras.models.load_model(MODEL_PATH)
    # weights are optional; if present and newer, they’ll refine the saved model
    try:
        if os.path.exists(MODEL_WEIGHTS):
            model.load_weights(MODEL_WEIGHTS)
    except Exception as e:
        logging.warning(f"Could not load extra weights: {e}")
    return model


def init_runtime():
    """
    Lazily initialize model, scaler, and feature order.
    Safe to call multiple times; caches globals.
    """
    global _lstm_model, _scaler, _feature_order
    if _lstm_model is None:
        _lstm_model = _load_model()
        logging.info("✅ LSTM model loaded for inference.")
    if _scaler is None:
        _scaler = _load_scaler()
        logging.info("✅ Scaler loaded for inference.")
    if _feature_order is None:
        _feature_order = _load_feature_order()
        logging.info(f"✅ Feature order loaded ({len(_feature_order)} features).")


def get_runtime():
    """
    Returns (model, scaler, feature_order). Calls init_runtime() if needed.
    """
    if _lstm_model is None or _scaler is None or _feature_order is None:
        init_runtime()
    return _lstm_model, _scaler, _feature_order


def make_sequence(feature_rows, timesteps):
    """
    Build a (1, timesteps, F) array from a list of feature dicts in temporal order.

    feature_rows: iterable of dicts, each mapping feature_name -> value (float‑castable)
    timesteps: required sequence length (e.g., 60; must match training)

    If fewer than timesteps rows are provided, we left‑pad with the first row.
    If more, we take the most recent 'timesteps' rows.
    """
    if not feature_rows:
        raise ValueError("No feature rows provided.")
    # ensure stable order
    rows = list(feature_rows)

    # pad/truncate
    if len(rows) < timesteps:
        pad = [rows[0]] * (timesteps - len(rows))
        rows = pad + rows
    elif len(rows) > timesteps:
        rows = rows[-timesteps:]

    model, scaler, feature_order = get_runtime()

    # assemble matrix (timesteps, F) in saved feature order
    mat = np.zeros((timesteps, len(feature_order)), dtype=np.float32)
    for i, r in enumerate(rows):
        for j, col in enumerate(feature_order):
            try:
                mat[i, j] = float(r.get(col, 0.0))
            except Exception:
                mat[i, j] = 0.0

    # scale with the training scaler
    # StandardScaler expects 2D -> reshape, transform, back
    flat = mat.reshape(-1, len(feature_order))
    flat_scaled = scaler.transform(flat)
    mat_scaled = flat_scaled.reshape(timesteps, len(feature_order))

    # add batch dimension
    return np.expand_dims(mat_scaled, axis=0)  # (1, T, F)


def predict_sequence(feature_rows, timesteps=60):
    """
    Convenience wrapper: returns raw model prediction given temporal feature rows.
    For your current architecture this returns a 2‑vector [lat, lon] in *scaled space*.
    If you later train to predict something else, this still generalizes.

    NOTE: Because your model was trained with scaled targets (lat/lon at the end of the feature vector),
    this returns the *scaled* lat/lon. If you want to inverse‑transform them to real degrees,
    use `invert_latlon()` below.
    """
    model, _, _ = get_runtime()
    seq = make_sequence(feature_rows, timesteps)
    preds = model.predict(seq, verbose=0)  # shape (1, 2) for your current model
    return preds[0]


def invert_latlon(scaled_lat, scaled_lon):
    """
    Invert the scaling of the lat/lon outputs back to degrees.

    This works by constructing a dummy 1xF vector filled with zeros and placing
    the scaled lat/lon in the last two positions (because training put labels
    at the end of the feature list), then applying scaler.inverse_transform
    and reading back those last two values.

    Returns (lat_deg, lon_deg)
    """
    _, scaler, feature_order = get_runtime()
    F = len(feature_order)
    dummy = np.zeros((1, F), dtype=np.float32)
    dummy[0, F - 2] = scaled_lat
    dummy[0, F - 1] = scaled_lon
    inv = scaler.inverse_transform(dummy)
    return float(inv[0, F - 2]), float(inv[0, F - 1])
