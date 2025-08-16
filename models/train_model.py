import os
import logging
import numpy as np
import joblib
import pandas as pd
from sklearn.preprocessing import StandardScaler
from tensorflow.keras import Sequential
from tensorflow.keras.layers import Input, LSTM, Dense
from tensorflow.keras.callbacks import ModelCheckpoint
from db.db_connection import load_training_vectors

logging.basicConfig(level=logging.INFO)

# ✅ Paths for saving models + scaler metadata
MODEL_PATH = "/app/saved_models/lstm_model.keras"
MODEL_WEIGHTS_PATH = "/app/saved_models/lstm_model.weights.h5"
SCALER_PATH = "/app/saved_models/feature_scaler.joblib"
FEATURES_PATH = "/app/saved_models/feature_columns.txt"

# ✅ Model Hyperparameters
EPOCHS = 20
BATCH_SIZE = 32
TIME_STEPS = 60


def prepare_lstm_data(df, time_steps=TIME_STEPS):
    if df.empty:
        logging.error("❌ Received empty DataFrame for LSTM preparation.")
        return None, None

    required_cols = {"lat", "lon"}
    if not required_cols.issubset(df.columns):
        logging.error(f"❌ Required columns missing: {required_cols - set(df.columns)}")
        return None, None

    # Move labels to the end
    feature_columns = [c for c in df.columns if c not in ["lat", "lon"]] + ["lat", "lon"]
    df = df[feature_columns]

    X, y = [], []
    for i in range(time_steps, len(df)):
        X.append(df.iloc[i - time_steps:i].values)
        y.append(df.iloc[i][["lat", "lon"]].values)

    X, y = np.array(X), np.array(y)

    if X.shape[0] == 0:
        logging.error("❌ Not enough samples after slicing. Try lowering TIME_STEPS.")
        return None, None

    logging.info(f"✅ Prepared LSTM dataset: {X.shape[0]} samples | shape={X.shape[1:]}")
    return X, y


def build_lstm_model(input_shape):
    logging.info(f"🧠 Building LSTM model with input shape {input_shape}...")

    model = Sequential([
        Input(shape=input_shape),
        LSTM(64, activation='relu', return_sequences=True),
        LSTM(50, activation='relu'),
        Dense(2)  # Output: [lat, lon]
    ])
    model.compile(optimizer='adam', loss='mse', metrics=['mse'])

    logging.info("✅ LSTM model compiled successfully.")
    return model


def train_unified_model():
    logging.info("🚀 Starting unified LSTM training pipeline...")

    df = load_training_vectors()
    if df.empty:
        logging.error("❌ No training data available. Aborting.")
        return None

    # ✅ Ensure strict temporal order for sequences (already sorted in loader, but double‑safe here)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
        # Drop timestamp from model features
        df = df.drop(columns=["timestamp"])

    # Normalize features (including lat/lon since model predicts in scaled space)
    feature_cols = df.columns.tolist()
    scaler = StandardScaler()
    df[feature_cols] = scaler.fit_transform(df[feature_cols])

    # Persist scaler + feature order for inference
    try:
        joblib.dump(scaler, SCALER_PATH)
        with open(FEATURES_PATH, "w", encoding="utf-8") as f:
            for c in feature_cols:
                f.write(c + "\n")
        logging.info(f"💾 Saved scaler → {SCALER_PATH} and feature list → {FEATURES_PATH}")
    except Exception as e:
        logging.warning(f"⚠️ Could not save scaler/feature columns: {e}")

    X, y = prepare_lstm_data(df, TIME_STEPS)
    if X is None:
        return None

    model = build_lstm_model((TIME_STEPS, X.shape[2]))

    checkpoint_callback = ModelCheckpoint(
        filepath=MODEL_WEIGHTS_PATH,
        save_weights_only=True,
        save_best_only=True,
        verbose=1
    )

    model.fit(X, y, epochs=EPOCHS, batch_size=BATCH_SIZE, callbacks=[checkpoint_callback])
    model.save(MODEL_PATH)

    logging.info(f"✅ Model trained and saved to: {MODEL_PATH}")
    return model


if __name__ == "__main__":
    # Optional: allow `python train_model.py` direct run inside the container
    train_unified_model()
