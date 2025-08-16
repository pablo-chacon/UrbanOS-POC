import logging
import os
import time
from train_model import train_unified_model

logging.basicConfig(level=logging.INFO)

# ✅ Constants
MODEL_PATH = "/app/saved_models/lstm_model.keras"
MODEL_MAX_AGE_HOURS = 26  # retrain threshold in hours


def should_retrain_model():
    """
    Checks if the model file exists and is fresh enough.
    Returns True if retraining is needed.
    """
    if not os.path.exists(MODEL_PATH):
        logging.warning("⚠ No model found on disk. Training required.")
        return True

    model_age_hours = (time.time() - os.path.getmtime(MODEL_PATH)) / 3600
    if model_age_hours > MODEL_MAX_AGE_HOURS:
        logging.info(f"🕒 Model age: {model_age_hours:.1f}h. Retraining triggered.")
        return True

    return False


def main():
    logging.info("🧠 LSTM model trainer module started.")

    if should_retrain_model():
        train_unified_model()
    else:
        logging.info("✅ Model is fresh. No retraining needed.")

    logging.info("✅ Trainer module done. Exiting.")


if __name__ == "__main__":
    main()
