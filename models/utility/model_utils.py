import os
import logging
import tensorflow as tf

MODEL_PATH = "/app/saved_models/lstm_model.keras"


def load_model(filepath=MODEL_PATH):
    if not os.path.exists(filepath):
        logging.error(f"❌ Model file not found at {filepath}")
        return None, None

    try:
        logging.info(f"📥 Loading model from {filepath}")
        model = tf.keras.models.load_model(filepath)
        input_shape = model.input_shape[1:]
        logging.info(f"✅ Model loaded. Input shape: {input_shape}")
        return model, input_shape
    except Exception as e:
        logging.error(f"❌ Failed to load model: {e}")
        return None, None
