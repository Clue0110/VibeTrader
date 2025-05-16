import numpy as np
from sklearn.preprocessing import MinMaxScaler
import tensorflow
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import time
import pickle

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import time
import random

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql.types import FloatType
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.model_selection import train_test_split
from pymongo import MongoClient
import io
from bson.binary import Binary

from dotenv import load_dotenv
import os
load_dotenv()

class StockPredictor:
    def __init__(self, company_name=None , sequence_length=60):
        self.sequence_length = sequence_length
        self.comp_name = company_name
        self.scaler = MinMaxScaler(feature_range=(0, 1))
        self.model = None
        self.client = None

    def connect_to_db(self , uri=os.environ.get("MDB_CONNECTION_STRING")):
        try:
          self.client = MongoClient(uri, server_api=ServerApi('1'))
        except Exception as e:
          print("Unable to connect to the server.")

    def close_connection(self):
        if self.client:
          self.client.close()
          self.client = None

    def preprocess(self, csv_path):
        df = pd.read_csv(csv_path, usecols=["Date", "Close"])
        stock_name = os.path.basename(csv_path).split('.')[0].upper()
        df = df.rename(columns={'Close': stock_name})
        self.stock_name = stock_name
        self.data = df[stock_name].values.reshape(-1, 1)

        self.dates = df['Date']
        self.data_scaled = self.scaler.fit_transform(self.data)
        self.train_len = int(np.ceil(len(self.data_scaled)))

    def preprocess_df(self, df_data):
        df = df_data["Date","Close"]
        self.data = df["Close"].values.reshape(-1, 1)
        self.data_scaled = self.scaler.fit_transform(self.data)
        self.train_len = int(np.ceil(len(self.data_scaled)))

    def create_sequences(self , csv_path=None, df_data=None):
        if csv_path is not None:
            self.preprocess(csv_path)
        elif df_data is not None:
            self.preprocess_df(df_data)
           
        x_train, y_train = [], []
        train_data = self.data_scaled[:self.train_len]
        for i in range(self.sequence_length, len(train_data)):
            x_train.append(train_data[i - self.sequence_length:i, 0])
            y_train.append(train_data[i, 0])
        x_train, y_train = np.array(x_train), np.array(y_train)
        return x_train.reshape((x_train.shape[0], x_train.shape[1], 1)), y_train

    def build_model(self):
        model = Sequential()
        model.add(LSTM(128, return_sequences=True, input_shape=(self.sequence_length, 1)))
        model.add(LSTM(64, return_sequences=False))
        model.add(Dense(25))
        model.add(Dense(1))
        model.compile(optimizer='adam', loss='mean_squared_error')
        self.model = model

    def train(self, x_train, y_train, epochs=8, batch_size=1):
        self.model.fit(x_train, y_train, batch_size=batch_size, epochs=epochs)
        self.save_model(self.comp_name)

    def predict_next(self):
        last_60_days = self.data_scaled[-self.sequence_length:]
        input_data = last_60_days.reshape(1, self.sequence_length, 1)
        predicted_scaled = self.model.predict(input_data)
        return self.scaler.inverse_transform(predicted_scaled)[0][0]

    def user_predict(self , ip , pre_loaded=True):
        if not pre_loaded:
          self.load_model(self.comp_name)

        ip_array = np.array(ip).reshape(-1, 1)
        ip_scaled = self.scaler.fit_transform(ip_array)
        input_data = ip_scaled.reshape(1, self.sequence_length, 1)
        predicted_scaled = self.model.predict(input_data)
        predicted_price = self.scaler.inverse_transform(predicted_scaled)
        return float(predicted_price[0][0])

    def save_model(self , model_name):
        pickled_model = pickle.dumps(self.model)

        self.connect_to_db()
        db = self.client['model']
        collection = db['company']

        info = collection.insert_one({
            'model_name': model_name,
            'model_data': pickled_model,
            'time_stamp': time.time()
            })

        print(f"Model saved to DB with id: {info.inserted_id}")
        self.close_connection()
        return {'inserted_id': info.inserted_id,'model_name': model_name , 'time_stamp': time.time()}

    def load_model(self , model_name):
        print("Here")
        if self.client is None:
          self.connect_to_db()
        db = self.client['model']
        collection = db['company']
        doc = collection.find_one({'model_name': model_name},sort=[('time_stamp', -1)])
        if doc is None:
          raise ValueError(f"Model '{model_name}' not found in the database.")
        pickled_model = doc['model_data']
        self.model = pickle.loads(pickled_model)
        self.close_connection()
        print(f"Model '{model_name}' loaded from MongoDB.")

load_dotenv()

# -------------------------
# PyTorch Model Definition
# -------------------------
class StockSentimentModel(nn.Module):
    def __init__(self):
        super(StockSentimentModel, self).__init__()
        self.lstm = nn.LSTM(input_size=1, hidden_size=32, batch_first=True)
        self.fc = nn.Sequential(
            nn.Linear(32 + 4, 64),
            nn.ReLU(),
            nn.Linear(64, 1)
        )

    def forward(self, x_seq, x_aux):
        lstm_out, _ = self.lstm(x_seq)
        h_last = lstm_out[:, -1, :]
        combined = torch.cat((h_last, x_aux), dim=1)
        return self.fc(combined)

# -------------------------
# 1. Spark Setup
# -------------------------
def setup_spark():
    return SparkSession.builder.appName("BigDataStockSentimentPrediction").getOrCreate()

# -------------------------
# 2. Generate Synthetic Data
# -------------------------
def generate_synthetic_data(spark, n=10000):
    return spark.range(n).select(*[
        (col("id") * (i + 1) % 100).cast("float").alias(f"v{i+1}") for i in range(65)
    ])

# -------------------------
# 3. Preprocessing and Scaling
# -------------------------
def preprocess_and_scale(df):
    vector_cols = [f"v{i}" for i in range(1, 66)]

    for col_name in vector_cols:
        if col_name in df.columns: # Check if column exists
            df = df.withColumn(col_name, col(col_name).cast(FloatType()))
        else:
            # Optionally, handle cases where a column might be missing,
            # though for v1-v65 this might indicate an upstream issue.
            print(f"Warning: Column '{col_name}' not found in DataFrame. Skipping its conversion.")

    assembler = VectorAssembler(inputCols=vector_cols, outputCol="features_vector")
    df_vectorized = assembler.transform(df).select("features_vector")

    scaler = StandardScaler(inputCol="features_vector", outputCol="scaled_features", withMean=True, withStd=True)
    scaler_model = scaler.fit(df_vectorized)
    scaled_df = scaler_model.transform(df_vectorized).select("scaled_features")

    #scaled_np = np.array(scaled_df.rdd.map(lambda row: row["scaled_features"].toArray()).collect(), dtype=np.float32)

    # Collect Row objects containing Vector objects first
    collected_rows = scaled_df.select("scaled_features").collect()

    # Then convert to NumPy array on the driver
    scaled_list = []
    for row in collected_rows:
        if row["scaled_features"] is not None:
            scaled_list.append(row["scaled_features"].toArray())
        else:
            # Decide how to handle nulls, e.g., append a row of NaNs or skip
            # For now, let's assume you might want to skip or use a placeholder
            pass # Or append(np.full(expected_vector_size, np.nan)) if you know the size

    if scaled_list:
        scaled_np = np.array(scaled_list, dtype=np.float32)
    else:
        scaled_np = np.array([], dtype=np.float32)




    stock_seq = scaled_np[:, 0:60].reshape(-1, 60, 1)
    aux_input = scaled_np[:, 60:64]
    target = scaled_np[:, 64]

    return train_test_split(stock_seq, aux_input, target, test_size=0.2, random_state=42)

# -------------------------
# 4. Convert to PyTorch Tensors
# -------------------------
def convert_to_tensors(X_seq_train, X_seq_test, X_aux_train, X_aux_test, y_train, y_test):
    return (
        torch.tensor(X_seq_train),
        torch.tensor(X_seq_test),
        torch.tensor(X_aux_train),
        torch.tensor(X_aux_test),
        torch.tensor(y_train).unsqueeze(1),
        torch.tensor(y_test).unsqueeze(1)
    )

# -------------------------
# 5. Train the Model
# -------------------------
def train_model(model, X_seq_train, X_aux_train, y_train, epochs=5):
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    for epoch in range(epochs):
        model.train()
        output = model(X_seq_train, X_aux_train)
        loss = criterion(output, y_train)

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        print(f"Epoch {epoch+1}/{epochs} - Loss: {loss.item():.4f}")

# -------------------------
# 6. Save Model to MongoDB
# -------------------------
def save_model_to_mongodb(model, db_name="VibeTraderNN", collection_name="model_weights"):
    client = MongoClient(os.environ.get("MDB_CONNECTION_STRING"))
    db = client[db_name]
    collection = db[collection_name]
    collection.delete_many({})

    buffer = io.BytesIO()
    torch.save(model.state_dict(), buffer)
    buffer.seek(0)

    collection.insert_one({
        "model_name": "stock_sentiment_lstm",
        "weights": Binary(buffer.read())
    })
    print("✅ Model saved to MongoDB")

# -------------------------
# 7. Load Model from MongoDB
# -------------------------
def load_model_from_mongodb(db_name="VibeTraderNN", collection_name="model_weights"):
    client = MongoClient(os.environ.get("MDB_CONNECTION_STRING"))
    db = client[db_name]
    collection = db[collection_name]
    doc = collection.find_one({"model_name": "stock_sentiment_lstm"})

    buffer = io.BytesIO(doc["weights"])
    buffer.seek(0)

    model = StockSentimentModel()
    model.load_state_dict(torch.load(buffer))
    print("✅ Model loaded from MongoDB")
    return model

# -------------------------
# 8. Evaluate Loaded Model
# -------------------------
def evaluate_model(model, X_seq_test, X_aux_test, y_test):
    model.eval()
    with torch.no_grad():
        predictions = model(X_seq_test, X_aux_test)
        print("\nSample Predictions (Loaded Model):")
        for pred, true in zip(predictions[:5], y_test[:5]):
            print(f"Pred: {pred.item():.2f} | True: {true.item():.2f}")
    return predictions

# PREDICTION
def predict_with_model(model: StockSentimentModel, input_numpy_array: np.ndarray) -> float:
    """
    Predicts a single floating-point value using the loaded StockSentimentModel.

    Args:
        model (StockSentimentModel): The loaded and trained PyTorch model.
        input_numpy_array (np.ndarray): A 1D NumPy array containing 64
                                        floating-point values, scaled appropriately
                                        as per the model's training data.

    Returns:
        float: The predicted floating-point value.
    """
    if not isinstance(input_numpy_array, np.ndarray):
        raise TypeError("Input must be a NumPy array.")
    if input_numpy_array.shape != (64,):
        raise ValueError(f"Input NumPy array must have shape (64,). Got {input_numpy_array.shape}")
    if not np.issubdtype(input_numpy_array.dtype, np.floating):
        # Convert to float32 if it's some other float type or even integer, to be safe
        input_numpy_array = input_numpy_array.astype(np.float32)
        print("Warning: Input array was cast to np.float32.")


    # 1. Reshape and split the input NumPy array for the model
    # First 60 features are for the LSTM sequence
    # Reshape to (batch_size=1, sequence_length=60, input_features_per_step=1)
    x_seq_np = input_numpy_array[0:60].reshape(1, 60, 1)

    # Next 4 features (from index 60 to 63) are auxiliary inputs
    # Reshape to (batch_size=1, num_aux_features=4)
    x_aux_np = input_numpy_array[60:64].reshape(1, 4)

    # 2. Convert NumPy arrays to PyTorch tensors
    # The model expects torch.float32 by default for its weights and computations
    x_seq_tensor = torch.tensor(x_seq_np, dtype=torch.float32)
    x_aux_tensor = torch.tensor(x_aux_np, dtype=torch.float32)

    # 3. Set the model to evaluation mode
    # This disables layers like Dropout or BatchNorm if they were used,
    # which is important for consistent inference.
    model.eval()

    # 4. Perform inference without calculating gradients
    with torch.no_grad():
        prediction_tensor = model(x_seq_tensor, x_aux_tensor)

    # 5. Extract the single floating-point prediction value
    predicted_value = prediction_tensor.item()

    return predicted_value

def unit_test_final_model():
    spark = setup_spark()
    df = generate_synthetic_data(spark)
    print(df.columns)
    print(df.show(5))
    
    X_seq_train, X_seq_test, X_aux_train, X_aux_test, y_train, y_test = preprocess_and_scale(df)
    X_seq_train, X_seq_test, X_aux_train, X_aux_test, y_train, y_test = convert_to_tensors(
        X_seq_train, X_seq_test, X_aux_train, X_aux_test, y_train, y_test
    )

    model = StockSentimentModel()
    train_model(model, X_seq_train, X_aux_train, y_train)
    save_model_to_mongodb(model, collection_name="MSFT")

    loaded_model = load_model_from_mongodb(collection_name="MSFT")
    

    evaluate_model(loaded_model, X_seq_test, X_aux_test, y_test)
    

    mock_data = np.random.randn(64).astype(np.float32)
    pred=predict_with_model(loaded_model, mock_data)
    print(f"Predictions for {mock_data}: || {pred} ||")

    spark.stop()

def unit_test_stock_predictor():
    # Example usage of the StockPredictor class
    predictor = StockPredictor(company_name="META")
    x_train, y_train = predictor.create_sequences("test/META_10_years_data.csv")
    predictor.build_model()
    predictor.train(x_train, y_train)
    next_price = predictor.predict_next()
    print(f"Predicted next price: {next_price}")

if __name__ == "__main__":
    # Example usage of the StockPredictor class
    #unit_test_stock_predictor()
    ##############################################################
    # Example usage of the PyTorch model
    unit_test_final_model()
