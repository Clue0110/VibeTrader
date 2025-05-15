from custom_models import *
from sentiment import *
from news import *
from stock import *

def per_hour_routine(ticker: str = "^OEX"):
    """
        This function is called every hour to predict the stock price of a company.
    """
    #Get ticker mapping
    ticker_mapping = {
        "AAPL": "Apple",
        "GOOGL": "Google",
        "MSFT": "Microsoft",
        "AMZN": "Amazon",
        "TSLA":"Tesla",
        "NFLX": "Netflix",
        "META": "Meta",
        "^OEX": "S&P 500"
    }
    #Step 1: Fetch the last 60 hr stock data
    seq_stock_60=fetch_prev_stock_data(ticker, period=60,freq="h")

    #Step 2: Get Last 24 hour news data
    news_data_comprehensive=get_news_with_text(ticker_mapping[ticker])
    news_data_text=convert_news_array_to_text(news_data_comprehensive)

    #Step 3: Fetch Prediction Model and predict stock price (Algorithmic Trading)
    predictor = StockPredictor(ticker)
    stock_pred_value=predictor.user_predict(seq_stock_60)

    #Step 4: Fetch Sentiment Score of the news data
    sentiment_score_vader=get_sentiment_score(news_data_text)
    sentiment_score_ml=get_sentiment_score_JLabsML(news_data_text)
    sentiment_score_llm=get_sentiment_score_llm_gemini(news_data_text)

    #Step 5: Final Prediction Model
    new_predictor_values = np.array([stock_pred_value, sentiment_score_vader, sentiment_score_ml, sentiment_score_llm])
    final_predictor_values=np.append(seq_stock_60, new_predictor_values)
    loaded_model = load_model_from_mongodb(collection_name=ticker)
    final_predicted_stock_value=predict_with_model(loaded_model, final_predictor_values)

    #Step 6: Save the data to Redis
    # Save Predicted stock value (both algo trade + algoTradewithsenti)

    #Step 7: Return the final_predicator_row+final_predicted_stock_value
    return np.append(final_predicted_stock_value, np.array([final_predicted_stock_value]))

def per_day_routine(ticker: str = "^OEX"):
    #Step 0: Fetch Last 5 years stock data
    stock_df=None #Get a dataframe
    stock_senti_df=None #Get a dataframe

    #Step 1: Train an Algorithmic Trading model for last 5 years
    predictor = StockPredictor(ticker)
    x_train, y_train = predictor.create_sequences(df_data=stock_df)
    predictor.build_model()
    predictor.train(x_train,y_train)

    #Step 2: Train Final Prediction Model for last 5 years
    X_seq_train, X_seq_test, X_aux_train, X_aux_test, y_train, y_test = preprocess_and_scale(stock_senti_df)
    X_seq_train, X_seq_test, X_aux_train, X_aux_test, y_train, y_test = convert_to_tensors(
        X_seq_train, X_seq_test, X_aux_train, X_aux_test, y_train, y_test
    )
    model = StockSentimentModel()
    train_model(model, X_seq_train, X_aux_train, y_train)
    save_model_to_mongodb(model,collection_name=ticker)

    #Step 3: Save the data to SQL
