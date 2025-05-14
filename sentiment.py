import sys
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import date, timedelta, datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from sparknlp.pretrained import PretrainedPipeline
import json
import sparknlp
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.types import StringType, IntegerType
import os
from typing import Optional
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

from dotenv import load_dotenv

load_dotenv()

from sparknlp.annotator import (
    SentenceDetector,
    Tokenizer,
    Lemmatizer,
    SentimentDetector
)
#nltk.download('vader_lexicon')
#spark=sparknlp.start()

def get_sentiment_score(text, get_compound_score=True):
    """Calculate the sentiment score of a given text using VADER setiment analysis.
    Args:
        text (str): Input Text for Sentiment Analysis

    Returns:
        dict: A dictionary containing the sentiment scores with following keys: (positive, negative, neutral, compound)
            "neg": This score indicates the proportion of the text that expresses a negative sentiment. It is also a float value between 0.0 and 1.0. A higher "neg" score suggests a greater presence of negative language.
            "pos": This score represents the proportion of the text that conveys a positive sentiment. It is a float value ranging from 0.0 to 1.0. A higher "pos" score indicates a greater amount of positive language used in the text
            "neu": This score signifies the proportion of the text that is neutral in sentiment. It is a float between 0.0 and 1.0. Text that is objective, factual, or doesn't express strong emotion will contribute to a higher "neu" score. 
            "compound": a value between - and 1, where -1 indicates a strong negative sentiment and +1 indicates a strong positive sentiment
    """
    nltk.download('vader_lexicon')
    sia = SentimentIntensityAnalyzer()
    sentiment_scores=sia.polarity_scores(text)
    if get_compound_score:
        return sentiment_scores['compound']
    else:
        return sentiment_scores
    
class sentiment_jslabs_ml:
  def __init__(self , name='analyze_sentiment'):
    self.pipeline = PretrainedPipeline(name, lang = 'en')
    #self.start_spark()

  def start_spark(self):
    self.spark = sparknlp.start()
  
  def stop_spark(self):
    self.spark.stop()

  def get_sentiment_scores(self, text, get_score=True):
    result = self.pipeline.fullAnnotate(text)
    result1 = self.pipeline.annotate(text)
    annotations = result[0]['sentiment']
    labels = result1.get('sentiment', [])
    res = self.get_details(annotations , labels)
    return res["normalized_score"] if get_score else res["details"]

  def get_details(self , annotations , labels):
    res = []
    weights={"positive":1,"negative":-1}
    score=0
    num_sentences=0
    for i, (label, ann) in enumerate(zip(labels, annotations)):
      confidence = ann.metadata.get('confidence')
      res.append((i+1, label, confidence))
      num_sentences+=1
      score+=weights[label]*float(confidence)
    result={
        "score":score,
        "normalized_score":score/num_sentences,
        "details":res
    }
    return result

def get_sentiment_score_JLabsML(text):
    spark=sparknlp.start()
    sentiment_obj=sentiment_jslabs_ml()
    sentiment_score=sentiment_obj.get_sentiment_scores(text, get_score=True)
    return sentiment_score


def get_sentiment_score_llm_gemini(text: str, model_name: str = "gemini-1.5-flash-latest") -> Optional[float]:
    """
    Gets a sentiment score (float) for a given text using an LLM
    The GOOGLE_API_KEY environment variable must be set.

    Args:
        text_to_analyze: The text string to analyze.
        model_name: The name of the Gemini model to use (e.g., "gemini-1.5-flash-latest").

    Returns:
        A float representing the sentiment score:
        - 1.0 for POSITIVE
        - -1.0 for NEGATIVE
        - 0.0 for NEUTRAL
        - None if sentiment cannot be determined or an error occurs.
    """
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("Error: GOOGLE_API_KEY environment variable not set.")
        return None

    try:
        # Initialize the Gemini LLM through Langchain
        llm = ChatGoogleGenerativeAI(model=model_name, google_api_key=api_key)
        # Define the prompt template
        # We ask for a specific categorical output to make parsing more reliable.
        prompt_template = ChatPromptTemplate.from_messages([
            ("system", "You are an expert in analyzing text and calculating a sentiment score."),
            ("human", "Analyze the sentiment of the following text. "
                      "Calculate the sentiment score. It should be a floating value between +1 and -1 where a value closer to +1 indicates a very positive sentiment and a value close to -1 means a very negative sentiment and a value of 0 means neutral sentiment."
                      "Return only the sentiment score without any additional text or markups."
                      "Text: \"{text}\"")
        ])
        # Initialize the output parser
        output_parser = StrOutputParser()
        # Create the chain
        chain = prompt_template | llm | output_parser
        # Invoke the chain
        raw_response_text = chain.invoke({"text": text}).strip().upper()
        #print(f"Raw response from llm gemin: {raw_response_text}")
        return float(raw_response_text.strip()) if raw_response_text else None

    except Exception as e:
        print(f"An error occurred during sentiment analysis: {e}")
        return None


if __name__=="__main__":
    text="Demonicus is a movie turned into a video game! I just love the story and the things that goes on in the film. It is a B-film ofcourse but that doesn't bother one bit because its made just right and the music was rad! Horror and sword fight freaks,buy this movie now!"
    #score=get_sentiment_score(text)
    #score=get_sentiment_score_JLabsML(text)
    score=get_sentiment_score_llm_gemini(text)
    print(f"sentiment Score: {score}")