#!/usr/bin/env python
# coding: utf-8

# #### Producer

# In[ ]:


import json
from confluent_kafka import Producer
import requests
import time

# Confluent Kafka configuration with credentials
conf = {
    'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'W2456P7LEILCKLFR',
    'sasl.password': 'iXwBlnQ9nZ59wHmkPDU+XlCqpOCivLRmNHlVkzpPyRLSRYUe1nhIdUYMeMuaxEHc'
}

producer = Producer(conf)

# Function to fetch data from CoinGecko API
def fetch_coin_data():
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 10,
        'page': 1,
        'sparkline': 'false',
        'x_cg_demo_api_key': 'CG-UQwdLdQFYQ3P4JBapi6sPTsZ'
    }
    response = requests.get(url, params=params)
    
    # Ensure response is valid JSON
    try:
        return response.json()  # Parse JSON response
    except json.JSONDecodeError as e:
        print("Failed to parse API response as JSON:", e)
        print("Response content:", response.text)
        return []

# Producer loop to stream data to Kafka
while True:
    coin_data = fetch_coin_data()
    print("Fetched Coin Data:", coin_data)  # Debug: Print the API response

    # Iterate over the data and produce messages
    for coin in coin_data:
        try:
            # Ensure 'coin' is a dictionary and has the expected keys
            if isinstance(coin, dict) and 'id' in coin:
                producer.produce('data228-project-topic', key=str(coin['id']), value=json.dumps(coin))
                print(f"Produced record to topic 'data228-project-topic': {coin['id']}")
            else:
                print(f"Skipping invalid coin data: {coin}")
        except Exception as e:
            print(f"Error producing record for coin: {coin}. Error: {e}")
    producer.flush()
    time.sleep(60)  # Fetch data every 60 seconds


# #### Consumer

# In[ ]:


import json
import boto3
from confluent_kafka import Consumer, KafkaException

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'W2456P7LEILCKLFR',
    'sasl.password': 'iXwBlnQ9nZ59wHmkPDU+XlCqpOCivLRmNHlVkzpPyRLSRYUe1nhIdUYMeMuaxEHc',
    'group.id': 'crypto-consumer-group',
    'auto.offset.reset': 'earliest'
}

TOPIC = "data228-project-topic"
S3_BUCKET_NAME = "data228-project"
S3_FILE_PATH = "raw/crypto_data.json"

# AWS S3 Client
s3_client = boto3.client('s3')

# Consume messages from Kafka and upload to S3
def consume_and_upload_to_s3():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    records = []

    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break
            if msg.error():
                raise KafkaException(msg.error())
            value = msg.value().decode('utf-8')
            records.append(json.loads(value))

        # Upload to S3
        if records:
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=S3_FILE_PATH,
                Body=json.dumps(records, indent=2),
                ContentType="application/json"
            )
            print(f"Uploaded {len(records)} records to S3: s3://{S3_BUCKET_NAME}/{S3_FILE_PATH}")
        else:
            print("No records to upload.")

    finally:
        consumer.close()

if __name__ == "__main__":
    consume_and_upload_to_s3()


# #### MapReduce

# In[1]:


import sys
import json

for line in sys.stdin:
    record = json.loads(line)
    print(f"{record['name']}\t{record['market_cap']}")

import sys
from collections import defaultdict

market_cap_totals = defaultdict(float)

for line in sys.stdin:
    name, market_cap = line.strip().split('\t')
    market_cap_totals[name] += float(market_cap)

for name, total in market_cap_totals.items():
    print(f"{name}\t{total}")


# #### Algorithms

# In[2]:


# Bloom Filter

from pybloom_live import BloomFilter
import json

# Load Data
with open('s3://data228-project/raw/crypto_data.json', 'r') as f:
    data = json.load(f)

# Initialize Bloom Filter
bloom = BloomFilter(capacity=10000, error_rate=0.01)

# Check for Duplicates
for record in data:
    if record['id'] in bloom:
        print(f"Duplicate detected: {record['id']}")
    else:
        bloom.add(record['id'])
    
# LSH

from pyspark.sql import SparkSession
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.sql.functions import col

# Spark Session
spark = SparkSession.builder.appName("LSH").getOrCreate()

# Load Data from S3
data = spark.read.json("s3://data228-project/raw/crypto_data.json")

# Select Features
lsh_data = data.select("market_cap", "total_volume").dropna()

# Create LSH Model
lsh = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", bucketLength=1.0)
model = lsh.fit(lsh_data)

# Transform Data
transformed_data = model.transform(lsh_data)

# Find Nearest Neighbors
neighbors = model.approxSimilarityJoin(transformed_data, transformed_data, 0.5, distCol="EuclideanDistance")

neighbors.show()

# Differential Privacy

import numpy as np
import json

# Load Data
with open('s3://data228-project/raw/crypto_data.json', 'r') as f:
    data = json.load(f)

# Add Laplace Noise
epsilon = 1.0
for record in data:
    record['market_cap'] += np.random.laplace(0, 1/epsilon)

# Save Back to S3
with open('s3://data228-project/processed/differential_privacy.json', 'w') as f:
    json.dump(data, f)


# Flajolet-Martin

from hashlib import sha256
import json

def trailing_zeros(hash):
    return len(hash) - len(hash.rstrip('0'))

# Load Data
with open('s3://data228-project/raw/crypto_data.json', 'r') as f:
    data = json.load(f)

# Hash and Count
max_zeros = 0
for record in data:
    h = bin(int(sha256(record['id'].encode('utf-8')).hexdigest(), 16))[2:]
    max_zeros = max(max_zeros, trailing_zeros(h))

# Distinct Count Estimate
distinct_count = 2 ** max_zeros
print("Estimated Distinct Cryptocurrencies:", distinct_count)

# Reservoir Sampling

import random

# Load Data
with open('s3://data228-project/raw/crypto_data.json', 'r') as f:
    data = json.load(f)

k = 10
reservoir = []
for i, record in enumerate(data):
    if i < k:
        reservoir.append(record)
    else:
        j = random.randint(0, i)
        if j < k:
            reservoir[j] = record

print("Reservoir Sample:", reservoir)

# Explainable AI

import shap
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import boto3
import json

# Load data from S3
s3 = boto3.client('s3')
bucket_name = "data228-project"
key = "raw/crypto_data.json"

response = s3.get_object(Bucket=bucket_name, Key=key)
data = json.loads(response['Body'].read())

# Parse data into a DataFrame
df = pd.DataFrame(data)
df = df[['market_cap', 'total_volume', 'current_price', 'price_change_24h']].dropna()

# Feature Engineering
X = df[['total_volume', 'current_price', 'price_change_24h']]
y = df['market_cap']

# Train-Test Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train a Random Forest Regressor
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Explain the model's predictions using SHAP
explainer = shap.Explainer(model, X_train)
shap_values = explainer(X_test)

# Summary Plot
shap.summary_plot(shap_values, X_test, plot_type="bar")

# Detailed Visualization for a single prediction
shap.force_plot(explainer.expected_value, shap_values[0, :], X_test.iloc[0, :], matplotlib=True)


# #### Visualizations

# In[ ]:


import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from confluent_kafka import Consumer, KafkaException

# Kafka Consumer Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'W2456P7LEILCKLFR',
    'sasl.password': 'iXwBlnQ9nZ59wHmkPDU+XlCqpOCivLRmNHlVkzpPyRLSRYUe1nhIdUYMeMuaxEHc',
    'group.id': 'crypto-consumer-group',
    'auto.offset.reset': 'earliest'
}

TOPIC = "data228-project-topic"

# Fetch Data from Kafka
def fetch_kafka_data():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    records = []

    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break
            if msg.error():
                raise KafkaException(msg.error())
            value = msg.value().decode('utf-8')
            records.append(json.loads(value))
    finally:
        consumer.close()

    return records

# Data Parsing
def parse_data(records):
    df = pd.DataFrame(records)
    # Filter meaningful columns
    df = df[['name', 'current_price', 'market_cap', 'total_volume', 'price_change_24h']].dropna()
    df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce')
    df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce')
    df['total_volume'] = pd.to_numeric(df['total_volume'], errors='coerce')
    df['price_change_24h'] = pd.to_numeric(df['price_change_24h'], errors='coerce')
    return df.dropna()

# Generate Visualizations
def generate_visualizations(df):
    visualizations = []
    
    # 1. Scatter Plot: Market Cap vs. Total Volume
    plt.figure(figsize=(12, 6))
    plt.scatter(df['market_cap'], df['total_volume'], alpha=0.7, edgecolors='k')
    plt.title('Market Cap vs. Total Volume')
    plt.xlabel('Market Cap (USD)')
    plt.ylabel('Total Volume (USD)')
    plt.grid()
    plt.savefig('scatter_market_cap_vs_volume.png')
    visualizations.append('scatter_market_cap_vs_volume.png')
    plt.close()

    # 2. Heatmap: Cryptocurrency Metrics
    heatmap_data = df[['current_price', 'market_cap', 'total_volume']].div(df[['current_price', 'market_cap', 'total_volume']].max())
    plt.figure(figsize=(10, 6))
    sns.heatmap(heatmap_data, annot=False, cmap='YlGnBu', cbar=True)
    plt.title('Heatmap of Metrics')
    plt.savefig('heatmap_metrics.png')
    visualizations.append('heatmap_metrics.png')
    plt.close()

    # 3. Line Chart: Market Cap by Cryptocurrency
    plt.figure(figsize=(12, 6))
    plt.plot(df['name'], df['market_cap'], marker='o', linestyle='-', label='Market Cap')
    plt.title('Market Cap by Cryptocurrency')
    plt.xlabel('Cryptocurrency')
    plt.ylabel('Market Cap (USD)')
    plt.xticks(rotation=45)
    plt.grid()
    plt.savefig('line_chart_market_cap.png')
    visualizations.append('line_chart_market_cap.png')
    plt.close()

    # 4. Bubble Chart: Market Cap vs. Current Price
    plt.figure(figsize=(12, 6))
    plt.scatter(df['market_cap'], df['current_price'], s=df['total_volume'] * 0.0001, alpha=0.6, c='blue', edgecolors='k')
    plt.title('Bubble Chart: Market Cap vs. Current Price')
    plt.xlabel('Market Cap (USD)')
    plt.ylabel('Current Price (USD)')
    plt.grid()
    plt.savefig('bubble_chart_market_cap_price.png')
    visualizations.append('bubble_chart_market_cap_price.png')
    plt.close()

    # 5. Horizontal Stacked Bar Chart
    normalized_data = df[['current_price', 'market_cap', 'total_volume']].div(df[['current_price', 'market_cap', 'total_volume']].sum(axis=1), axis=0)
    normalized_data.index = df['name']
    normalized_data.plot(kind='barh', stacked=True, figsize=(12, 6), cmap='tab20c')
    plt.title('Stacked Bar Chart of Cryptocurrency Metrics')
    plt.xlabel('Proportion')
    plt.ylabel('Cryptocurrency')
    plt.savefig('stacked_bar_chart.png')
    visualizations.append('stacked_bar_chart.png')
    plt.close()

    # 6. Density Plot: Current Price Distribution
    plt.figure(figsize=(12, 6))
    sns.kdeplot(df['current_price'], fill=True, color='purple')
    plt.title('Density Plot of Current Price')
    plt.xlabel('Current Price (USD)')
    plt.ylabel('Density')
    plt.grid()
    plt.savefig('density_plot_current_price.png')
    visualizations.append('density_plot_current_price.png')
    plt.close()

    # 7. Histogram: Market Cap Distribution
    plt.figure(figsize=(12, 6))
    plt.hist(df['market_cap'], bins=20, color='green', alpha=0.7, edgecolor='k')
    plt.title('Market Cap Distribution')
    plt.xlabel('Market Cap (USD)')
    plt.ylabel('Frequency')
    plt.grid()
    plt.savefig('histogram_market_cap.png')
    visualizations.append('histogram_market_cap.png')
    plt.close()

    # 8. Scatter Plot: Price Change vs. Market Cap
    plt.figure(figsize=(12, 6))
    plt.scatter(df['market_cap'], df['price_change_24h'], alpha=0.7, edgecolors='k', c='red')
    plt.title('Price Change vs. Market Cap')
    plt.xlabel('Market Cap (USD)')
    plt.ylabel('Price Change (USD)')
    plt.grid()
    plt.savefig('scatter_price_change_vs_market_cap.png')
    visualizations.append('scatter_price_change_vs_market_cap.png')
    plt.close()

    # 9. Radar Chart: Metric Profiles of Top Cryptocurrencies
    from math import pi
    top_coins = df.nlargest(5, 'market_cap')
    metrics = ['current_price', 'market_cap', 'total_volume']
    radar_data = top_coins[metrics].div(top_coins[metrics].max())
    radar_data.index = top_coins['name']
    angles = [n / float(len(metrics)) * 2 * pi for n in range(len(metrics))]
    angles += angles[:1]
    plt.figure(figsize=(8, 8))
    ax = plt.subplot(111, polar=True)
    for i, row in radar_data.iterrows():
        values = row.tolist()
        values += values[:1]
        ax.plot(angles, values, label=i)
        ax.fill(angles, values, alpha=0.1)
    plt.title('Radar Chart of Top Cryptocurrencies')
    plt.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1))
    plt.savefig('radar_chart.png')
    visualizations.append('radar_chart.png')
    plt.close()

    # 10. Violin Plot: Total Volume Distribution
    plt.figure(figsize=(12, 6))
    sns.violinplot(data=df, x='name', y='total_volume', scale='width', inner='quartile', palette='muted')
    plt.title('Violin Plot: Total Volume Distribution')
    plt.xlabel('Cryptocurrency')
    plt.ylabel('Total Volume (USD)')
    plt.xticks(rotation=45)
    plt.grid()
    plt.savefig('violin_plot_total_volume.png')
    visualizations.append('violin_plot_total_volume.png')
    plt.close()

    return visualizations

# Main Function
if __name__ == "__main__":
    print("Fetching data from Kafka...")
    records = fetch_kafka_data()
    if not records:
        print("No data available from Kafka.")
    else:
        print(f"Total messages fetched: {len(records)}")
        df = parse_data(records)
        visualizations = generate_visualizations(df)
        print(f"Visualizations created: {visualizations}")

