#!/usr/bin/env python3
"""
E-commerce Data Generator & Kafka Producer
Based on the original project notebook logic.
"""

import pandas as pd
import json
import random
import hashlib
import time
import sys
import os
import logging
from datetime import datetime
from confluent_kafka import Producer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Configuration & Setup ---
DATA_FILE = "data/TV_DATASET_USA.csv"
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_kafka_producer():
    """Initialize Kafka producer from environment variables"""
    conf = {
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('SASL_USERNAME'),
        'sasl.password': os.getenv('SASL_PASSWORD')
    }
    
    if not conf['bootstrap.servers'] or not conf['sasl.username'] or not conf['sasl.password']:
        logger.error("Missing environment variables. Please check .env file.")
        sys.exit(1)
        
    return Producer(conf)

# --- Variables from Notebook ---
cities = ['New York','Los Ángeles','Chicago','Houston','Philadelphia']

payment_modes = ['Credit_card','Stripe','Paypal','Apple_Pay','Google_Pay','Samsung_Pay']

payment_store = ['Cash','Credit_card']

sources = ['Facebook','Instagram','Organic','Twitter','Influencer_1','Influencer_2','Influencer_3','Influencer_4']

purchase_statuses = ['COMPLETED','FAILED_CHECKOUT','FAILED_API_RESPONSE','INSUFICCIENT_FUNDS','COMPLETED','COMPLETED','COMPLETED','COMPLETED','COMPLETED','COMPLETED','FAILED_API_RESPONSE','INSUFICCIENT_FUNDS','USER_ERROR','FRAUD','COMPLETED','COMPLETED','COMPLETED']

commission = [0.2,0.25,0.3,0.27,0.35,0.4,0.37,0.15,0.1]

NY_coords = [(40.76046814557239, -73.97764793953105),(40.76921169592604, -73.98326984936075),(40.762515994719834, -73.98095242088134)]
LA_coords = [(34.07210945806006, -118.35747350374957),(34.071754649810025, -118.37593530089991)]
CHI_coords = [(41.89819876058171, -87.62280110486684),(41.89182575694393, -87.6249468719774),(41.88375296758592, -87.62814652743663)]
HOU_coords = [(29.742233338438325, -95.44654054545151),(29.743148850926094, -95.45312636612748),(29.739981565214627, -95.46428435510245)]
PHI_coords = [(40.089499621312456, -75.39015007888118),(40.085310197975055, -75.40444450974655),(40.09069475292698, -75.3815277170056)]

# --- Helper Functions ---
def get_pay_method(sources,purchase_statuses,payment_modes,payment_store):
    if sources == 'Organic':
        payment = random.choice(payment_store)
        status = 'COMPLETED'
        order_type = 'STORE'
    elif sources != 'Organic':
        payment = random.choice(payment_modes)
        status = random.choice(purchase_statuses)
        order_type = 'ONLINE'
    return payment,status,order_type

def get_coords(city):
    if city == 'New York':
        coords = random.choice(NY_coords)
    elif city == 'Los Ángeles':
        coords = random.choice(LA_coords)
    elif city == 'Chicago':
        coords = random.choice(CHI_coords)
    elif city == 'Houston':
        coords = random.choice(HOU_coords)
    elif city == 'Philadelphia':
        coords = random.choice(PHI_coords)
    return coords

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        # Matches the print style from the notebook
        pass 

# --- Main Logic ---
def main():
    logger.info("Starting Data Generator...")

    # Load Data
    if not os.path.exists(DATA_FILE):
        logger.error(f"Data file not found: {DATA_FILE}")
        sys.exit(1)
        
    df = pd.read_csv(DATA_FILE)
    
    # Cleaning as per notebook
    # df['PRICING'] = df['PRICING'].apply(lambda x : float(x.replace('$','').replace(',','')))
    # Handling potential nulls or errors effectively
    def clean_price(x):
        try:
            return float(str(x).replace('$','').replace(',',''))
        except:
            return 0.0
            
    df['PRICING'] = df['PRICING'].apply(clean_price)
    
    producer = get_kafka_producer()
    topic = "ecommerce-topic-1"
    
    x = 1
    delivered_records = 0
    
    # Infinite loop logic adapted from the notebook's finite loop
    try:
        while True:
            # Generate one record
            date = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
            
            # Random selection
            random_index = random.randint(0, len(df)-1)
            row = df.iloc[random_index]
            
            product = row['PRODUCT_NAME'] if pd.notna(row['PRODUCT_NAME']) else "Unknown"
            pricing = row['PRICING']
            commission_temp = random.choice(commission)
            
            brand = row['BRAND'] if pd.notna(row['BRAND']) else "Unknown"
            screen = row['SCREEN_SIZE'] if pd.notna(row['SCREEN_SIZE']) else 0
            display = row['DISPLAY_TYPE'] if pd.notna(row['DISPLAY_TYPE']) else "Unknown"
            resolution = row['RESOLUTION'] if pd.notna(row['RESOLUTION']) else "Unknown"
            
            source_temp = random.choice(sources)
            pay = get_pay_method(source_temp, purchase_statuses, payment_modes, payment_store)
            city = random.choice(cities)
            
            # SHA256 ID Generation
            # ISO 8601 Date
            timestamp = datetime.now().isoformat()
            
            raw_string = f"{x} {product} {pricing} {commission_temp} {timestamp} {source_temp} {pay[1]}"
            purchase_id = str(hashlib.sha256(raw_string.encode('utf-8')).hexdigest())[:10]
            
            # Additional fields for dashboard compatibility
            devices = ['Mobile', 'Desktop', 'Tablet']
            
            purchase = {
                'transaction_id': purchase_id,
                'customer_id': f"CUST-{random.randint(10000, 99999)}",
                'product_name': product,
                'category': "Electronics",
                'price': float(pricing),
                'total_amount': float(round(pricing * commission_temp, 2)),
                'currency': "USD",
                'quantity': 1,
                'payment_method': pay[0],
                'status': pay[1],
                'order_type': pay[2],
                'city': city,
                'country': "USA",
                'device': random.choice(devices),
                'brand': brand,
                'screen_size': int(screen) if screen else 0,
                'display_type': display,
                'resolution': resolution,
                'latitude': float(get_coords(city)[0]),
                'longitude': float(get_coords(city)[1]),
                'source': source_temp,
                'timestamp': timestamp
            }
            
            # Send to Kafka
            try:
                producer.produce(
                    topic,
                    key=purchase_id,
                    value=json.dumps(purchase),
                    on_delivery=delivery_report
                )
                
                # Notebook style print
                logger.info(f"Envoi réussi : {product} | Status : {pay[1]}")
                
                delivered_records += 1
                x += 1
                
                # Maintain poll and sleep
                producer.poll(0)
                # time.sleep(random.choice([1, 1.5])) # Uncomment to slow down again if needed
                
                # Slightly faster than notebook for demo purposes, but keeping structure
                time.sleep(0.1) 
                
            except BufferError:
                logger.warning("Buffer full, waiting...")
                producer.poll(1)
                
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        logger.info(f"{delivered_records} messages were produced")
        producer.flush()

if __name__ == "__main__":
    main()
