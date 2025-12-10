# Real-time E-Commerce RFM Segmentation (Lambda Architecture)

**Author:** Peiyuan Luo (CNETID: peiyuanluo)
**Course:** MPCS 53014 - Big Data Application Architecture
**Date:** December 2025

## 📖 0. Dataset Background & Processing Objectives

### 📊 The Dataset
The project utilizes the classic **Online Retail Dataset**, a transnational data set containing transactions occurring between 2010 and 2011 for a UK-based and registered non-store online retail.
* **Scale:** Approximately 540,000+ transaction records.
* **Key Features:**
    * `InvoiceNo`: Transaction ID.
    * `StockCode` & `Description`: Product details.
    * `Quantity` & `UnitPrice`: Transaction volume and value.
    * `CustomerID`: Unique identifier for user profiling.
    * `Country`: Customer location.
* **Business Context:** The company mainly sells unique all-occasion gifts. Many customers are wholesalers.

### 🎯 Processing Objectives
Raw transactional logs are noisy and unstructured. The goal of this project is to transform this raw data into actionable **Business Intelligence** through the following steps:

1.  **Data Cleaning:**
    * Filter out cancelled transactions (records with negative `Quantity`).
    * Remove records with missing `CustomerID` (which cannot be profiled).
    * Ensure data integrity by validating `UnitPrice`.

2.  **Feature Engineering (RFM Model):**
    We transform time-series transactions into the industry-standard **RFM** metrics for each user:
    * **Recency (R):** How many days have passed since the customer's last purchase?
    * **Frequency (F):** How many distinct orders has the customer placed?
    * **Monetary (M):** What is the total lifetime value (LTV) of the customer?

3.  **Machine Learning Segmentation:**
    Using **K-Means Clustering** in Spark MLlib, we group customers into distinct segments (e.g., *"Loyal/Frequent"* vs. *"Churn Risk"*). This static model (Batch Layer) then serves as the "Knowledge Base" for tagging real-time incoming orders (Speed Layer).

## 📖 1. Project Background & Objective

### Context
In the competitive e-commerce landscape, understanding customer behavior is critical. Businesses need to identify high-value customers (VIPs) to reward them and at-risk customers to prevent churn.

### The Problem
Traditional analytics often rely solely on **Batch Processing**, which provides deep insights but with high latency (e.g., "Yesterday's Report"). On the other hand, pure **Stream Processing** often lacks historical context.

### The Solution
This project implements a **Lambda Architecture** to combine the best of both worlds:
1.  **Batch Layer:** Analyzes years of historical data to build accurate customer profiles using the **RFM Model** (Recency, Frequency, Monetary) and **K-Means Clustering**.
2.  **Speed Layer:** Processes live order streams in real-time, instantly tagging new orders with the customer's historical segment.
3.  **Serving Layer:** A web dashboard that visualizes batch analytics and allows for real-time customer lookups.

---

## 📂 2. Project Structure & File Locations

Here is the organization of the source code and where each file is located:

```text
peiyuanluo_final_project/
│
├── online_retail.csv     # The dataset
├── pom.xml                        # Maven build configuration (dependencies for Spark, Kafka, Hive)
│
├── FinalProjectScala
│   └── src/
│       └── main/
│           └── scala/
│               ├── RfmBatch.scala     # [Batch Layer] Spark job for ETL, RFM Calc, K-Means -> Hive
│               ├── RfmStreaming.scala # [Speed Layer] Spark Structured Streaming job (Kafka + Hive Join)
│               └── OrderProducer.scala# [Data Source] Scala app that generates mock orders to AWS MSK
│
└── rfm_webapp/                    # [Serving Layer] Web Application
    ├── app.js                     # Node.js/Express Backend (SSH/Beeline connection to Hive)
    ├── package.json               # Web dependencies (Express, etc.)
    └── public/
        └── index.html             # Frontend Dashboard (Chart.js, Real-time feed)
```


## 3. Architecture & Tech Stack
* Compute: Apache Spark 

* Storage: HDFS (Raw Data), Apache Hive (Data Warehouse)

* Messaging: AWS MSK (Managed Streaming for Kafka) 

* Web: Node.js, Express, PM2

* Language: Scala 2.12, JavaScript

## 🚀 4. How to Run (Step-by-Step Guide)

### Step 0: Prerequisites & Build
Ensure you are logged into the cluster gateway. Build the "Fat Jar" (Uberjar) containing all dependencies.

```bash
# Clean and build the project
mvn clean package
```

Output Artifact: `target/uber-FinalProjectScala-1.0-SNAPSHOT.jar`


### Step 1: Run the Batch Layer
This job reads the historical CSV (online_retail.csv), performs data cleaning, calculates RFM metrics, trains the K-Means model, and saves the results to Hive tables.

Command:

```bash
## Go to the cluster
cd /home/hadoop/peiyuanluo/finalProject/target

spark-submit \
--class RfmBatch \
--master yarn \
--deploy-mode client \
uber-FinalProjectScala-1.0-SNAPSHOT.jar
```
Input: /user/hadoop/peiyuanluo/finalProject/online_retail.csv

Output: Hive Tables (`default.peiyuanluo_ecom_user_rfm_batch`, `default.peiyuanluo_ecom_user_segments`)


Note: If you encounter HDFS write errors, clear the staging directory:


```bash
hdfs dfs -rm -r -f /user/hadoop/.sparkStaging
```


### Step 2: Run the Speed Layer (Real-time Ingestion)
Open two separate terminal windows for this step.

##### Terminal A: Kafka Producer Generates simulated real-time orders and sends them to the AWS MSK topic peiyuanluo_orders.

```bash
java -cp uber-FinalProjectScala-1.0-SNAPSHOT.jar OrderProducer

```
(Status: You should see "✅ Sent: ..." logs scrolling)

#### Terminal B: Spark Streaming Engine Consumes data from Kafka and joins it with the Hive Segment table created in Step 1.

```bash
spark-submit \
--class RfmStreaming \
--master yarn \
--deploy-mode client \
uber-FinalProjectScala-1.0-SNAPSHOT.jar
```
(Status: You should see "Batch: 0", "Batch: 1" tables appearing)

## Step 3: Run the Serving Layer (Web App)
The web application runs on the edge node and connects to the Hive Master Node via SSH Tunnel/Beeline to fetch real data.

### 1. Navigate to the web app directory on the web server:

```bash
cd /home/ec2-user/peiyuanluo/rfm_webapp

```


### 2. Install dependencies (First time only):

```bash
npm install
```

### 3. Start the Server using PM2:

```bash
pm2 delete peiyuanluo_app  # Cleanup old process if any
pm2 start app.js --name peiyuanluo_app
```

### 4. Access the Dashboard: Open your browser and navigate to: http://ec2-52-20-203-80.compute-1.amazonaws.com:3034/

## 📊 5. What the Project Does (Functional Demo)
### 1.*Customer Segmentation (Batch View)*: The dashboard displays charts showing the distribution of customers across 4 clusters:

* Cluster 0: Churn Risk (High Recency, Low Frequency)

* Cluster 1: Loyal / Frequent (High Frequency, Recent activity)

* Cluster 2: New / Promising (Recent activity, Low Frequency)

* Cluster 3: High Value VIP (Very High Monetary value)

### 2. Real-time Ingestion Monitor: The right-side panel on the dashboard ("Live Kafka Stream") visualizes orders as they are ingested by the Speed Layer.

### 3.Real-time Customer Lookup: Users can enter a Customer ID (e.g., 12347 or 12348) into the search box.

* The system queries the Hive Data Warehouse in real-time.

* It instantly returns the customer's segment (e.g., "Loyal / Frequent"), enabling immediate marketing actions.