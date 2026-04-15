# Real-Time Disaster Detector Pipeline  
Airflow + Kafka + Spark + Snowflake

## Overview
This project implements a real-time disaster detection pipeline that ingests, processes, and analyzes unstructured text data to identify potential disaster events instantly. It combines modern data engineering tools with machine learning to deliver actionable insights in real time.

The system is designed for scalability, low latency, and high accuracy, enabling organizations to monitor and respond to disasters as they happen.

---

## Key Features
- Real-time data ingestion using Apache Kafka  
- Workflow orchestration with Apache Airflow  
- BERT-based NLP model for disaster classification (>90% accuracy)  
- Named Entity Recognition (NER) powered by Ollama for extracting:
  - Disaster types  
  - Geographic locations  
- Distributed processing with Apache Spark  
- Cloud data warehousing using Snowflake  
- Real-time dashboard in Power BI for monitoring disaster events  

---

## Architecture

```
Data Source → Kafka → Spark Processing → ML Model (BERT)
                          ↓
                    NER (Ollama)
                          ↓
                     Snowflake
                          ↓
                    Power BI Dashboard
```

Orchestration is handled by Apache Airflow.

---

## Machine Learning

### Disaster Classification
- Model: BERT (Bidirectional Encoder Representations from Transformers)  
- Task: Binary classification (Disaster / Non-Disaster)  
- Accuracy: >90%  

### Named Entity Recognition (NER)
- Tool: Ollama  
- Extracts:
  - Disaster types (e.g., flood, earthquake)  
  - Locations (cities, countries, regions)  

---

## Tech Stack

| Layer              | Technology              |
|-------------------|------------------------|
| Ingestion         | Apache Kafka           |
| Orchestration     | Apache Airflow         |
| Processing        | Apache Spark           |
| ML Model          | BERT                   |
| NER               | Ollama                 |
| Data Warehouse    | Snowflake              |
| Visualization     | Power BI               |

---

## Dashboard
The Power BI dashboard provides:
- Real-time disaster alerts  
- Geographic distribution of events  
- Disaster type trends  
- Actionable insights for decision-makers  

---

## Setup and Installation

### 1. Clone the repository
```bash
git clone https://github.com/Andyvo1009/threads-disaster-detector.git
cd threads-disaster-detector
```

### 2. Set up environment
```bash
pip install -r requirements.txt
```

### 3. Start services
- Kafka and Zookeeper  
- Spark cluster  
- Airflow scheduler and webserver  

### 4. Configure
- Snowflake credentials  
- Kafka topics  
- Airflow DAGs  

---

## Running the Pipeline

1. Start Kafka producers to stream text data  
2. Trigger the Airflow DAG  
3. Spark processes incoming data  
4. The model classifies disaster-related text  
5. NER extracts entities  
6. Data is stored in Snowflake  
7. Power BI dashboard updates in real time  

---

## Use Cases
- Disaster monitoring systems  
- Emergency response coordination  
- Social media intelligence  
- Government and NGO alert systems  

---

## Future Improvements
- Multi-language support  
- More advanced NER models  
- Real-time alert notifications (SMS/Email)  
- Model retraining pipeline (MLOps)  

---

## Author
Andy Vo  
GitHub: https://github.com/Andyvo1009  
