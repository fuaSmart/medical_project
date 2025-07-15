# 💊 Ethiopian Medical Data Pipeline

This project aims to build a robust data pipeline for collecting, transforming, enriching, and serving data from public Telegram channels related to Ethiopian medical businesses. The pipeline leverages modern data stack tools to create a clean, trusted, and analyzable data product.

## ✨ Project Overview

The pipeline consists of the following main components (tasks):

1.  **Data Scraping and Collection (Extract & Load)**:
    * Extracts messages and media from specified public Telegram channels.
    * Stores raw, unaltered data in a structured data lake (local files).
    * Loads raw data into a PostgreSQL database for further processing.

2.  **Data Modeling and Transformation (Transform)**:
    * Uses dbt (data build tool) to transform messy raw data into a clean, structured star schema in PostgreSQL.
    * Includes staging models, dimension tables (`dim_channels`, `dim_dates`), and a fact table (`fct_messages`).
    * Implements data quality tests to ensure reliability.

3.  **Data Enrichment with Object Detection (YOLO)**:
    * Applies a pre-trained YOLOv8 model to detect objects in scraped images.
    * Integrates detection results into the data warehouse (`fct_image_detections` table), linking visual content to messages.

4.  **Analytical API (FastAPI)**:
    * Provides a RESTful API to query the transformed data (dbt data marts).
    * Offers analytical endpoints to answer key business questions (e.g., top-mentioned products, channel activity, message search).

5.  **Pipeline Orchestration (Dagster)**:
    * Uses Dagster to define, schedule, and monitor the entire data pipeline workflow.
    * Ensures reliable and automated execution of all pipeline steps.

## ⚙️ Technology Stack

* **Telegram Scraping**: `Telethon` (Python)
* **Database**: `PostgreSQL`
* **Data Transformation**: `dbt` (data build tool)
* **Object Detection**: `YOLOv8` (`ultralytics` Python package)
* **API Framework**: `FastAPI` (Python)
* **Orchestration**: `Dagster`
* **Containerization**: `Docker` / `Docker Compose`
* **Version Control**: `Git` / `GitHub`
* **CI/CD**: `GitHub Actions`

## 🚀 Getting Started

### Prerequisites

* Docker and Docker Compose installed.
* Python 3.9+
* Git

### 1. Clone the Repository

```bash
git clone [https://github.com/YOUR_USERNAME/medical_project.git](https://github.com/YOUR_USERNAME/medical_project.git)
cd medical_project