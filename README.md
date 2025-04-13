# Web Scraping ETL Pipeline

A scalable ETL pipeline for web scraping multiple websites using Apache Airflow.

## Project Structure
```
apache-pr-pipeline/
├── airflow/
│   ├── dags/
│   │   └── scraping_dag.py
│   └── config/
│       └── airflow.cfg
├── extractors/
│   ├── base/
│   │   └── base_extractor.py
│   ├── utils/
│   │   ├── browser.py
│   │   └── validators.py
│   └── websites/
│       ├── website_1.py
│       ├── website_2.py
│       └── ...
├── config/
│   ├── database.yaml
│   └── websites/
│       ├── website_1.yaml
│       ├── website_2.yaml
│       └── ...
├── loaders/
│   ├── base/
│   │   └── base_loader.py
│   └── postgres_loader.py
├── transformers/
│   └── data_transformer.py
├── validators/
│   └── data_validator.py
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── requirements.txt
└── README.md
```

## Setup Instructions

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your database settings in `config/database.yaml`

3. Configure website-specific settings in `config/websites/*.yaml`

4. Run with Docker:
```bash
docker-compose up -d
```

## Configuration

Each website has its own configuration file in `config/websites/` containing:
- XPath selectors
- URL patterns
- Validation rules
- Data transformation rules 