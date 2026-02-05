# PySpark AI-Pipeline Masterkit
A production-ready template for building scalable Data Pipelines using **PySpark** and **LLM-based Data Enrichment**.



###  Features
* **Modular ETL:** Clean separation of Extraction, Transformation, and Loading.
* **AI Cleaning:** Integrated Python module to handle unstructured text using GPT-4o.
* **Idempotent Design:** Handles retries and partial failures gracefully.
* **Cloud Ready:** Optimized for AWS Glue or EMR.

###  Quick Start
```bash
git clone [https://github.com/shradhadubey/pyspark-ai-pipeline-masterkit.git](https://github.com/shradhadubey/pyspark-ai-pipeline-masterkit.git)
pip install -r requirements.txt
python src/main.py
```
#### The Repository Structure
```
├── .github/workflows/   # CI/CD for testing
├── src/
│   ├── extract.py       # API/S3 Extraction logic
│   ├── transform.py     # PySpark cleaning logic
│   └── ai_enricher.py   # AI-powered data labeling
├── tests/               # Pytest scripts
├── Dockerfile           # For containerized deployment
└── README.md            # The "Star Magnet"
```
