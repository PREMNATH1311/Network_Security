# 🛡️ Network Security — ML-Powered Threat Detection System

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100%2B-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-ML-F7931E?style=for-the-badge&logo=scikit-learn&logoColor=white)
![MLflow](https://img.shields.io/badge/MLflow-Tracking-0194E2?style=for-the-badge&logo=mlflow&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-Database-47A248?style=for-the-badge&logo=mongodb&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![DagsHub](https://img.shields.io/badge/DagsHub-Experiment%20Tracking-FF6F00?style=for-the-badge)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)

---

## 📌 Overview

**Network Security** is an end-to-end Machine Learning project that detects network intrusions and classifies malicious traffic using a production-ready ML pipeline. The system ingests raw network data from **MongoDB**, processes it through a structured training pipeline (ingestion → validation → transformation → model training), and exposes predictions via a **FastAPI** REST API. Experiment tracking is powered by **MLflow + DagsHub**, and the app is containerized with **Docker** for seamless deployment.

---

## ✨ Features

- 🔄 **Automated ML Pipeline** — Data ingestion, validation, transformation, and model training in one flow
- 🌐 **FastAPI REST API** — `/train` endpoint triggers retraining; `/predict` endpoint accepts CSV files and returns predictions
- 📊 **MLflow + DagsHub Integration** — Full experiment tracking, metric logging, and model versioning
- 🗄️ **MongoDB Backend** — Raw network data stored and fetched from MongoDB Atlas
- 🐳 **Dockerized** — Ready-to-deploy container image
- ⚙️ **CI/CD with GitHub Actions** — Automated workflows for testing and deployment
- 📁 **Structured Output** — Predictions saved to CSV and rendered as HTML tables

---

## 🗂️ Project Structure

```
Network_Security/
├── .github/
│   └── workflows/           # GitHub Actions CI/CD pipelines
├── Network_Data/            # Raw network dataset files
├── data_schema/             # Schema definitions for data validation
├── final_model/             # Saved preprocessor and model artifacts
│   ├── preprocessor.pkl
│   └── model.pkl
├── networksecurity/         # Core Python package
│   ├── components/          # Pipeline components
│   │   ├── data_ingestion.py
│   │   ├── data_validation.py
│   │   ├── data_transformation.py
│   │   └── model_trainer.py
│   ├── contants/            # Training pipeline constants
│   ├── entity/              # Config and artifact entities
│   ├── exception/           # Custom exception handling
│   ├── logging/             # Logging configuration
│   ├── pipeline/            # Training pipeline orchestration
│   └── utils/               # Utility functions & ML model estimator
├── notebooks/               # Exploratory Data Analysis (EDA)
├── prediction_output/       # CSV output of predictions
├── templates/               # Jinja2 HTML templates for result display
├── valid_data/              # Validated datasets
├── app.py                   # FastAPI application entry point
├── main.py                  # Standalone pipeline runner
├── push_data.py             # Script to push data to MongoDB
├── Dockerfile               # Docker image definition
├── requirements.txt         # Python dependencies
└── setup.py                 # Package setup
```

---

## 🧰 Tech Stack

| Category | Technology |
|---|---|
| Language | Python 3.8+ |
| ML Framework | scikit-learn |
| API Framework | FastAPI + Uvicorn |
| Database | MongoDB Atlas (via PyMongo) |
| Experiment Tracking | MLflow + DagsHub |
| Containerization | Docker |
| CI/CD | GitHub Actions |
| Data Processing | Pandas, NumPy |
| Serialization | Dill, PyYAML |

---

## ⚙️ Installation & Setup

### 1. Clone the Repository

```bash
git clone https://github.com/PremnathAnbu/Network_Security.git
cd Network_Security
```

### 2. Create a Virtual Environment

```bash
python -m venv venv
source venv/bin/activate       # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
pip install -e .
```

### 4. Set Up Environment Variables

Create a `.env` file in the root directory:

```env
MONGODB_URI=your_mongodb_connection_string
```

---

## 🚀 Usage

### Option A: Run the Full Training Pipeline Locally

```bash
python main.py
```

This will execute the pipeline in sequence:
1. **Data Ingestion** — Fetches data from MongoDB
2. **Data Validation** — Validates schema and data quality
3. **Data Transformation** — Preprocesses features
4. **Model Training** — Trains and evaluates the ML model

### Option B: Run the FastAPI Application

```bash
python app.py
```

The server starts at `http://0.0.0.0:8080`. Visit `http://localhost:8080/docs` for the interactive Swagger UI.

#### API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Redirects to Swagger docs |
| `GET` | `/train` | Triggers the full training pipeline |
| `POST` | `/predict` | Upload a CSV file to get predictions |

#### Example Predict Request (cURL)

```bash
curl -X POST "http://localhost:8080/predict" \
  -H "accept: application/json" \
  -F "file=@your_network_data.csv"
```

---

## 🐳 Docker Deployment

### Build the Image

```bash
docker build -t network-security-app .
```

### Run the Container

```bash
docker run -p 8080:8080 --env MONGODB_URI=your_mongo_uri network-security-app
```

### EC2 / Ubuntu Server Setup

```bash
sudo apt-get update -y && sudo apt-get upgrade -y
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker ubuntu
newgrp docker
```

---

## 📊 Experiment Tracking with MLflow & DagsHub

This project uses **DagsHub** as the remote MLflow tracking server.

```python
import mlflow
mlflow.set_tracking_uri("https://dagshub.com/<your-username>/Network_Security.mlflow")
```

Metrics, parameters, and model artifacts are automatically logged during each training run. You can view them on your DagsHub dashboard.

---

## 📦 Push Data to MongoDB

To load the network dataset into MongoDB:

```bash
python push_data.py
```

---

## 🔄 CI/CD Pipeline

GitHub Actions workflows (`.github/workflows/`) automate:

- Code linting and testing on push
- Docker image build and push to registry
- Deployment to cloud infrastructure

---

## 📁 Data

The project uses network traffic data containing features such as connection duration, protocol type, service, flag, and various byte-level statistics. Labels indicate whether a connection is normal or a specific type of attack.

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m 'Add your feature'`
4. Push to the branch: `git push origin feature/your-feature`
5. Open a Pull Request

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

## 👤 Author

**Premnath Anbu**
- GitHub: [@PremnathAnbu](https://github.com/PremnathAnbu)

---

> ⭐ If you found this project helpful, please give it a star!
