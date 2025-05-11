# Shipment Cost Prediction Project

This project is a machine learning application that predicts shipment costs based on various features.

## Setup

1. Clone the repository:
```bash
git clone https://github.com/prajapatibhavesh815/shipment_project.git
cd shipment_project
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the FastAPI application:
```bash
uvicorn app:app --reload
```

The application will be available at `http://localhost:8000`

## Project Structure

- `app.py`: Main FastAPI application
- `shipment/`: Main package directory
  - `component/`: ML pipeline components
  - `configuration/`: Configuration utilities
  - `entity/`: Data entities and models
  - `pipeline/`: Training pipeline
- `notebook/`: Jupyter notebooks for EDA and model development
- `static/`: Static files (CSS, JS)
- `templates/`: HTML templates