# Bangkok Air Quality MLOps Project - Airflow DAGs

## Project Overview
This repository contains the Apache Airflow DAG configurations and related code for the Bangkok Air Quality MLOps project. The project focuses on processing, analyzing, and modeling air quality data for Bangkok using a robust MLOps pipeline deployed on AWS.

## Architecture
The project is deployed on AWS using Apache Airflow for orchestration of the ML pipeline. 

### Key Components
- Apache Airflow: Orchestrates the entire ML pipeline
- AWS Infrastructure: Hosts the Airflow deployment and related services
- ML Pipeline: Processes and analyzes Bangkok air quality data

## Prerequisites
- Apache Airflow 2.x
- Python 3.8+
- AWS Account with appropriate permissions
- Required Python packages (specified in `requirements.txt`)

## Setup Instructions

### 1. Environment Setup
```bash
# Create a virtual environment
python -m venv venv

# Activate virtual environment
# For Windows
.\venv\Scripts\activate
# For Unix/MacOS
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. AWS Configuration
- Ensure AWS credentials are properly configured
- Set up necessary AWS services (EC2, S3, etc.)
- Configure security groups and IAM roles

### 3. Airflow Configuration
- Set up Airflow connections for AWS services
- Configure Airflow variables as needed
- Place DAG files in the appropriate Airflow DAGs directory

## Project Structure
```
baq-airflow/
├── dags/                  # Airflow DAG definitions
├── scripts/              # Helper scripts
├── config/              # Configuration files
├── requirements.txt     # Python dependencies
└── README.md           # Project documentation
```

## Usage
1. Ensure all prerequisites are met
2. Configure AWS credentials
3. Start Airflow services
4. Access Airflow web UI to monitor DAGs

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License
[Specify your license here]

## Contact
[Add contact information if needed] 