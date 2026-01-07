#!/bin/bash
set -e

echo "Setting up Apache Airflow environment with Python 3.13.8 using uv..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "Error: uv is not installed. Please install uv first."
    echo "Visit: https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
fi

# Create virtual environment with Python 3.13.8
if [ -d ".venv" ]; then
    echo "Virtual environment already exists, skipping creation..."
else
    echo "Creating virtual environment with Python 3.13.8..."
    uv venv --python 3.13.8 .venv
fi

# install required apt packages for mysqlclient
echo "Installing required system packages..."
sudo apt update --fix-missing
sudo apt install -y \
    python3-dev \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config


# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Install Apache Airflow using uv
echo "Installing Apache Airflow..."
AIRFLOW_VERSION=3.1.5
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

uv pip install "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-mysql apache-airflow-providers-elasticsearch --constraint "${CONSTRAINT_URL}"

# Set Airflow home directory
export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
# mysql connection string for airflow metadata
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN='mysql+mysqldb://appuser:apppassword@127.0.0.1:3306/airflowdb'


# Initialize Airflow database
echo ""
echo "Initializing Airflow database..."
airflow db migrate

# Note: In Airflow 3.x, user creation is handled differently
# Use 'airflow standalone' which will create a default admin user automatically
# Or manage users through the web UI after starting the server

echo ""
echo "Setup complete!"
echo ""
echo "Airflow home: ${AIRFLOW_HOME}"
echo ""
echo "Starting Airflow standalone server..."
echo "This will create an admin user automatically and display the credentials."
echo ""
airflow standalone
