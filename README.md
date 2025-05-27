# Cloud ETL Pipeline for API-Based Weather Data

A brief description of your project: This project implements an ETL (Extract, Transform, Load) pipeline to fetch weather data from an API, transform it, and store it in Azure Blob Storage. The pipeline is orchestrated using Azure Data Factory.

## Project Goals and Objectives
*   Extract current weather data from a public weather API.
*   Transform the raw JSON data into a structured tabular format.
*   Load the raw and transformed data into Azure Blob Storage.
*   Orchestrate the ETL process using Azure Data Factory.
*   Securely manage API keys and Azure credentials.
*   Maintain code in a Git repository hosted on GitHub.
*   (Add any other specific goals you had)

## Architecture Diagram
(We will create a simple text-based or link to an image diagram later)
[API] ---> [Python Script 1: get_weather_data.py (Extract)] ---> [Azure Blob Storage: raw-weather-data container] ---> [Python Script 2: transform_weather_data.py (Transform)] ---> [Azure Blob Storage: transformed-weather-data container]
^
|
[Azure Data Factory (Orchestration)]

## Technologies Used
*   **Cloud Provider:** Microsoft Azure
*   **Azure Services:**
    *   Azure Blob Storage (for data lake storage)
    *   Azure Data Factory (for pipeline orchestration)
    *   (Potentially Azure Key Vault for secrets later)
*   **Programming Language:** Python 3.13+
*   **Python Libraries:**
    *   `requests` (for API calls)
    *   `pandas` (for data manipulation)
    *   `azure-storage-blob` (Azure Blob Storage SDK)
    *   `python-dotenv` (for managing environment variables locally)
*   **API:** OpenWeatherMap API (or your chosen API)
*   **Version Control:** Git & GitHub
*   **Development Environment:** (e.g., VS Code, Python Virtual Environment)

## Setup Instructions
(Details to be added on how to run the project, configure Azure, etc.)

1.  **Prerequisites:**
    *   Python 3.13+
    *   Azure Account & Subscription
    *   Git
    *   (Any other tools)
2.  **Clone the repository:**
    ```bash
    git clone https://github.com/ayod3le/weather-data-pipeline-new.git
    cd weather-data-pipeline-new
    ```
3.  **Set up Python virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Linux/macOS
    # .\venv\Scripts\activate    # On Windows
    ```
4.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt 
    ```
    *(We will create a requirements.txt file soon)*
5.  **Configure Environment Variables:**
    *   Create a `.env` file in the project root.
    *   Add the following (replace placeholders with your actual values):
        ```
        WEATHER_API_KEY="your_openweathermap_api_key"
        AZURE_STORAGE_CONNECTION_STRING="your_azure_storage_connection_string"
        ```
6.  **Azure Setup:**
    *   Ensure you have an Azure Resource Group.
    *   Ensure you have an Azure Storage Account with two containers: `raw-weather-data` and `transformed-weather-data`.
    *   (ADF setup instructions will be added here later)
7.  **Running the scripts (locally before ADF orchestration):**
    ```bash
    # To fetch data from API and upload to Azure raw container
    python get_weather_data.py

    # To transform data from Azure raw container and upload to Azure transformed container
    python transform_weather_data.py
    ```

## Explanation of Scripts and ADF Pipeline Components
*   **`get_weather_data.py`:**
    *   (Brief explanation)
*   **`transform_weather_data.py`:**
    *   (Brief explanation)
*   **Azure Data Factory Pipeline:**
    *   (Explanation of ADF components once built)

## Challenges Faced and Lessons Learned
*   (To be filled in as you progress or reflect)

## Potential Future Enhancements
*   Implement more robust error handling and logging.
*   Use Azure Key Vault for managing secrets in ADF.
*   Schedule the ADF pipeline to run automatically.
*   Add data quality checks.
*   Store transformed data in a more queryable format (e.g., Parquet).
*   Set up monitoring and alerting for the pipeline.
*   (Add your own ideas)