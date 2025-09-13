# Google Books API Data Pipeline

This project is a data pipeline that fetches book data from the Google Books API, processes it, and stores it in a PostgreSQL database using Apache Airflow for orchestration. The pipeline is designed to automate the retrieval and storage of book information based on user-defined queries.

## Table of Contents
- [Overview](#overview)
- [Installation](#installation)
- [Setup](#setup)
- [Usage](#usage)
- [Technologies](#technologies)
- [Contact](#contact)

## Overview
The Google Books API Data Pipeline retrieves book data (e.g., title, authors, publication date) from the Google Books API, transforms it as needed, and stores it in a PostgreSQL database. The pipeline is orchestrated using Apache Airflow, with tasks defined in a DAG to handle data fetching and storage.

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/Maddy-Das/Google-API-Data-Books_pipeline.git
   cd Google-API-Data-Books_pipeline
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up PostgreSQL:
   - Ensure PostgreSQL is running locally or in a Docker container.
   - Create a database named `airflow` and a user with appropriate permissions.

4. (Optional) Use Docker to set up Airflow and PostgreSQL:
   ```bash
   docker-compose up -d
   ```

## Setup
1. **Google Books API Key**:
   - Obtain an API key from the [Google Cloud Console](https://console.cloud.google.com/).
   - Set the API key as an environment variable:
     ```bash
     export GOOGLE_API_KEY='your-api-key'
     ```

2. **Airflow Configuration**:
   - Initialize the Airflow database:
     ```bash
     airflow db init
     ```
   - Create an Airflow user:
     ```bash
     airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
     ```
   - Update the Airflow connection for PostgreSQL:
     - In the Airflow UI, go to **Admin > Connections**.
     - Create a connection named `books_connection` with:
       - Conn Type: `Postgres`
       - Host: `postgres` (or your PostgreSQL host)
       - Schema: `airflow`
       - Login: `airflow`
       - Password: `airflow`
       - Port: `5432`

3. **DAG Setup**:
   - Place the DAG file (`app.py`) in the Airflow `dags/` folder.
   - Ensure the DAG file is correctly named and located in the Airflow DAGs directory (e.g., `~/airflow/dags`).

## Usage
1. Start the Airflow webserver and scheduler:
   ```bash
   airflow webserver -p 8080
   airflow scheduler
   ```

2. Access the Airflow UI at `http://localhost:8080` and log in with the credentials created above.

3. Enable the `fetch_and_store_books_with_google_api` DAG in the Airflow UI.

4. Trigger the DAG manually or let it run on the defined schedule to fetch book data and store it in the PostgreSQL database.

5. Verify the data in PostgreSQL:
   ```bash
   psql -h postgres -U airflow -d airflow
   SELECT * FROM books;
   ```


## Technologies
- Python
- Apache Airflow
- PostgreSQL
- Google Books API
- Docker

## Contact
- GitHub: [Maddy-Das](https://github.com/Maddy-Das)
- LinkedIn: [maddydas07](https://www.linkedin.com/in/maddydas07/)
