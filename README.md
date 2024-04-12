# flask-api-read-write-asqldb

A Flask API for uploading historical data from CSV files to an Azure SQL database, with endpoints for data analysis queries. This project provides a simple yet powerful solution for managing and analyzing historical data stored in CSV files within an Azure SQL environment.

## Project Hierarchy

The project is structured as follows:
```
.
├── README.md
├── app.py
├── files_sources
│   ├── employees.csv
│   ├── departments.csv
│   ├── jobs.csv
├── app.py
└── requirements.txt
```
  
## Installation

1. Clone the repository:

   ```bash
   git clone <repository_url>
   cd api-read-write
   
2. Install dependecies using pip
   
   pip install -r requirements.txt

## Usage

Ensure that the CSV files are placed in the files_sources folder.
Run the Flask application:

python app.py
* The application will run on http://localhost:105/

## Endpoints
* POST /full_load: Upload CSV files from the files_sources folder to the Azure SQL database.
* GET /inv_employees: Retrieve the number of employees hired for each job and department in 2021 divided by quarter.
* GET /list_employees_hired_by_department: Retrieve the list of departments that hired more employees than the average in 2021.