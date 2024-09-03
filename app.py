from flask import Flask, request, render_template, jsonify
import pyodbc
import pandas as pd
import os

driver = '{ODBC Driver 17 for SQL Server}'
server = 'asever-demo-dev.database.windows.net'
database = 'asql-demo-dev'
username = 'adminuser'
password = 'V@lenchange2024*'

app = Flask(__name__)

#Connection Azure DB
connection_string = f'Driver={driver};Server={server},1433;Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

#endpoints POST full load to Azure SQL DB
@app.route('/full_load', methods=['POST'])
def upload_csv():
    folder_path = os.path.join(os.getcwd(), 'files_source')

    # Check if the folder exists
    if not os.path.exists(folder_path):
        return jsonify({'error': 'CSV folder not found'})

    # Monitoring files
    success_files = []
    error_files = []
    invalid_rows = []

    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            table_name = os.path.splitext(filename)[0]

            # Validate table name
            if table_name not in ['departments', 'hired_employees', 'jobs']:
                error_files.append({'filename': filename, 'error': 'Invalid table'})
                continue

            try:
                # Read the CSV file into a DataFrame
                if table_name == 'departments':
                    colnames = ['id', 'department']
                elif table_name == 'hired_employees':
                    colnames = ['id', 'name', 'datetime', 'department_id', 'job_id']
                elif table_name == 'jobs':
                    colnames = ['id', 'job']

                df = pd.read_csv(file_path, delimiter=",", encoding="utf8", names=colnames, header=None)

                # Process each row
                for index, row in df.iterrows():
                    try:
                        if table_name == 'departments':
                            if validate_department_row(row):
                                cursor.execute(f"INSERT INTO dbo.{table_name} (id, department) VALUES (?, ?)", row['id'], row['department'])
                            else:
                                invalid_rows.append({'filename': filename, 'row': row.tolist(), 'error': 'Invalid department data'})
                        
                        elif table_name == 'hired_employees':
                            if validate_hired_employees_row(row):
                                cursor.execute(f"INSERT INTO dbo.{table_name} (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)",
                                                row['id'], row['name'], row['datetime'], row['department_id'], row['job_id'])
                            else:
                                invalid_rows.append({'filename': filename, 'row': row.tolist(), 'error': 'Invalid hired employee data'})
                        
                        elif table_name == 'jobs':
                            if validate_jobs_row(row):
                                cursor.execute(f"INSERT INTO dbo.{table_name} (id, job) VALUES (?, ?)", row['id'], row['job'])
                            else:
                                invalid_rows.append({'filename': filename, 'row': row.tolist(), 'error': 'Invalid job data'})
                        
                    except Exception as e:
                        # Log the invalid row and continue with the next row
                        invalid_rows.append({'filename': filename, 'row': row.tolist(), 'error': str(e)})

                # Commit the transaction only if no rows caused an error
                conn.commit()
                success_files.append(filename)

            except Exception as e:
                # Rollback the transaction in case of a file-level error
                conn.rollback()
                error_files.append({'filename': filename, 'error': str(e)})

    # Prepare response
    response = {'success_files': success_files, 'error_files': error_files, 'invalid_rows': invalid_rows}
    return jsonify(response)


def validate_department_row(row):
    try:
        int(row['id'])
        str(row['department'])
        return True
    except (ValueError, TypeError):
        return False

def validate_hired_employees_row(row):
    try:
        id_value = int(row['id'])
        name_value = str(row['name'])
        datetime_value = pd.to_datetime(row['datetime'], format='%Y-%m-%dT%H:%M:%SZ', errors='raise')
        department_id_value = int(row['department_id'])
        job_id_value = int(row['job_id'])
    
        if pd.isna(datetime_value) or not name_value.strip():
            return False
        
        return True
    except (ValueError, TypeError):
        return False

def validate_jobs_row(row):
    try:
        int(row['id'])
        str(row['job'])
        return True
    except (ValueError, TypeError):
        return False

########################################################

#endpoints GET employees inventory
@app.route('/inv_employees', methods=['GET'])
def employees_by_quarter():
    try:
        # Query to fetch data from hired_employees table for 2021
        query = """
        SELECT d.department AS Department, j.job AS Job,
        SUM(CASE WHEN DATEPART(QUARTER, emp.datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
        SUM(CASE WHEN DATEPART(QUARTER, emp.datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
        SUM(CASE WHEN DATEPART(QUARTER, emp.datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
        SUM(CASE WHEN DATEPART(QUARTER, emp.datetime) = 4 THEN 1 ELSE 0 END) AS Q4
        FROM hired_employees emp
        LEFT JOIN departments d ON emp.department_id = d.id
        LEFT JOIN jobs j ON emp.job_id = j.id
        WHERE  YEAR(emp.datetime) = 2021
        GROUP BY d.department, j.job
        ORDER BY d.department, j.job;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        # Convert rows to a list of dictionaries
        columns = [column[0] for column in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]

        return render_template('inv_employees.html', rows=result)
    except Exception as e:
        return jsonify({'error': str(e)})

#endpoints GET employees hired more than the avg
@app.route('/list_employees_hired_by_department', methods=['GET'])
def departments_with_more_employees():
    try:
        # Query to get the departments that hired more employees than the average in 2021
        dept_query = """
            WITH hired_2021 AS (
            SELECT COUNT(*) AS num_employees
            FROM hired_employees
            WHERE YEAR([datetime]) =2021
            GROUP BY department_id
            )

            SELECT d.id , d.department, COUNT(*) AS hired
            FROM hired_employees he
            JOIN departments d ON he.department_id = d.id
            GROUP BY d.id, d.department
            HAVING COUNT(*) > (
                SELECT AVG(num_employees)
                FROM hired_2021)
        """
        cursor.execute(dept_query)
        dept_results = cursor.fetchall()

        # Convert rows to a list of dictionaries
        columns = [column[0] for column in cursor.description]
        result = [dict(zip(columns, row)) for row in dept_results]

        return render_template('employees_hired_by_department.html', rows=result)
    except Exception as e:
        return jsonify({'error': str(e)})
    
app.run(host='0.0.0.0', port=105)