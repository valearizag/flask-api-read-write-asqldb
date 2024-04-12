from flask import Flask, request, jsonify
import pyodbc
import pandas as pd
import os

driver = '{ODBC Driver 17 for SQL Server}'
server = 'assql-demo-dev.database.windows.net'
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

  for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            table_name = os.path.splitext(filename)[0]
            
            if table_name == 'departments':
                try:
                    # Read the CSV file into a DataFrame and assing column names
                    colnames=['id', 'department'] 
                    df = pd.read_csv(file_path,delimiter=",", encoding="utf8", names=colnames, header=None)
                    
                    # Write the DataFrame to the corresponding table in Azure SQL
                    for index, row in df.iterrows():
                        cursor.execute(f"INSERT INTO dbo.{table_name} (id,department) values(?,?)", row['id'], row['department'])
                    conn.commit()
                    success_files.append(filename)

                except Exception as e:
                    error_files.append({'filename': table_name, 'error': str(e)})

            elif table_name == 'hired_employees':
                try:
                    # Read the CSV file into a DataFrame and assing column names
                    colnames=['id','name','datetime','department_id','job_id'] 
                    df = pd.read_csv(file_path,delimiter=",", encoding="utf8", names=colnames, header=None)
                    df = df.fillna(0)
                    # Write the DataFrame to the corresponding table in Azure SQL
                    for index, row in df.iterrows():
                        cursor.execute(f"INSERT INTO dbo.{table_name} (id,name,datetime,department_id,job_id) values(?,?,?,?,?)", row['id'], row['name'], row['datetime'],row['department_id'],row['job_id'])
                    conn.commit()
                    success_files.append(filename)

                except Exception as e:
                    error_files.append({'filename': table_name, 'error': str(e)})
            
            elif table_name == 'jobs':
                try:
                    # Read the CSV file into a DataFrame and assing column names
                    colnames=['id','job'] 
                    df = pd.read_csv(file_path,delimiter=",", encoding="utf8", names=colnames, header=None)
                    
                    # Write the DataFrame to the corresponding table in Azure SQL
                    for index, row in df.iterrows():
                        cursor.execute(f"INSERT INTO dbo.{table_name} (id,job) values(?,?)", row['id'], row['job'] )
                    conn.commit()
                    success_files.append(filename)

                except Exception as e:
                    error_files.append({'filename': table_name, 'error': str(e)})
            

    # Prepare response
  response = {'success_files': success_files, 'error_files': error_files}

  return jsonify(response)

#endpoints GET employees inventory
@app.route('/inv_employees', methods=['GET'])
def employees_by_quarter():
    try:
        # Query to fetch data from hired_employees table for 2021
        query = """
        SELECT d.department, j.job,
        SUM(CASE WHEN SUBSTRING(emp.datetime, 6, 2) BETWEEN '01' AND '03' THEN 1 ELSE 0 END) AS Q1,
        SUM(CASE WHEN SUBSTRING(emp.datetime, 6, 2) BETWEEN '04' AND '06' THEN 1 ELSE 0 END) AS Q2,
        SUM(CASE WHEN SUBSTRING(emp.datetime, 6, 2) BETWEEN '07' AND '09' THEN 1 ELSE 0 END) AS Q3,
        SUM(CASE WHEN SUBSTRING(emp.datetime, 6, 2) BETWEEN '10' AND '12' THEN 1 ELSE 0 END) AS Q4
        FROM hired_employees emp
        LEFT JOIN departments d ON emp.department_id = d.id
        LEFT JOIN jobs j ON emp.job_id = j.id
        WHERE SUBSTRING(emp.datetime, 1, 4) = '2021'
        GROUP BY d.department, j.job
        ORDER BY d.department, j.job
        """
        cursor.execute(query)
        
        # Fetch all rows from the cursor
        rows = cursor.fetchall()

        # Convert rows to a list of dictionaries
        result = []
        for row in rows:
            result.append({
                'department': row.department,
                'job': row.job,
                'Q1': row.Q1,
                'Q2': row.Q2,
                'Q3': row.Q3,
                'Q4': row.Q4
            })

        return jsonify(result)
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
               WHERE SUBSTRING(datetime, 1, 4) = '2021'
               GROUP BY department_id
            )

            SELECT d.id , d.department, COUNT(*) AS hired
            FROM hired_employees he
            JOIN departments d ON he.department_id = d.id
            GROUP BY d.id, d.department
            HAVING COUNT(*) > (
                SELECT AVG(num_employees)
                FROM hired_2021
            )
        """
        cursor.execute(dept_query)
        dept_results = cursor.fetchall()

        #output
        result = []
        for row in dept_results:
            result.append({
                'id': row.id,
                'department': row.department,
                'hired': row.hired
            })

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})
    
app.run(host='0.0.0.0', port=105)