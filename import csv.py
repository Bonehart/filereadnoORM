import csv
import numpy as np
import json

# Function to insert data from a CSV file into a database
def insertdataCSV(tableName, filename, fields, batch_size, year, env):
    """
    Inserts data from a CSV file into a specified database table in batches.

    Args:
    tableName (str): Name of the table to insert the data into.
    filename (str): Path to the CSV file.
    fields (list): List of field names with data types.
    batch_size (int): Number of rows to insert per batch.
    year (int): Reporting year.
    env (str): Environment (e.g., dev, prod) for database schema.

    Returns:
    None
    """
    # Extract only field names (ignoring data types) for SQL columns
    field_cov = [field.split()[0] for field in fields]
    cols = "(" + ', '.join(field_cov) + ")"
    
    query = ""

    # Open the CSV file
    with open(filename, encoding='utf8') as f:
        csvreader = csv.reader(f)
        
        # Skip and clean the header row
        header_in = next(csvreader)
        header = [col.replace("\ufeff", '').upper() for col in header_in]

        print(f"Processing file: {filename}")
        
        # Get the index of fields in the CSV header
        for_insert = [header.index(field.upper()) for field in field_cov]
        
        # Iterate through each row in the CSV file
        for row_num, line in enumerate(csvreader):
            # Construct the initial part of the SQL insert query
            if row_num == 0:
                insert_into = f"INSERT INTO {env}.{tableName} "
            else:
                insert_into = f"; INSERT INTO {env}.{tableName} "
            
            # Prepare values for SQL insert
            value_list = []
            for i in range(len(fields)):
                field_type = fields[i].split()[1]
                value = line[for_insert[i]]
                
                # Handle integer fields, replacing 'nan' with 0
                if field_type == "INTEGER":
                    if value == 'nan':
                        value = 0
                    value_list.append(f"{value}")
                else:
                    value_list.append(f"'{value}'")
            
            # Format the values into SQL syntax
            values_output = "(" + ", ".join(value_list) + ")"
            query += insert_into + cols + " VALUES " + values_output + ";"
            
            # Execute query after reaching the batch size
            if row_num % batch_size == 0:
                try:
                    cursor.execute(query)
                    query = ""
                except Exception as e:
                    print(f"Error processing file {filename}")
                    print(e)

        # Execute any remaining queries
        if row_num % batch_size != 0:
            try:
                cursor.execute(query)
            except Exception as e:
                print(e)

        print(f"Data successfully added to {tableName}")


# Function to check if the file columns match the metadata columns
def checkFile(fileType, file_columns, label):
    """
    Validates the columns in the file against the expected metadata columns.

    Args:
    fileType (DataFrame): The file to validate.
    file_columns (list): List of column names in the file.
    label (str): The label or name of the file being checked.

    Returns:
    None
    """
    INVALID_LIST= [ 'note', 'NOTE', 'NUMBER', 'REPORTING_YEAR']
    file_columns = fileType.columns.tolist()

    with open("err.txt", 'a') as f:
        f.write(f"### CHECKING FILE {label} ###\n")
        
        column_errors = [col for col in file_columns if col not in meta_columns]
        
        if column_errors:
            print(f"There are errors in the file {label}. Columns not found in the config: {column_errors}")
            f.write(f"There are errors in the file {label}. Columns not found in the config: {column_errors}\n")
            f.write(f"Correct columns are: {meta_columns}\n")
            print("Manually change files and re-run.")
        else:
            print(f"No errors found in the file {label}.")

    print("")


# Function to check if the variables in the file are valid based on a config file
def checkVars(name, file):
    """
    Validates the values in each column of the file against the expected values from the config file.

    Args:
    name (str): The name of the file being validated.
    file (DataFrame): The DataFrame containing the file data.

    Returns:
    webin (list), errors (list): Webin dictionary of errors and list of error descriptions.
    """
    print(f"### CHECKING FILE {name} ###")
    
    path = f"./CONFIGS/{name}_config.json"
    with open(path) as f:
        valid_values = json.load(f)
    
    file_columns = file.columns.tolist()
    webin = []
    errors = []

    # Loop through each column and check for invalid values
    for col in (c for c in file_columns if c not in INVALID_list):
        try:
            valid_list = valid_values[col]
            file_values = file[col].unique()

            # Ensure all values are strings for comparison
            file_values_char = np.array([str(x) for x in file_values])
            valid_list_char = np.array([str(x) for x in valid_list])

            # Find any invalid values not in the config
            invalid_values = [val for val in file_values_char if val not in valid_list_char and val not in ("NS", "nan", "NP")]
            
            if invalid_values:
                print(f"Invalid values in column {col}: {invalid_values}")
                errors.append(invalid_values)
                
                # Log the errors
                for invalid in invalid_values:
                    webin.append({
                        'File': name,
                        'Column': col,
                        'Value': invalid
                    })
            else:
                print(f"No errors found in column {col}.")
        
        except Exception as e:
            print(f"Exception occurred while checking column {col}: {e}")

    print("CHECK COMPLETED")
    return webin, errors

print("Code loaded and ready to run!")
