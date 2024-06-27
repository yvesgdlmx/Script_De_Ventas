import csv
import re
import mysql.connector
from datetime import datetime

# Definir el archivo de entrada y salida
input_file = 'I:/VISION/A_INKREC.TXT'
output_file = 'C:/Users/Desarrollo/Desktop/a_INKREC_modificado.txt'

# Lista para almacenar los datos procesados
data = []
patient_counts = {}

# Conectar a la base de datos
try:
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='luis',
        database='facturas_optimex'
    )
    cursor = connection.cursor()

    with open(input_file, 'r') as original_file:
        reader = csv.reader(original_file, delimiter='\t')
        # Omitir la primera fila (encabezados)
        next(reader, None)
        # Leer cada línea del archivo
        for row in reader:
            # Verificar si la fila contiene datos válidos (no está vacía)
            if row and row[0].strip():
                try:
                    # Omitir el segundo registro (55555)
                    row.pop(1)
                    # Verificar si la fecha es válida
                    ship_date_str = row[14]  # Asegurándonos de que estamos accediendo a la columna correcta
                    if re.match(r'\d{2}/\d{2}/\d{2}', ship_date_str):
                        ship_date = datetime.strptime(ship_date_str, '%d/%m/%y').strftime('%Y-%m-%d')
                        row[14] = ship_date
                    else:
                        raise ValueError(f"Formato de fecha inválido: {ship_date_str}")
                    # Verificar si ya existe un registro con el mismo mes y año
                    ship_date_month_year = datetime.strptime(ship_date, '%Y-%m-%d').strftime('%Y-%m')
                    cursor.execute("SELECT COUNT(*) FROM Orders WHERE DATE_FORMAT(ShipDate, '%Y-%m') = %s", (ship_date_month_year,))
                    count = cursor.fetchone()[0]
                    if count == 0:
                        patient = row[0]
                        if patient in patient_counts:
                            patient_counts[patient] += 1
                        else:
                            patient_counts[patient] = 1
                        data.append(row)
                except ValueError as e:
                    print(f"Formato de fecha inválido en la fila: {row}. Error: {e}")

    # Filtrar los registros que tienen duplicados en el campo Patient
    filtered_data = [row for row in data if patient_counts[row[0]] == 1]

    # Escribir los datos procesados en el archivo de salida temporal
    with open(output_file, 'w', newline='') as new_file:
        writer = csv.writer(new_file, delimiter='\t')
        for row in filtered_data:
            writer.writerow(row)

    # Cargar los datos en MySQL
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='luis',
            database='facturas_optimex',
            allow_local_infile=True
        )
        cursor = connection.cursor()
        # Usar LOAD DATA LOCAL INFILE para cargar los datos en la tabla Orders
        sql_command = f"""
        LOAD DATA LOCAL INFILE '{output_file}'
        INTO TABLE Orders
        FIELDS TERMINATED BY '\\t'
        LINES TERMINATED BY '\\n'
        (Patient, LensStyle, LensMaterial, LensColor, LensOrdered, LensSupplied, LensPrice, ARCoating, Mirror, CoatingsPrice, Tint, TintOrdered, TintPrice, JobType, ShipDate, TAT, Redo, Poder)
        SET ShipDate = STR_TO_DATE(ShipDate, '%Y-%m-%d')
        """
        cursor.execute(sql_command)
        connection.commit()

        # Actualizar los precios a 0 donde TAT es mayor que 5.0 y Poder no cumple las condiciones
        update_command = """
        UPDATE Orders
        SET LensPrice = 0, CoatingsPrice = 0, TintPrice = 0
        WHERE TAT > 5.0 AND (Poder < 12 AND Poder > -12)
        """
        cursor.execute(update_command)
        connection.commit()
    except mysql.connector.Error as err:
        print("Error al ejecutar el comando SQL:", err)
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
    print("Carga de datos completada.")
except mysql.connector.Error as err:
    print("Error al conectar a la base de datos:", err)
finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()