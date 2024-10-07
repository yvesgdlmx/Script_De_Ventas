import csv
import re
import mysql.connector
from datetime import datetime

# Definir el archivo de entrada y salida
input_file = 'I:/VISION/A_INKREC.TXT'
output_file = 'C:/Users/Desarrollo/Desktop/a_INKREC_modificado.txt'

# Lista para almacenar los datos procesados
data = []
# Diccionario para almacenar los pacientes y sus fechas de envío
processed_patients = {}

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
        next(reader, None)  # Omitir la primera fila (encabezados)

        # Leer cada línea del archivo
        for row in reader:
            if row and row[0].strip():
                try:
                    row.pop(1)  # Omitir el segundo registro (55555)
                    ship_date_str = row[14]

                    # Verificar y convertir la fecha
                    if re.match(r'\d{2}/\d{2}/\d{2}$', ship_date_str):
                        # Formato corto MM/DD/YY
                        ship_date = datetime.strptime(ship_date_str, '%m/%d/%y')
                        ship_date = ship_date.replace(year=datetime.now().year)
                    elif re.match(r'\d{2}/\d{2}/\d{4}$', ship_date_str):
                        # Formato largo MM/DD/YYYY
                        ship_date = datetime.strptime(ship_date_str, '%m/%d/%Y')
                    else:
                        raise ValueError(f"Formato de fecha inválido: {ship_date_str}")

                    ship_date_str = ship_date.strftime('%Y-%m-%d')
                    row[14] = ship_date_str
                    patient_id = row[0]

                    # Verificar si el poder está dentro del rango permitido
                    poder = float(row[17])  # Poder está en la columna 18 (índice 17)
                    if -12 <= poder <= 12:
                        # Verificar si TAT es mayor o igual a 5
                        if int(row[15]) >= 5:  # TAT está en la columna 16 (índice 15)
                            row[6] = '0'  # LensPrice
                            row[9] = '0'  # CoatingsPrice
                            row[12] = '0'  # TintPrice

                    # Verificar si ya hemos procesado este paciente con una fecha diferente
                    if patient_id in processed_patients:
                        if ship_date_str not in processed_patients[patient_id]:
                            # Establecer los precios a 0 para el nuevo registro
                            row[6] = '0'  # LensPrice
                            row[9] = '0'  # CoatingsPrice
                            row[12] = '0'  # TintPrice
                            # También marcar el registro anterior para que los precios sean 0
                            for previous_row in data:
                                if previous_row[0] == patient_id and previous_row[14] in processed_patients[patient_id]:
                                    previous_row[6] = '0'  # LensPrice
                                    previous_row[9] = '0'  # CoatingsPrice
                                    previous_row[12] = '0'  # TintPrice
                    else:
                        processed_patients[patient_id] = set()

                    # Añadir la fecha de envío al set de fechas procesadas para este paciente
                    processed_patients[patient_id].add(ship_date_str)

                    # Añadir el nuevo registro para la inserción
                    data.append(row)

                except ValueError as e:
                    print(f"Formato de fecha inválido en la fila: {row}. Error: {e}")

    # Escribir los datos procesados en el archivo de salida
    with open(output_file, 'w', newline='') as new_file:
        writer = csv.writer(new_file, delimiter='\t')
        for row in data:
            writer.writerow(row)

    # Insertar los datos en MySQL
    try:
        for row in data:
            insert_command = """
            INSERT INTO Orders (Patient, LensStyle, LensMaterial, LensColor, LensOrdered, LensSupplied, LensPrice, ARCoating, Mirror, CoatingsPrice, Tint, TintOrdered, TintPrice, JobType, ShipDate, TAT, Redo, Poder)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_command, row)
        connection.commit()
    except mysql.connector.Error as err:
        print("Error al ejecutar el comando SQL:", err)
        connection.rollback()  # Revertir la transacción en caso de error
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