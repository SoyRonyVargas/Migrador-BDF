from datetime import datetime
import pyodbc
import time

# Configuración de la conexión a SQL Server (base de datos de origen)
source_server = 'localhost\SQLEXPRESS'
source_database = 'SIE'
source_username = 'sa2'
source_password = '123'
source_driver = '{SQL Server}'

# Cadena de conexión a la base de datos de origen
source_connection_string = f'DRIVER={source_driver};SERVER={source_server};DATABASE={source_database};UID={source_username};PWD={source_password}'

# Configuración de la conexión a SQL Server (base de datos de destino)
destination_server = 'localhost\SQLEXPRESS'
destination_database = 'planeacion-2'
destination_username = 'sa2'
destination_password = '123'
destination_driver = '{SQL Server}'

# Cadena de conexión a la base de datos de destino
destination_connection_string = f'DRIVER={destination_driver};SERVER={destination_server};DATABASE={destination_database};UID={destination_username};PWD={destination_password}'

# Establecer conexión a SQL Server (base de datos de origen)
source_conn = pyodbc.connect(source_connection_string)
source_cursor = source_conn.cursor()

# Establecer conexión a SQL Server (base de datos de destino)
destination_conn = pyodbc.connect(destination_connection_string)
destination_cursor = destination_conn.cursor()

try:
    # Leer datos de la primera tabla (DMATER)
    select_query = "SELECT * FROM DMATER"
    source_cursor.execute(select_query)
    data_to_move = source_cursor.fetchall()

    # Insertar datos en la segunda tabla (Asignaturas)
    insert_query = "INSERT INTO Asignaturas (NombrePE, Nombre, NombreTitulo, Objetivos, Competencias, HorasPracticas, HorasTeoricas, Version, Eliminado) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

    for row in data_to_move:
        
        print(row)

        current_datetime = datetime.now()

        insercion = {
            'Nombre': row[1],
            'NombreTitulo': row[2],
            'NombrePE': row[0],
            'Objetivos': row[7],
            'Competencias': row[9],
            'HorasPracticas': 1,
            'HorasTeoricas': 1,
            'Version': current_datetime,  # Agregar la fecha y hora actual
            'Eliminado': 0,
        }

        print(insercion)

        # Convertir el diccionario a una tupla de pares clave-valor
        values_tuple = tuple(insercion.values())

        # Ejecutar la consulta de inserción
        destination_cursor.execute(insert_query, values_tuple)

    # Confirmar los cambios en la base de datos de destino
    destination_conn.commit()

finally:
    # Cerrar conexiones
    source_conn.close()
    destination_conn.close()

# Proceso largo aquí
time.sleep(10)

done = True
print("Proceso completado.")