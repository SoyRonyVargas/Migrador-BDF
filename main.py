import itertools
import threading
import time
import os
import pyodbc
import sys
from dbfread import DBF

# Configuración de la conexión a SQL Server
server = 'localhost\SQLEXPRESS'
database = 'SIE'
username = 'sa2'
password = '123'
driver = '{SQL Server}'
#Puede variar según tu configuración

# Ruta a la carpeta que contiene los archivos DBF
ruta_carpeta_dbf = './datos'

# Cadena de conexión a SQL Server
connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

done = False
#here is the animation
def animate():
    for c in itertools.cycle(['|', '/', '-', '\\']):
        if done:
            break
        sys.stdout.write('\rCargando ' + c)
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\rProceso Terminado     ')

hiloCerrado = False

def animate2():
    for c in itertools.cycle(['|', '/', '-', '\\']):
        if hiloCerrado:
            break
        sys.stdout.write('\rInsertando datos a la tabla... \n' + c)
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\rDatos insertados \n')

t = threading.Thread(target=animate)

t.start()
# Función para crear una tabla en SQL Server
def crear_tabla_sql_server(cursor, nombre_tabla, campos):
    
    # Crear la instrucción CREATE TABLE
    # create_table_query = f"CREATE TABLE {nombre_tabla} ({', '.join(campos)})"

    cursor.execute(f"IF OBJECT_ID('{nombre_tabla}', 'U') IS NULL CREATE TABLE {nombre_tabla} ({', '.join(campos)})")

# Función para insertar datos en una tabla de SQL Server
def insertar_datos_sql_server(cursor, nombre_tabla, nombres_columnas, datos):
    # Crear la instrucción INSERT INTO
    insert_query = f"INSERT INTO {nombre_tabla} ({', '.join(nombres_columnas)}) VALUES ({', '.join(['?' for _ in nombres_columnas])})"
    
    # Ejecutar la instrucción para cada fila de datos
    for fila in datos:
        # Convertir todos los datos a cadenas antes de la inserción
        datos_string = [str(valor) for valor in fila]
        cursor.execute(insert_query, datos_string)

# Establecer conexión a SQL Server
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Iterar sobre los archivos en la carpeta
for archivo_dbf in os.listdir(ruta_carpeta_dbf):
    if archivo_dbf.endswith('.DBF'):

        ruta_completa = os.path.join(ruta_carpeta_dbf, archivo_dbf)
        
        nombre_tabla = os.path.splitext(archivo_dbf)[0]

        print("\nInsertando tabla: " , nombre_tabla , "\n")

        hiloCerrado = True

        j = threading.Thread(target=animate2)
        
        j.start()

        # Leer el archivo DBF con codificación UTF-8
        tabla_dbf = DBF(ruta_completa, encoding='latin-1')
        
        # Obtener los nombres de las columnas
        nombres_columnas = tabla_dbf.field_names
        
        # Crear la tabla en SQL Server
        crear_tabla_sql_server(cursor, nombre_tabla, [f'{campo} NVARCHAR(MAX)' for campo in nombres_columnas])
        
        # Insertar datos en la tabla
        datos_a_insertar = [tuple(registro.values()) for registro in tabla_dbf]

        insertar_datos_sql_server(cursor, nombre_tabla, nombres_columnas, datos_a_insertar)

        hiloCerrado = False

# Confirmar los cambios y cerrar la conexión
conn.commit()
conn.close()

#long process here
time.sleep(10)

done = True

print("Proceso completado.")