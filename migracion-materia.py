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

def insertarUnidadesTematicas( idAsignatura , objetivos ):
    
    insert_query = "INSERT INTO UnidadesTematicas (Nombre, Objetivos, HorasPracticas, HorasTeoricas, Version, Eliminado , IdAsignatura) VALUES (?, ?, ?, ?, ?, ?, ?)"

    for i in objetivos:
        
        current_datetime = datetime.now()

        insercion = {
            'Nombre': i,
            'Objetivos': "",
            'HorasPracticas': 0,
            'HorasTeoricas':0,
            'Version': current_datetime,
            'Eliminado': 0,
            'IdAsignatura': idAsignatura,
            'Version': current_datetime, 
            'Eliminado': 0,
        }
        
        values_tuple = tuple(insercion.values())
    
        destination_cursor.execute(insert_query, values_tuple)
    
    destination_conn.commit()
    
    return

def insertarPlanEstudioRelacion( idAsignatura , planes ):
    
    insert_query = "INSERT INTO PlanesEstudios (Nombre, TipoPlanEstudio, Version, Eliminado) VALUES (?, ?, ?, ?)"
    
    insert_query_relacion = "INSERT INTO PlanesEstudioAsignaturas (IdAsignatura, IdPlanEstudio, Version, Eliminado) VALUES (?, ?, ?, ?)"

    for i in planes:
        
        current_datetime = datetime.now()

        insercion = {
            'Nombre': i,
            'TipoPlanEstudio': 0,
            'Version': current_datetime,
            'Eliminado': 0,
        }
        
        values_tuple = tuple(insercion.values())
    
        destination_cursor.execute(insert_query, values_tuple)
        
        destination_conn.commit()

        inserted_id = destination_cursor.execute("SELECT @@IDENTITY").fetchone()[0]

        insercionRelacion = {
            'IdAsignatura': idAsignatura,
            'IdPlanEstudio': inserted_id,
            'Version': current_datetime,
            'Eliminado': 0,
        }
        
        values_tuple = tuple(insercionRelacion.values())

        destination_cursor.execute(insert_query_relacion, values_tuple)
        
        destination_conn.commit()

    return


# try:

#     select_query = "SELECT * FROM DPERSO"
    
#     source_cursor.execute(select_query)
    
#     data_to_move = source_cursor.fetchall()
    
#     for row in data_to_move:
        
#         insert_query = "INSERT INTO Personas (Nombre, Apellidos, CURP,Discriminator, Area) VALUES (?, ?, ?, ?, ?)"
        
#         current_datetime = datetime.now()

#         insercion = {
#             'Nombre': row[3],
#             'Apellidos': f"{row[1]} {row[2]}",
#             'CURP': row[7],
#             # 'IdApplicationUser': "SP1",
#             'Discriminator': "",
#             'Area': '',
#         }

#         # Convertir el diccionario a una tupla de pares clave-valor
#         values_tuple = tuple(insercion.values())

#         # Ejecutar la consulta de inserción
#         destination_cursor.execute(insert_query, values_tuple)

#     destination_conn.commit()

#     print("Terminado Persona")

# finally:
#     print()

try:
    # Leer datos de la primera tabla (DMATER)
    select_query = "SELECT * FROM DMATER"
    source_cursor.execute(select_query)
    data_to_move = source_cursor.fetchall()

    # Insertar datos en la segunda tabla (Asignaturas)
    insert_query = "INSERT INTO Asignaturas (NombrePE, Nombre, NombreTitulo, Objetivos, Competencias, HorasPracticas, HorasTeoricas, Version, Eliminado) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    
    index = 0

    for row in data_to_move:
        
        print(f"Insertando {index}")
        # print(row)

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

        # print(row[12])
        # print(row[13])
        # print(row[14])
        # print(row[15])
        # print(row[16])

        # print(insercion)

        # Convertir el diccionario a una tupla de pares clave-valor
        values_tuple = tuple(insercion.values())

        # Ejecutar la consulta de inserción
        destination_cursor.execute(insert_query, values_tuple)
        
        # obtener el ultimo id
        inserted_id = destination_cursor.execute("SELECT @@IDENTITY").fetchone()[0]

        unidades = list(filter(lambda x: "None" != x , [ row[12] , row[13] , row[14] , row[15] , row[16] ]))

        print(inserted_id)
        # print(unidades)

        insertarUnidadesTematicas( inserted_id , unidades )
        
        # print(row[57:80])
        planes = list(filter(lambda x: "None" != x , [ 
            row[57], 
            row[58], 
            row[59], 
            row[60], 
            row[61],
            row[62],
            row[63],
            row[64],
            row[65],
            row[66],
            row[67],
            row[68],
            row[69],
            row[70]
        ]))

        print(planes)
        
        # raise ValueError("Esto es un mensaje de error")

        #57
        insertarPlanEstudioRelacion( inserted_id , planes )
        
        # insertarPlanEstudioRelacion( inserted_id , unidades )

        index+=1

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