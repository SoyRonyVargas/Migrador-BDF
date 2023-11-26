from datetime import datetime
from rich import print
import pyodbc
import time
import uuid

# import pytz
# import os

# Configuración de la conexión a SQL Server (base de datos de origen)
# source_server = 'localhost\SQLEXPRESS'
# source_database = 'basededatosmoises'
# source_username = 'sa2'
# source_password = '123'
# source_driver = '{SQL Server}'

# # Cadena de conexión a la base de datos de origen

# # Configuración de la conexión a SQL Server (base de datos de destino)
# destination_server = 'localhost\SQLEXPRESS'
# destination_database = 'planeacion-2'
# destination_username = 'sa2'
# destination_password = '123'
# destination_driver = '{SQL Server}'

destination_cursor = None

source_cursor = None

# Cadena de conexión a la base de datos de destino

# Establecer conexión a SQL Server (base de datos de origen)

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

def insertarEnPersonas( row , id ):
    
    insert_query = "INSERT INTO Personas (Nombre, Apellidos, CURP, IdApplicationUser , Discriminator, Area) VALUES (?, ?, ?, ?, ? , ?)"
        
    current_datetime = datetime.now()

    curp = row[7] if len(row[7]) > 0 else ""

    insercion = {
        'Nombre': row[3],
        'Apellidos': f"{row[1]} {row[2]}",
        'CURP': curp,
        'IdApplicationUser': id,
        'Discriminator': "",
        'Area': ""
    }

    # Convertir el diccionario a una tupla de pares clave-valor
    values_tuple = tuple(insercion.values())

    # Ejecutar la consulta de inserción
    destination_cursor.execute(insert_query, values_tuple)

    destination_conn.commit()

    return

def eliminarTodo():

    print("Eliminando todo...")

    query = "DELETE FROM AspNetUsers; "
    query2 = "DELETE FROM Personas;"
    query5 = "DELETE FROM PlanesEstudioAsignaturas;"
    query4 = "DELETE FROM PlanesEstudios;"
    query3 = "DELETE FROM UnidadesTematicas;"
    query4 = "DELETE FROM Asignaturas;"

    destination_cursor.execute(query)
    destination_cursor.execute(query2)
    destination_cursor.execute(query5)
    destination_cursor.execute(query3)
    destination_cursor.execute(query4)

    destination_conn.commit()

    print("Eliminado completamente")

    return

def existeNombreUsuario(username):

      # Verificar si el UserName ya existe
    query_verificacion = "SELECT COUNT(*) FROM AspNetUsers WHERE UserName = ?"
    
    count = destination_cursor.execute(query_verificacion, username).fetchone()[0]

    return count > 0

def insertarEnAspUsers(row):

    insert_query = """INSERT INTO AspNetUsers 
        (   
            Id, 
            UserName, 
            NormalizedUserName,
            Email, 
            NormalizedEmail, 
            EmailConfirmed, 
            PasswordHash, 
            SecurityStamp,
            ConcurrencyStamp,
            PhoneNumber,
            PhoneNumberConfirmed,
            TwoFactorEnabled,
            LockoutEnd,
            LockoutEnabled,
            AccessFailedCount
        ) 
        VALUES (?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?)
    """

    nuevo_guid = uuid.uuid4()

    PhoneNumberConfirmed = 1 if len(row[23]) > 0 else 0
    
    EmailConfirmed = 1 if len(row[27]) > 0 else 0

    Email = row[27] if len(row[27]) > 0 else None

    userName = row[27]
    
    name = str(row[1]).lower()
    
    lastname = str(row[3]).lower()
    
    lastname = lastname.replace(" ", "_")
    
    name = name.replace(" ", "_")

    userName = f"{name}_{lastname}"
    
    # if Email == None:
    #     print("userName" , userName)

    existe = existeNombreUsuario(userName)

    if existe:
        print("userName",userName)
        return False
        # userName = f"{name}-{lastname}"

    if Email == None:
        if name == "":
            Email = f"{lastname}@upqroo.edu.mx"
        else:
            primeraLetra = name[0]
            Email = f"{primeraLetra}{lastname}@upqroo.edu.mx"
        print("name: ",name)
        print("lastname: ",lastname)
       
        

    # print("userName", userName)

    insercion = {
        'Id': nuevo_guid,
        'UserName': userName,
        'NormalizedUserName':  userName,
        'Email': Email,
        'NormalizedEmail': Email,
        'EmailConfirmed': EmailConfirmed,
        'PasswordHash': "",
        'SecurityStamp': "",
        'ConcurrencyStamp': "",
        'PhoneNumber': row[23],
        'PhoneNumberConfirmed': PhoneNumberConfirmed ,
        'LockoutEnd': 0,
        'LockoutEnabled': None,
        'AccessFailedCount': 0,
        'TwoFactorEnabled': 0,
    }

    # Convertir el diccionario a una tupla de pares clave-valor
    values_tuple = tuple(insercion.values())

    # Ejecutar la consulta de inserción
    destination_cursor.execute(insert_query, values_tuple)

    destination_conn.commit()

    return nuevo_guid

def insertarPersonas():
    
    try:

        eliminarTodo()

        print("Insertando en Persona")

        select_query = "SELECT * FROM DPERSO"
        
        source_cursor.execute(select_query)
        
        data_to_move = source_cursor.fetchall()
        
        i = 0
        
        for row in data_to_move:
            
            print("Insertando " , i)
            
            id = insertarEnAspUsers(row)
            
            if id is False:
                continue

            print("id", id)

            insertarEnPersonas(row , id)

            i+=1


        print("Terminado Persona")

    finally:
        print()

def insertarMaterias():
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
                'HorasPracticas': 0,
                'HorasTeoricas': 0,
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

            # print(inserted_id)
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

            # print(planes)
            
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
    

if __name__ == "__main__":
    
    #     source_server = 'localhost\SQLEXPRESS'
    # source_database = 'basededatosmoises'
    # source_username = 'sa2'
    # source_password = '123'
    # source_driver = '{SQL Server}'
    driver = '{SQL Server}'

    print("[bold red]Servidor Origen[/bold red]")

    source_server = input("Servidor: ")

    if source_server == "":
        source_server = 'localhost\SQLEXPRESS'

    source_database = input("Base de datos: ")
    source_username = input("Usuario: ")
    source_password = input("Contraseña: ")
    
    source_connection_string = f'DRIVER={driver};SERVER={source_server};DATABASE={source_database};UID={source_username};PWD={source_password}'

    print("[bold red]Servidor Destino[/bold red]")

    destination_server = input("Servidor: ")

    if destination_server == "":
        destination_server = 'localhost\SQLEXPRESS'
        
    destination_database = input("Base de datos: ")
    destination_username = input("Usuario: ")
    destination_password = input("Contraseña: ")

    destination_connection_string = f'DRIVER={driver};SERVER={destination_server};DATABASE={destination_database};UID={destination_username};PWD={destination_password}'

    source_conn = pyodbc.connect(source_connection_string)
    source_cursor = source_conn.cursor()

    # Establecer conexión a SQL Server (base de datos de destino)
    destination_conn = pyodbc.connect(destination_connection_string)
    
    destination_cursor = destination_conn.cursor()

    print("Insertando datos...")

    insertarPersonas()

    insertarMaterias()

    print("[bold red]Proceso terminado[/bold red]")