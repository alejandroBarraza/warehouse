from Registro import *
from Producto import *
import sqlite3
from sqlite3 import Error
import pika
from json import loads
import threading




# connection method to database 
def create_connection(db_file):
   
    conn = None
    try:
        conn = sqlite3.connect(db_file,check_same_thread=False)
    except Error as e:
        print(e)
    return conn


def rabbitmq(conn):
    
    def insert_db_rabitmq(ch,method,properties,body):
    
        body_parse = loads(body)

        nombre_material = body_parse["nombre_material"]
        cantidad_material = body_parse["cantidad_material"]
        fecha = body_parse["fecha"] 

        cur = conn.cursor()
        cur.execute("""SELECT id FROM PRODUCTO WHERE nombre = '%s'""" % nombre_material)

        id_producto = "NULL"

        try:
            id_producto = cur.fetchone()[0]
        except:
            id_producto = "NULL"
            pass

        if id_producto == "NULL":
                        
            cur.execute("""INSERT INTO PRODUCTO( nombre, cantidad) 
                    VALUES (?,?)""",( nombre_material, cantidad_material))

            conn.commit()

            cur.execute("""SELECT id FROM PRODUCTO WHERE nombre = '%s'""" % nombre_material)

            id_producto = cur.fetchone()[0]
        
        cur.execute("""INSERT INTO Registro( id_producto, fecha, cantidad) 
                    VALUES (?,?,?)""",( id_producto, fecha, cantidad_material))

        conn.commit()
    # ==================================================================
    # rabbit connection.




    print ("iniciando rabbit")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='mantencion', exchange_type='direct')

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='mantencion', queue=queue_name, routing_key="sBodega")

    channel.basic_consume(queue=queue_name, on_message_callback= insert_db_rabitmq, auto_ack=True)

    channel.start_consuming()





#select all workers
def show_all_registro(conn):

    cur = conn.cursor()
    cur.execute("SELECT * FROM Registro")
    rows = cur.fetchall()
    for row in rows:
        print(row)

def show_all_producto(conn):

    cur = conn.cursor()
    cur.execute("SELECT * FROM Producto")
    rows = cur.fetchall()
    for row in rows:
        print(row)

def insertar_registro(conn):
    
    try:
        nombre = input(" Ingrese nombre del producto a registrar: ")

        cur = conn.cursor()
        cur.execute("""SELECT id FROM Producto WHERE nombre = '%s'""" % nombre)
        conn.commit()

        id_producto = cur.fetchone()[0]


        if id_producto == "NULL":
            print ("No existe producto con ese nombre")
            return 

        cantidad = input("ingrese cantidad usada: ")

        try:
            cantidad = float(cantidad)
        except:
            print ("Ingrese un numero valido en cantidad")
            return 

        cur.execute("""SELECT cantidad FROM Producto WHERE nombre = '%s'""" % nombre)
        conn.commit()

        cantidadAntiguo = cur.fetchone()[0]
        
        cantidadNuevo = float(cantidadAntiguo) - cantidad
        
        if cantidadNuevo < 0:
            print ("Supera el maximo de cantidad del producto")
            return 

        # print("Entrando")


        cur.execute("UPDATE Producto SET cantidad = ? WHERE id = ?", (cantidadNuevo,id_producto))
        conn.commit()

        # print("Update")

        fecha = input("ingrese fecha: ")

        registro = Registro(id_producto,fecha,cantidad)

        cur.execute("""INSERT INTO Registro( id_producto,fecha,cantidad) 
                   VALUES (?,?,?)""",( registro.id_producto, registro.fecha,registro.cantidad))

        conn.commit()

        # print("Registro Insertado")
        return 

    except:
        print ("Error")
        return 

# insert a Product to Producto Table.
def insert_producto(conn):
    nombre = input("ingrese nombre del producto: ")
    cantidad = input('Ingrese cantidad del producto: ')
    
    p1= Producto( nombre, cantidad)

    cur = conn.cursor()

    cur.execute(""" SELECT nombre FROM Producto WHERE nombre = '%s' """ % nombre) 

    try:
        nombreP = cur.fetchone()[0]
        print("Ya existe un producto con ese nombre")
        return
    except:
        pass

    cur.execute("""INSERT INTO Producto( nombre, cantidad) 
                   VALUES (?,?)""",( p1.nombre, p1.cantidad))
    conn.commit()
    print("El producto fue insertado.")


def menuImpreso():
    print("[1]. Ingresar un producto")
    print("[2]. Ingresar un registro")
    print("[3]. Mostrar lista productos")
    print("[4]. Mostrar lista registros")
    print("[0]. Salir del Programa")

def menu(conn):
    menuImpreso()
    option = int(input("ingresar opcion: "))
    while option != 0:
        if option == 1:
            with conn:
                insert_producto(conn) 
            pass
        elif option == 2:
            with conn:
                insertar_registro(conn)
            pass
        elif option == 3:
            with conn:
                print("1. info correspondiente a la tabla productos:")
                show_all_producto(conn)
            pass
        elif option == 4:
            with conn:
                print("4. info correspondiente a la tabla registros:")
                show_all_registro(conn)       
        else:
            print("selecione un numero disponible en el menu")
        print()
        menuImpreso()
        option = int(input("ingresar opcion: "))
    print("gracias por usar este programa")
   

if __name__ == '__main__':
    database = r"C:\Users\ale\Desktop\bodega\BodegaDB.db"
    conn = create_connection(database)

    t = threading.Thread(target=menu, args=[conn])
    t.start()
    rabbitmq(conn)
    
    