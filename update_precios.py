import pymysql
import pymysql.cursors
import csv


def get_conexion_sie_cursor():
    return pymysql.connect(
        host="db-sie-prod.c6r9vkodxz44.us-east-1.rds.amazonaws.com",
        user="EducarDB",
        passwd="Si3#l021q1#",
        cursorclass=pymysql.cursors.DictCursor,
        db="tienda_v2018")


connect = get_conexion_sie_cursor()


def leer_archivo():
    with open('precios.csv', mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            procesar_linea(row['codigo'], row[' PRECIO NUEVO '])
            line_count += 1
        print(f'Processed {line_count} lines.')


def procesar_linea(codigo, precio):
    precion_nat = float(precio) * 1000
    cursor = connect.cursor()
    cursor.execute("UPDATE ps_product SET price=%s WHERE reference=%s ", (precion_nat, codigo))
    connect.commit()
    print(f'Actualizar {codigo} a precio {precion_nat}')


if __name__ == '__main__':
    leer_archivo()
