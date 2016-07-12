#########################################################################
#
# aDB
#
# Copyright (c) 2016 Melchor Monleon <mel@ctav.es>
#
#########################################################################
__author__ = "Melchor Monleon"
__date__ = "$Jun 17, 2016$"
#########################################################################

import mysql.connector
from mysql.connector import errorcode

# ~ import conexionDB

# ~ host = "localhost"
# ~ user = "domolibre"
# ~ passwd = "domolibre"
# ~ db = "domolibre"

# tabla10 = 'calder10'
# tabla60 = 'calder60'
tabla10 = 'lagarto10'
tabla60 = 'lagarto60'
tablaLog = 'logLagarto'
tablaActuadores = 'actuadores'


def addColumn(columna, tabla, tipoVar, endpName, cnx):
    """
    Anyade la columna si no existe en la base de datos
    tipoVar es la cadena en lenguaje sql con la definicion del tipo de variable sql
    """
    sql = "SHOW COLUMNS FROM domolibre." + tabla + " LIKE '" + columna + "';"
    # ~ print sql ,

    try:
        cursor = cnx.cursor()
        rows_affected = cursor.execute(sql)
        salida = cursor.fetchall() 
        if len(salida) != 1:
            if "Binary" in endpName or "Motion" in endpName:
                sql = "ALTER TABLE domolibre." + tabla + " ADD column " + columna + tipoVar + " ;"
            else:
                sql = "ALTER TABLE domolibre." + tabla + " ADD column " + columna + tipoVar + ") ;"
            print "COLUMNA NUEVA ", sql
            try:
                cursor = cnx.cursor()
                cursor.execute(sql)
            except mysql.connector.Error as err:
                print sql, "ERROR1: ", err
        cnx.commit()        # necesario si la tabla es InnoDB, si no no grabara nada sin dar ningun tipo de error
        cursor.close()
        # ~ print len(salida) ,
    except mysql.connector.Error as err:
        print sql, "ERROR2: ", err


def insertDb(tabla, columna, strval, strTiempo, cnx):
    """
    Inserta dato en la base de datos
    strTiempo es la cadena en lenguaje sql con la marca de tiempo
    """
    sql = "INSERT INTO domolibre." + tabla + " SET datetime = " + strTiempo + ", " + columna + " = '" + strval + "' ON DUPLICATE KEY UPDATE " + columna + "='" + strval + "'"
    #~ print sql ,

    cursor = cnx.cursor()
    try:
        cursor.execute(sql)
        print "insertOK.",
        print "response:", cursor.rowcount,
    except mysql.connector.Error as err:
        print "error3:", err

    cnx.commit()        # necesario si la tabla es InnoDB, si no no grabara nada sin dar ningun tipo de error
    cursor.close()


def insertDbLog(tabla, endpName, endpRegAddress, strval, cnx):
    """
    Inserta dato en la base de datos
    strTiempo es la cadena en lenguaje sql con la marca de tiempo
    """
    # selecciona el tiempo del ultimo registro de esta variable  ## podria unificarlo con la siguiente peticion sql 
    sql = "select CAST(TIMEDIFF(NOW(),(SELECT datetime FROM domolibre." + tabla + " WHERE variable='" + endpName + "' ORDER BY datetime desc LIMIT 1)) AS CHAR)"
    # print sql

    try:
        cursor = cnx.cursor()
        cursor.execute(sql)
        difT = cursor.fetchone()[0]
        if difT is None:
            difT = ""
    #    print(difT)   ###
        cnx.commit()        # necesario si la tabla es InnoDB, si no no grabara nada sin dar ningun tipo de error
        cursor.close()
    except mysql.connector.Error as err:
        print sql + ": ERROR4: ", err


    # escribe en logLagarto
    sql = "INSERT INTO domolibre." + tabla + " (datetime, variable, dispositivo, valor,difT) VALUES (now(), '" + endpName + "' , '" + str(endpRegAddress) + "' , '" + strval + "', '" + difT + "') ;"
    # print sql
    cursor = cnx.cursor()

    try:
        cursor.execute(sql)
    except mysql.connector.Error as err:
        print sql + ": ERROR5: ", err

    cnx.commit()        # necesario si la tabla es InnoDB, si no no grabara nada sin dar ningun tipo de error
    cursor.close()


def escribeEnBd(endpName, endpRegAddress, strval, endpValueChanged):
    """
    escribe en la base de datos
    si no existe la columna la crea previamente.
    """
    #~ global host
    #~ global user
    #~ global passwd
    #~ global db
    print '(', endpName, endpRegAddress, ':', strval, endpValueChanged, ')',

    #__________________________________conexion a la base de datos
    try:
        cnx = mysql.connector.connect(host = "localhost", user = "domolibre", passwd = "domolibre", db= "domolibre")
        #~ cnx = mysql.connector.connect(host, user, passwd, db)
    except mysql.connector.Error as err:
        print "ERROR6: ", err
        
    #___________________________________inserta en BD log
    insertDbLog(tablaLog, endpName, endpRegAddress, strval, cnx)


    #~ tabla= tabla10

    #recrea el nombre de la variable
    if "Humidity" in endpName:
        columna = "h" + str(endpRegAddress)
        decimales = 1
        #~ print "si esta h "
    if "Temperature" in endpName:
        columna = "t" + str(endpRegAddress)
        decimales = 1
        #~ print "si esta t "
    if "Voltage" in endpName:
        columna= "v" + str(endpRegAddress)
        decimales = 3
        #~ print "si esta v "
    if "Binary" in endpName:
        #~ tabla= tablaActuadores
        columna = endpName.replace("Binary_", "b")
        #~ if endp.valueChanged :
            #~ columna= "b" + str(endpRegAddress)
            #~ decimales=0
            #~ print "endpName: ", endpName , "endp: ", endp
            #~ print endp
    if "PWM" in endpName:
        columna = endpName
        columna = "p" + str(endpRegAddress)
        decimales =0
        #~ print "si esta v "
    if "Motion" in endpName:
        #~ tabla= tablaActuadores
        columna = endpName
        columna = "m" + str(endpRegAddress)
        decimales = 0 
        #~ print "si esta v "
    if "Channel" in endpName:
        #~ columna= "c" + str(endpRegAddress)
        columna = endpName.replace("Channel_", "c")
        decimales = 0

    #~ print "endpName " , endpName

    #inserta en BD
    if "Binary" in endpName or "Motion" in endpName:
        if endpValueChanged:
            addColumn(columna, tablaActuadores, " CHAR(6) ", endpName, cnx)
            insertDb(tablaActuadores, columna, strval, "FROM_UNIXTIME(UNIX_TIMESTAMP(now()))", cnx)
    #~ elif "Channel" in endpName :
        #~ addColumn(columna, tablaCurrent, " INT () ", endpName, cnx)
        #~ insertDb (tablaCurrent, columna, strval, "FROM_UNIXTIME(UNIX_TIMESTAMP(now()))", cnx)       # a intervalos de 10 min
    else:
        addColumn(columna, tabla10, " DECIMAL(6," + str(decimales), endpName, cnx)
        insertDb(tabla10, columna, strval, "FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(now())+600)/600)*600)", cnx)       # a intervalos de 10 min
        addColumn(columna, tabla60, " DECIMAL(6," + str(decimales), endpName, cnx)
        insertDb(tabla60, columna, strval, "FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(now())+3600)/3600)*3600)", cnx)      # a intervalos de 60 min. a las horas en punto.

# ___________________________________#cierra la conexion con BD
    cnx.close()
