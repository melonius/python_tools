# -*- coding: utf-8 -*-
import os
import mysql.connector
from mysql.connector import errorcode

import urllib2
import email.utils as eut
import time
import datetime
import commands

import MySQLdb
from datetime import date;
from time import localtime, strftime

global cnx
global calibrado


def getLastValue(variable):
    """
    Adquiere el último valor de variable de la base de datos y la dif de tiempo
    """
    sql = "select TIMEDIFF(NOW(),datetime) AS difT , variable, dispositivo, valor FROM domolibre.logLagarto WHERE variable='" + variable + "' ORDER BY id desc LIMIT 1"

    #~ print sql ,

    ultimoValor = []
    try:
	cursor = cnx.cursor(dictionary=True)
	rows_affected = cursor.execute(sql)
	registro = cursor.fetchone()
	if (registro != None):
	   ultimoValor  = [ registro['valor'], registro['difT']]
	   #~ ultimoValor [1] = registro['difT']
	cnx.commit()        # necesario si la tabla es InnoDB, si no no grabara nada sin dar ningun tipo de error
	cursor.close()
    except mysql.connector.Error as err:
	print sql, "ERROR: ", err
    return ultimoValor

def hABS(temp, hr):
    """
    Adquiere el último valor de variable de la base de datos y la dif de tiempo
    """
    humABS = 216.679 * (hr / 100) * (6.116441 * pow (2.71828183 , 17.27*temp / (237.3+temp))) / (temp+273.15);	
    #~ humABS = hr /  100	
    #~ humABS = 216.679 * (hr / 100) 	
    return humABS

def calibra(dato, variab):
    datoCalibrado = calibrado[variab][4] * float(dato) + calibrado[variab][5]
    return datoCalibrado

def actua( comando):
    if comando == 'ON':
        os.system('wget -q -O - http://localhost:8001/values/40.11.1/?value=ON')
    else:
        os.system('wget -q -O - http://localhost:8001/values/40.11.1/?value=OFF')




def main():
### 0. conecta con la BD
    try:
	global cnx
	cnx = mysql.connector.connect(host = "localhost", user = "domolibre", passwd = "domolibre", db = "domolibre")
	#~ cnx = mysql.connector.connect(host, user, passwd, db)
    except mysql.connector.Error as err:
	print "ERROR: ", err
	
    global calibrado
	
    ### 1. lee los últimos valores de la BD  ti te hi he h1 h2
    t2,t2d = getLastValue('Temperature_2')
    t11,t11d = getLastValue('Temperature_11')
    #~ print t2, t2d
    h2,h2d = getLastValue('Humidity_2')
    h11,h11d = getLastValue('Humidity_11')

    cnx.close ()



    #~ print h2, h2d
    #~ print t2, h2, t11, h11

    ### 2. calibrado y humedad absoluta
    # calibrar    negativo si hay que bajar el valor
    calibrado = dict()
    calibrado['t11']=[28,0,35,-0.3]
    calibrado['t2']=[23,0.1,33,-0.3]
    #~ //~ $calibrado['t4']=[50,-5,80,-3)
    calibrado['t37']=[25,0.1,33,0.1]

    calibrado['h11']=[45,0,80,-1]
    calibrado['h2']=[45,2,75,3]
    calibrado['h4']=[50,-5,80,-3]
    calibrado['h37']=[45,0,80,1]

    if ( calibrado):
	for k, v in calibrado.iteritems():
	    base1=float(calibrado[k][0])
	    dbase1=float(calibrado[k][1])
	    base2=float(calibrado[k][2])
	    dbase2=float(calibrado[k][3])

	    B= ((base1+dbase1) -(base2+dbase2))/(base1-base2)
	    C= (base1+dbase1)-(B*base1)
	    #~ print "calibrado3:\t", k, base1 , dbase1 , base2 , dbase2  , B , C 

	    calibrado[k].append(B)
	    calibrado[k].append(C)

    ### print calibrado

    minutosMax = 11
    dtmax = minutosMax*60
    if t2d.seconds > dtmax and h2d.seconds > dtmax:
	print t2d,' o ', h2d, ' es mayor de ',minutosMax, ' minutos'
	f1.write( strftime("%Y-%m-%d %H:%M:%S", localtime()) + ' dif tiempos: ' + str(t2d) + '  ó ' +  str(h2d) + ' mayor que ' + str(minutosMax))

    else:
	#calibrar
	if calibrado['t2'] and t2 !='':
	    t2= calibra(t2, 't2')
	if calibrado['h2'] and h2 !='':
	    h2= calibra(h2, 'h2')

	h2ABS = hABS (float(t2),float(h2))
	print t2, h2, h2ABS

	if calibrado['t11'] and t11 !='':
	    t11= calibra(t11, 't11')
	if calibrado['h11'] and h11 !='':
	    h11= calibra(h11, 'h11')

	h11ABS = hABS (float(t11),float(h11))
	print t11, h11, h11ABS



	"""
	    calcula hABS

	    lee el estado del ventilador
	2. decide la orden
	    si >80% y > 2gr dif y < 4º dif
	    si >80% y > 1gr dif y < 1º dif
	    si >85% y > 2gr dif y < 6º dif
	    
	3. actua encendiendo o apagando
	    os.system(“uname -a”)
	"""
	# actua
	f1=open('/home/mel/log/venti.log', 'a+')
	#~ f1.write(datetime.datetime.now().time())
	f1.write( '\n')
	f1.write( strftime("%Y-%m-%d %H:%M:%S", localtime()) + ' ' + '{:05.2f}'.format(h11) + ' ' +  '{:05.2f}'.format(h11ABS) + ' ' + '{:05.2f}'.format(h2ABS) + ' ' + '{:05.2f}'.format(t11) + ' ' + '{:05.2f}'.format(t2))
	if h11 > 90:
	    if (h11ABS - h2ABS) > 1:
		actua('ON')
		f1.write(' >90% y >1gr >> ON')
	    else:
		actua('OFF')
		f1.write(' >90% y <1gr >> OFF')
	  
	elif h11 > 85:
	    if (h11ABS - h2ABS) > 2 and (t2 - t11) < 7:
		actua('ON')
		f1.write(' >85% y >2gr  y <7º  >> ON')
	    else:
		actua('OFF')
		f1.write(' >85% y (dif  <2gr o  >8º ) >> OFF')
	elif h11 > 80:
	    if (h11ABS - h2ABS) > 2 and (t2 - t11) < 4:
		actua('ON')
		f1.write(' >80%  >2gr y <4º  >> ON')
	    elif (h11ABS - h2ABS) > 3 and (t2 - t11) < 3:
		actua('ON')
		f1.write(' >80%  >3gr y <3º  >> ON')
	    else:
		actua('OFF')
		f1.write(' >80% y ( >2gr y <4º) o (  >3gr y  < 3º) >> OFF')
	else:
	    actua('OFF')
	    f1.write(' <80%  >> OFF')


if __name__ == "__main__":
    # execute only if run as a script
    main()
