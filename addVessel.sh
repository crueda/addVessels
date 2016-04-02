#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# date: 2015-12-21
# mail: carlos.rueda@deimos-space.com
# version: 1.0

########################################################################
# version 1.0 release notes:
# Initial version
########################################################################

from __future__ import division
import time
import datetime
import os
import sys
import utm
import SocketServer, socket
import logging, logging.handlers
import json
import httplib2
from threading import Thread
import pika
import MySQLdb


########################################################################
# configuracion y variables globales
from configobj import ConfigObj
config = ConfigObj('./addVessel.properties')

LOG = config['directory_logs'] + "/addVessel.log"
LOG_FOR_ROTATE = 10

DB_IP = config['BBDD_host']
DB_NAME = config['BBDD_name']
DB_USER = config['BBDD_username']
DB_PASSWORD = config['BBDD_password']

FLEET_ID = config['fleet_id']

########################################################################

# Se definen los logs internos que usaremos para comprobar errores
try:
    logger = logging.getLogger('addVessel')
    loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG, 'midnight', 1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    loggerHandler.setFormatter(formatter)
    logger.addHandler(loggerHandler)
    logger.setLevel(logging.DEBUG)
except:
    print '------------------------------------------------------------------'
    print '[ERROR] Error writing log at %s' % LOG
    print '[ERROR] Please verify path folder exits and write permissions'
    print '------------------------------------------------------------------'
    exit()

########################################################################


########################################################################

def checkBoat(vehicleLicense):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)
	try:
		cursor = dbConnection.cursor()
		cursor.execute(""" SELECT DEVICE_ID from VEHICLE where VEHICLE_LICENSE= '%s' limit 0,1""" % (vehicleLicense,))
		result = cursor.fetchall()
		if len(result)==1 :
			return result[0][0]
		else :
			return '0'
		cursor.close
		dbConnection.close
	except Exception, error:
		logger.error('Error executing query: %s', error)

def addBoat(vehicleLicense):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)
	try:
		query = """INSERT INTO VEHICLE (VEHICLE_LICENSE,BASTIDOR,ALIAS,POWER_SWITCH,ALARM_STATE,SPEAKER,START_STATE,WARNER,PRIVATE_MODE,WORKING_SCHEDULE,ALARM_ACTIVATED,PASSWORD,CELL_ID,ICON_DEVICE, KIND_DEVICE,AIS_TYPE,MAX_SPEED,CONSUMPTION,CLAXON,MODEL_TRANSPORT,PROTOCOL_ID,BUILT,CALLSIGN,MAX_PERSONS,MOB,EXCLUSION_ZONE,FLAG,INITIAL_DATE_PURCHASE) VALUES (xxx,'',xxx,-1,-1,-1,'UNKNOWN',-1,0,0,0,'',0,1000,1,3,500,0.0,-1,'boat',0,0,xxx,-1,-1,0,'',NOW())"""
		QUERY = query.replace('xxx', vehicleLicense)
		cursor = dbConnection.cursor()
		cursor.execute(QUERY)
		dbConnection.commit()
		logger.info('Boat %s added at database', vehicleLicense)
		cursor.close
		cursor = dbConnection.cursor()
		cursor.execute("""SELECT LAST_INSERT_ID()""")
		result = cursor.fetchall()
		cursor.close
		dbConnection.close()
		logger.info('Boat added with DEVICE_ID: %s', result[0][0])
        	return result[0][0]
	except Exception, error:
		logger.error('Error executing query : %s', error)

def addComplementary(vehicleLicense, deviceID):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)
	try:
		DEVICE_ID = str(deviceID)
		query = """INSERT INTO OBT (IMEI, VEHICLE_LICENSE, DEVICE_ID, VERSION_ID, ALARM_RATE,COMS_MODULE,CONFIGURATION_ID,CONNECTED,MAX_INVALID_TRACKING_SPEED,PRIORITY,REPLICATED_SERVER_ID,GSM_OPERATOR_ID,ID_CARTOGRAPHY_LAYER,ID_TIME_ZONE,INIT_CONFIG,STAND_BY_RATE,HOST,LOGGER,TYPE_SPECIAL_OBT) VALUES (xxx,xxx,yyy,'11','','127.0.0.1',0,0,500,0,0,0,0,1,'','','',0,12)"""
		queryOBT = query.replace('xxx', vehicleLicense).replace('yyy', DEVICE_ID)
		query = """INSERT INTO HAS (FLEET_ID,VEHICLE_LICENSE,DEVICE_ID) VALUES (533,xxx,yyy)"""
		queryHAS = query.replace('xxx', vehicleLicense).replace('yyy', DEVICE_ID).replace('fff', FLEET_ID)
		cursor = dbConnection.cursor()
		cursor.execute(queryOBT)
		dbConnection.commit()
		logger.info('OBT info saved at database for deviceID %s', deviceID)
		cursor.execute(queryHAS)
		dbConnection.commit()
		logger.info('HAS info saved at database for deviceID %s', deviceID)
		cursor.close
		dbConnection.close()
	except Exception, error:
		logger.error('Error executing query: %s', error)
		
if len(sys.argv) != 2: 
        print "Introduce el MMSI como parametro"
else: 
	vehicleLicense = sys.argv[1]
	logger.info('Checking if boat %s is at database...', vehicleLicense)

	if (checkBoat(vehicleLicense) == '0'):
		logger.info('Boat is not at database.')
		# creamos el dispositivo
		deviceID = addBoat(vehicleLicense)
		logger.info('Boat saved at database with DEVICE_ID %s', deviceID)
		addComplementary(vehicleLicense, deviceID)
		#time.sleep(0.1)
	else:
		logger.info('Boat %s found at database', vehicleLicense)