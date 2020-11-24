import os
import time
import datetime

fileLocation = "C:\\Pricer\\R3Server\\logs\\data_importold.log"
year = 2020
month = 11
day = 16
hour = 11
minute = 00
second = 0

date = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)
modTime = time.mktime(date.timetuple())

os.utime(fileLocation, (modTime, modTime))