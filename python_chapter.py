# -*- coding: utf-8 -*-
"""
Created on Thu Apr  1 19:40:32 2021

@author: adm
"""

import sqlalchemy as adb
from sqlalchemy import MetaData
import cx_Oracle as ora
import pandas as pd
import datetime as dt

#Подключение к Oracle

l_user = 'Ovchinnikov_MG'
l_pass = '02031967'
l_tns = ora.makedsn('13.95.167.129', 1521, service_name = 'pdb1')

l_conn_ora = adb.create_engine(r'oracle://{p_user}:{p_pass}@{p_tns}'.format(
    p_user = l_user
    , p_pass = l_pass
    , p_tns = l_tns
    )
    )

print(l_conn_ora)

#Подключение к таблице

l_meta = MetaData(l_conn_ora)
l_meta.reflect()


l_f1_drivers = l_meta.tables['f1_drivers_1']
print(l_f1_drivers)
l_f1_races = l_meta.tables['f1_races']
print(l_f1_races)
l_f1_results = l_meta.tables['f1_results']
print(l_f1_races)

#Чтение csv

l_file_csv_drivers = pd.read_csv(r'C:\Users\Эл\Desktop\F1\drivers.csv')
print(l_file_csv_drivers)
#l_file_csv_races = pd.read_csv(r'C:\Users\Эл\Desktop\F1\races.csv')
#print(l_file_csv_races)
#l_file_csv_results = pd.read_csv(r'C:\Users\Эл\Desktop\F1\results.csv')
#print(l_file_csv_results)

#Замена \N на 0

l_result_csv_drivers = l_file_csv_drivers.replace("\\N","0")
#l_result_csv_races = l_file_csv_races.replace("\\N","0")
#l_result_csv_results = l_file_csv_results.replace("\\N","0")

l_list_csv_drivers = l_result_csv_drivers.values.tolist()
#l_list_csv_races = l_result_csv_races.values.tolist()
#l_list_csv_results = l_result_csv_results.values.tolist()

# Вставка данных в ora в таблицу f1_drivers

for i in l_list_csv_drivers:
    l_f1_drivers.insert([l_f1_drivers.c.d_driverid, l_f1_drivers.c.d_driverref, l_f1_drivers.c.d_number, l_f1_drivers.c.d_code,
    l_f1_drivers.c.d_forename, l_f1_drivers.c.d_surname, l_f1_drivers.c.d_dob, l_f1_drivers.c.d_nationality, l_f1_drivers.c.d_url]).values(
        d_driverid   = i[0],
        d_driverref   = i[1],
        d_number    = int(i[2]),
        d_code    = i[3],
        d_forename   = i[4],
        d_surname    = i[5],
        d_dob     = dt.datetime.strptime(i[6], '%Y-%m-%d'),
        d_nationality    = i[7],
        d_url    = i[8]
        ).execute()
    print(i)
'''
for j in l_list_csv_races:
    l_f1_races.insert([l_f1_races.c.r_raceid, l_f1_races.c.r_year, l_f1_races.c.r_round, l_f1_races.c.r_circuitid,
    l_f1_races.c.r_name, l_f1_races.c.r_date, l_f1_races.c.r_time, l_f1_races.c.r_url]).values(
        r_raceid   = j[0],
        r_year   = j[1],
        r_round    = j[2],
        r_circuitid = j[3],
        r_name   = j[4],
        r_date    = dt.datetime.strptime(j[5], '%Y-%m-%d'),
        r_time     = j[6], #dt.datetime.strptime(j[6], '%H:%M:%S'),
        r_url    = j[7],
        ).execute()
    print(j)

for h in l_list_csv_results:
    l_f1_results.insert([l_f1_results.c.re_resultid , l_f1_results.c.re_raceid, l_f1_results.c.re_driverid, l_f1_results.c.re_constructorid,
    l_f1_results.c.re_number , l_f1_results.c.re_grid  , l_f1_results.c.re_position, l_f1_results.c.re_positiontext , l_f1_results.c.re_positionorder, 
    l_f1_results.c.re_points , l_f1_results.c.re_laps  , l_f1_results.c.re_time  , l_f1_results.c.re_milliseconds , l_f1_results.c.re_fastestlap,
    l_f1_results.c.re_rank  , l_f1_results.c.re_fastestlaptime  , l_f1_results.c.re_fastestlapspeed  , l_f1_results.c.re_statusid]).values(
        re_resultid   = h[0],
        re_raceid   = h[1],
        re_driverid    = h[2],
        re_constructorid    = h[3],
        re_number   = h[4],
        re_grid    = h[5],
        re_position     = h[6],
        re_positiontext    = h[7],
        re_positionorder    = h[8],
        re_points = h[9],
        re_laps = h[10],
        re_time = h[11],
        re_milliseconds = h[12],
        re_fastestlap = h[13],
        re_rank = h[14],
        re_fastestlaptime = h[15],#dt.datetime.strptime(h[15], '%M:%S.%f'),
        re_fastestlapspeed = h[16],
        re_statusid = h[17],
        ).execute()
    print(h)

'''

print('Запуск процедуры')

l_conn_ora.execute(adb.text('BEGIN pkg_f1_drivers_res.f1_drivers_results_proc; END;'))

print('Готово')
      
      
      
    