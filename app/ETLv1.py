


from google.transit import gtfs_realtime_pb2
from urllib.request import urlopen
import petl
from enum import Enum
from multiprocessing import Process
import multiprocessing
from google.transit import gtfs_realtime_pb2
from optparse import OptionParser
import time as t
import sys
from datetime import datetime, timedelta, date
from shapely.geometry import Point
from geoalchemy2 import shape
import psycopg2
import psycopg2.extras
from petl import look, todb
import argparse

# p = argparse.ArgumentParser()
# p.add_argument('-d','--d', nargs='+', help='db_connection', required=False)


def connection():
    con = psycopg2.connect(dbname='ov', user='postgres', host='172.17.0.2', password='postgres')
    cur = con.cursor()

    return con,cur

class ScheduleRelationship(Enum):
    SCHEDULED = 0
    ADDED = 1
    UNSCHEDULED = 2
    CANCELED = 3

class Cause (Enum):
    UNKNOWN_CAUSE = 1
    OTHER_CAUSE = 2
    TECHNICAL_PROBLEM = 3
    STRIKE = 4
    DEMONSTRATION = 5
    ACCIDENT = 6
    HOLIDAY = 7
    WEATHER = 8
    MAINTENANCE = 9
    CONSTRUCTION = 10
    POLICE_ACTIVITY = 11
    MEDICAL_EMERGENCY = 12

class Effect (Enum):
    NO_SERVICE = 1
    REDUCED_SERVICE = 2
    SIGNIFICANT_DELAYS = 3
    DETOUR = 4
    ADDITIONAL_SERVICE = 5
    MODIFIED_SERVICE = 6
    OTHER_EFFECT = 7
    UNKNOWN_EFFECT = 8
    STOP_MOVED = 9


def alert():
    con,cur = connection()

    alertdata = []
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(
        urlopen('http://gtfs.openov.nl/gtfs-rt/alerts.pb').read()
        )
    for entity in feed.entity:
        al = entity.alert
        active_period_start = datetime.fromtimestamp(al.active_period[0].start)
        active_period_end = datetime.fromtimestamp(al.active_period[0].end)
        route_id = al.informed_entity[0].route_id
        stop_id = al.informed_entity[0].stop_id
        cause = Cause(al.cause).name
        effect = Effect(al.effect).name
        message = al.header_text.translation[0].text
        description = al.description_text.translation[0].text

        alertdata.append([
                active_period_start,
                active_period_end,
                route_id,
                stop_id,
                cause,
                effect,
                message,
                description,
                ])

    qal = """INSERT INTO alerts (active_period_start, active_period_end, route_id, stop_id, cause, effect, message, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""

    psycopg2.extras.execute_batch(cur,qal,alertdata)
    print('adding alerts')
    con.commit()
    # table = petl.fromdicts(alertdata)
    # print(petl.nrows(table))
    # uniquealert = petl.unique(table)
    # print(petl.nrows(uniquealert))
    # petl.appenddb(uniquealert, con, 'alerts2')

    t.sleep(3600)

def vp():
    con,cur = connection()

    feed = gtfs_realtime_pb2.FeedMessage()
    while True:
        feed.ParseFromString(
        urlopen('http://gtfs.openov.nl/gtfs-rt/vehiclePositions.pb').read()
        )
        data = []
        # timer = date.today()
        for entity in feed.entity:
            vp = entity.vehicle
            timex = datetime.fromtimestamp(vp.timestamp)
            # if timex.date() == timer:
            if timex.time() < datetime.now().time() and timex > (datetime.now() - timedelta(minutes=60)):
                x = vp.position.longitude
                y = vp.position.latitude
                time = datetime.fromtimestamp(vp.timestamp)
                geo_loc = str(shape.from_shape(Point(x, y), srid=4326))
                schedule_relationship = vp.trip.schedule_relationship,
                direction_id = vp.trip.direction_id,
                current_stop_sequence = vp.current_stop_sequence,
                #see "enum VehicleStopStatus"
                current_status = vp.DESCRIPTOR.enum_types_by_name['VehicleStopStatus'].values_by_number[vp.current_status].name,
                rt_trip_id = vp.trip.trip_id,
                route_id = vp.trip.route_id,
                stop_id = vp.stop_id,
                trip_start_time = vp.trip.start_time,
                trip_start_date = vp.trip.start_date,
                vehicle_label = vp.vehicle.label,



                data.append(
                [time,
                geo_loc,
                direction_id,
                current_stop_sequence,
                current_status,
                rt_trip_id,
                stop_id,
                vehicle_label,
                ])

        qvp = """INSERT INTO vehicle_positions (time, geo_loc, direction_id, current_stop_sequence, current_status, rt_trip_id, stop_id, vehicle_label)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""

        psycopg2.extras.execute_batch(cur,qvp,data)
        print("adding vp")
        con.commit()

        t.sleep(60)





def stu(sleeptime=60):
    con,cur = connection()

    feed = gtfs_realtime_pb2.FeedMessage()

    while True:
        feed.ParseFromString(
        urlopen('http://gtfs.openov.nl/gtfs-rt/tripUpdates.pb').read()
        )
        liststu = []
        listtu= []
        liststucompare = []
        listtucompare = []
        timer = date.today()
        for entity in feed.entity:
            tu = entity.trip_update
            tu_id = entity.id
            try:
                start_time = datetime.strptime(tu.trip.start_time, '%H:%M:%S').time()
            except ValueError:
                continue
            start_date = tu.trip.start_date[:4]+"-"+tu.trip.start_date[4:6]+"-"+tu.trip.start_date[6:8]
            rt_trip_id = tu.trip.trip_id
            route_id = tu.trip.route_id
            direction_id = tu.trip.direction_id
            schedule = ScheduleRelationship(tu.trip.schedule_relationship).name

            listtu.append([tu_id,
                    start_time,
                    start_date,
                    rt_trip_id,
                    route_id,
                    direction_id,
                    schedule])

            # data2.append({'tu_id':tu_id,
            #         'start_time':start_time,
            #         'start_date':start_date,
            #         'trip_id':trip_id,
            #         'route_id':route_id,
            #         'direction_id':direction_id,
            #         'schedule':schedule
            #         })

            for stu in tu.stop_time_update:
                timex = datetime.fromtimestamp(stu.arrival.time)
                if timex.time() < datetime.now().time() and timex > (datetime.now() - timedelta(minutes=60)):
                    time = datetime.fromtimestamp(stu.arrival.time)
                    departure_time = datetime.fromtimestamp(stu.departure.time)
                    rt_trip_id = rt_trip_id,
                    stop_sequence = stu.stop_sequence,
                    stop_id = stu.stop_id,
                    arrival_delay = stu.arrival.delay,
                    departure_delay = stu.arrival.delay,
                    tu_id = tu_id

                    liststu.append([
                    time,
                    departure_time,
                    stop_sequence,
                    rt_trip_id,
                    stop_id,
                    arrival_delay,
                    departure_delay,
                    tu_id
                    ])

                # data.append(
                # {'time':time,
                # 'departure_time':departure_time,
                # 'stop_sequence':stop_sequence,
                # 'trip_id':trip_id,
                # 'stop_id':stop_id,
                # 'arrival_delay':arrival_delay,
                # 'departure_delay':departure_delay,
                # 'tu_id':tu_id
                # })


        # table = petl.fromdicts(data2)
        # un = petl.unique(table)
        # print(petl.nrows(table), petl.nrows(un))

        # table2 = petl.fromdicts(data)
        # un1 = petl.unique(table2)
        # print(petl.nrows(table2), petl.nrows(un1))



        liststufilter = list(filter(lambda x:x not in liststucompare, liststu))
        listtufilter = list(filter(lambda x:x not in listtucompare,listtu))

        qtu = """INSERT INTO trip_updates (tu_id, start_time, start_date, rt_trip_id, route_id, direction_id, schedule)
            VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""

        qstu = """INSERT INTO stop_time_updates (time, departure_time, stop_sequence, rt_trip_id, stop_id, arrival_delay, departure_delay, tu_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""

        psycopg2.extras.execute_batch(cur,qtu,listtufilter)
        print('adding tu')
        psycopg2.extras.execute_batch(cur,qstu,liststufilter)
        print('adding stu')
        con.commit()
        liststucompare = liststu.copy()
        listtucompare = listtu.copy()


        t.sleep(sleeptime)

def tstu(sleeptime=60):
    con,cur = connection()

    feed = gtfs_realtime_pb2.FeedMessage()

    while True:
        feed.ParseFromString(
        urlopen('http://gtfs.openov.nl/gtfs-rt/trainUpdates.pb').read()
        )
        liststu = []
        listtu= []
        liststucompare = []
        listtucompare = []
        timer = date.today()
        for entity in feed.entity:
            tu = entity.trip_update
            tu_id = entity.id
            try:
                start_time = datetime.strptime(tu.trip.start_time, '%H:%M:%S').time()
            except ValueError:
                continue
            start_date = tu.trip.start_date[:4]+"-"+tu.trip.start_date[4:6]+"-"+tu.trip.start_date[6:8]
            rt_trip_id = tu.trip.trip_id
            route_id = tu.trip.route_id
            direction_id = tu.trip.direction_id
            schedule = ScheduleRelationship(tu.trip.schedule_relationship).name

            listtu.append([tu_id,
                    start_time,
                    start_date,
                    rt_trip_id,
                    route_id,
                    direction_id,
                    schedule])

            # data2.append({'tu_id':tu_id,
            #         'start_time':start_time,
            #         'start_date':start_date,
            #         'trip_id':trip_id,
            #         'route_id':route_id,
            #         'direction_id':direction_id,
            #         'schedule':schedule
            #         })

            for stu in tu.stop_time_update:
                timex = datetime.fromtimestamp(stu.arrival.time)
                if timex.time() < datetime.now().time() and timex > (datetime.now() - timedelta(minutes=60)):
                    time = datetime.fromtimestamp(stu.arrival.time)
                    departure_time = datetime.fromtimestamp(stu.departure.time)
                    rt_trip_id = rt_trip_id,
                    stop_sequence = stu.stop_sequence,
                    stop_id = stu.stop_id,
                    arrival_delay = stu.arrival.delay,
                    departure_delay = stu.arrival.delay,
                    tu_id = tu_id

                    liststu.append([
                    time,
                    departure_time,
                    stop_sequence,
                    rt_trip_id,
                    stop_id,
                    arrival_delay,
                    departure_delay,
                    tu_id
                    ])

                # data.append(
                # {'time':time,
                # 'departure_time':departure_time,
                # 'stop_sequence':stop_sequence,
                # 'trip_id':trip_id,
                # 'stop_id':stop_id,
                # 'arrival_delay':arrival_delay,
                # 'departure_delay':departure_delay,
                # 'tu_id':tu_id
                # })


        # table = petl.fromdicts(data2)
        # un = petl.unique(table)
        # print(petl.nrows(table), petl.nrows(un))

        # table2 = petl.fromdicts(data)
        # un1 = petl.unique(table2)
        # print(petl.nrows(table2), petl.nrows(un1))



        liststufilter = list(filter(lambda x:x not in liststucompare, liststu))
        listtufilter = list(filter(lambda x:x not in listtucompare,listtu))

        qtu = """INSERT INTO train_updates (tu_id, start_time, start_date, rt_trip_id, route_id, direction_id, schedule)
            VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""

        qstu = """INSERT INTO train_stop_time_updates (time, departure_time, stop_sequence, rt_trip_id, stop_id, arrival_delay, departure_delay, tu_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""

        psycopg2.extras.execute_batch(cur,qtu,listtufilter)
        print('adding train_tu')
        psycopg2.extras.execute_batch(cur,qstu,liststufilter)
        print('adding train_stu')
        con.commit()
        liststucompare = liststu.copy()
        listtucompare = listtu.copy()


        t.sleep(sleeptime)




if __name__ == "__main__":
    p1 = Process(target =vp)
    p2 = Process(target =stu)
    p3 = Process(target=alert)
    p4 = Process(target=tstu)

    p1.start()
    p2.start()
    p3.start()
    p4.start()
