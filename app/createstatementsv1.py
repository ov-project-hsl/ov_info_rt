import psycopg2

con = psycopg2.connect(dbname='ov', user='postgres', host='172.18.0.2', password='password')



cur = con.cursor()
# try:

cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")

cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

# # try:
# #     print("database bevat nu timescaledb en postgis")

# except:
# 	print("oops kon postgis/timescaledb niet koppelen aan de database")



#-----
cur.execute(
"""
CREATE TABLE IF NOT EXISTS vehicle_positions(
time timestamp,
rt_trip_id varchar(10),
current_stop_sequence varchar(10),
vehicle_label integer,
direction_id integer,
current_status varchar(20),
stop_id varchar(10),
geo_loc geometry(Point)
)""")


cur.execute("""CREATE TABLE IF NOT EXISTS trip_updates (
    tu_id varchar,
	start_time time,
	start_date date,
	rt_trip_id integer,
	route_id integer,
	direction_id integer,
	schedule varchar(30)
);""")

cur.execute("""create unique index if not exists trip_updates_tu_id_uindex
	on trip_updates (tu_id);""")

#---------
cur.execute("""CREATE TABLE IF NOT EXISTS stop_time_updates(
    time timestamp,
	stop_sequence integer,
	stop_id varchar(10),
	rt_trip_id varchar(10),
	arrival_delay integer,
	departure_delay integer,
	departure_time time,
	tu_id varchar(40)
	constraint stop_time_updates_fk
	references trip_updates (tu_id),
	constraint stop_time_updates_time_stop_id_key
	unique (time, stop_id)

)""")

#---------
cur.execute("""CREATE TABLE IF NOT EXISTS train_updates(
	tu_id varchar,
	start_time time,
	start_date date,
	rt_trip_id varchar(30),
	route_id varchar(30),
	direction_id integer,
	schedule varchar(30))""")


cur.execute("""create unique index if not exists train_updates_tu_id_uindex
	on train_updates (tu_id);""")




cur.execute("""CREATE TABLE IF NOT EXISTS train_stop_time_updates
(
	time timestamp,
	stop_sequence integer,
	stop_id varchar(30),
	rt_trip_id varchar(30),
	arrival_delay integer,
	departure_delay integer,
	departure_time time,
	tu_id varchar(40)
		constraint train_stop_time_updates_k
			references train_updates (tu_id),
	constraint stu_trains_time_stop_id_key
		unique (time, stop_id)
);""")



#-----
cur.execute("""CREATE TABLE IF NOT EXISTS alerts (
    active_period_start timestamp,
	active_period_end timestamp,
	route_id varchar(10),
	stop_id varchar(10),
	cause varchar(30),
	effect varchar(30),
	message text,
	description text,
	constraint alerts_active_period_start_stop_id_key
		unique (active_period_start, stop_id)
);""")

#------

#---------






cur.execute("SELECT create_hypertable(%s, %s)", ("vehicle_positions", "time"))
cur.execute("SELECT create_hypertable(%s, %s)", ("stop_time_updates", "time"))
cur.execute("SELECT create_hypertable(%s, %s)", ("train_stop_time_updates", "time"))


con.commit()
cur.close()
con.close()
