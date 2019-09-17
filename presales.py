#!/usr/bin/env python3.6
# Collect raw presales data from Fandango and put it into a database.
# Kelvin Porter 2018

from apscheduler.schedulers.blocking import BlockingScheduler
import numpy as np
import pandas as pd
import pytz
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import ProgrammingError, IntegrityError
from sqlalchemy.types import DateTime, Integer, Text, Numeric, BigInteger
from tzwhere import tzwhere

import asyncio
from datetime import datetime as da
from datetime import timedelta as td
import logging
import json
import time

tickets = pd.DataFrame()
processed_tickets = pd.DataFrame()
count, add_count = 1000, 1000
tz = tzwhere.tzwhere(forceTZ=True)
log = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.WARNING,
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    filename='/home/yack/.local/share/fml/collector.log'
)

def get_tickets():
    global tickets, count, add_count
    url = "https://www.fandango.com/dataviz/dataviz.aspx?dvm=transactions&when={0}&where=country:USA&count={1}".format(da.today(), count)
    r = requests.get(url)
    if r.status_code != 200:
        log.info("Server gave status code {}, skipping run and adding more to the count.".format(r.status_code))
        count += add_count
        return
    count = 1000 # reset count since this is a successful run, else statement not needed
    json_ = json.JSONDecoder().decode(r.text[:30] + r.text[31:])

    raw_dataframe = pd.DataFrame.from_records(json_['transactions'])
    raw_dataframe['fandango_id'] = raw_dataframe.movie.transform(lambda mov: mov['id'])
    raw_dataframe['title'] = raw_dataframe.movie.transform(lambda mov: mov['title'])
    raw_dataframe['theater_city'] = raw_dataframe.theater.transform(lambda t: t['city'])
    raw_dataframe['theater_name'] = raw_dataframe.theater.transform(lambda t: t['name'])
    raw_dataframe['theater_country'] = raw_dataframe.theater.transform(lambda t: t['country'])
    raw_dataframe['theater_state'] = raw_dataframe.theater.transform(lambda t: t['state'])
    raw_dataframe['theater_postal_code'] = raw_dataframe.theater.transform(lambda t: t['postal_code'])
    raw_dataframe['theater_lat'] = raw_dataframe.theater.transform(lambda t: t['lat'])
    raw_dataframe['theater_lon'] = raw_dataframe.theater.transform(lambda t: t['lon'])
    raw_dataframe['theater_id'] = raw_dataframe.theater.transform(lambda t: t['id'])
    raw_dataframe["buy_datetime"] = pd.to_datetime(raw_dataframe["buy_datetime"], format="%Y-%m-%d %H:%M:%S")
    raw_dataframe["buy_datetime_utc"] = pd.to_datetime(raw_dataframe["buy_datetime_utc"], format="%Y-%m-%d %H:%M:%S")
    raw_dataframe["show_datetime"] = pd.to_datetime(raw_dataframe["show_datetime"], format="%Y-%m-%d %H:%M:%S")
    raw_dataframe.drop(["theater"], axis=1, inplace=True)

    tickets = pd.concat([tickets, raw_dataframe])
    log.info("Made call to Fandango: length now " + str(len(tickets.index)))

def remove_duplicates():
    global tickets, processed_tickets
    processed_tickets = pd.concat([processed_tickets, tickets], sort=True).drop_duplicates(["buy_datetime", "show_datetime", "theater_id", "title"], keep="first")
    tickets = pd.DataFrame()
    # in case certain things are duplicates in two calls of tickets across transfers
    processed_tickets.drop_duplicates(["buy_datetime_utc", "show_datetime", "title", "theater_name"], keep="first").reset_index(drop=True, inplace=True)
    log.debug("Removed duplicates: length now " + str(len(processed_tickets.index)))

def insert_into_database():
    global tickets, processed_tickets
    log.warning("Inserting into database...")

    # copy this to lock it so that other methods can't change it
    locked_tickets = processed_tickets

    # convert
    locked_tickets["theater_id"] = locked_tickets["theater_id"].astype(str)
    locked_tickets.reset_index(drop=True, inplace=True)

    # fix all the showtimes to be the right timestamps
    def fix_timezone(row):
            return row["show_datetime"] - pytz.timezone(tz.tzNameAt(row["theater_lat"], row["theater_lon"], forceTZ=True)).utcoffset(row['show_datetime'], is_dst=False)
    locked_tickets["show_datetime"] = locked_tickets.apply(lambda row: fix_timezone(row), axis=1)
    def convert_to_utc(row):
        return (pytz.timezone(tz.tzNameAt(row["theater_lat"], row["theater_lon"], forceTZ=True)).localize(row["show_datetime"], is_dst=False).astimezone(pytz.utc)).replace(tzinfo=None)
    locked_tickets["show_datetime_utc"] = locked_tickets.apply(lambda row: convert_to_utc(row), axis=1)

    # update all the theaters...
    try:
        old_theaters = pd.read_sql_table(table_name="theaters", schema="presales", con=engine)
        old_theaters["theater_id"] = old_theaters["theater_id"].astype(str)
        theaters = locked_tickets[["theater_id", "theater_name", "theater_city", "theater_state", "theater_country", "theater_lat", "theater_lon", "theater_postal_code"]]
        theaters.columns = ["theater_id", "name", "city", "state", "country", "lat", "lon", "postal_code"]
        theaters = pd.concat([old_theaters, old_theaters, theaters], ignore_index=True).drop_duplicates(["theater_id"], keep=False)
        theaters.to_sql(name="theaters",
                        schema="presales",
                        con=engine,
                        index=False,
                        if_exists="append",
                        dtype={"theater_id": Text(),
                                "name": Text(),
                                "city": Text(),
                                "state": Text(),
                                "country": Text(),
                                "lat": Numeric(),
                                "long": Numeric(),
                                "postal_code": Text()})
        log.warning("Updated theater table. Added {} new theaters.".format(len(theaters.index)))
    except IntegrityError:
        log.error("IntegrityError: unique constraint failed... something's very wrong")


    # add the showtimes table to the database, containing any unique showtimes
    try:
        # pull showtimes for every day in the tickets to be added to check for
        # duplicates
        showtimes = locked_tickets[["fandango_id", "theater_id", "show_datetime", "show_datetime_utc"]].drop_duplicates(keep="first")
        dates = repr(set(showtimes["show_datetime_utc"].dt.floor("d").astype(str).str[:10]))[1:-1]
        showtimes_query = "select * from presales.showtimes where show_datetime_utc::date in ({}) order by show_datetime_utc ASC;".format(dates)
        try:
            old_showtimes = pd.DataFrame.from_records(engine.execute(showtimes_query)).drop(columns=[0])
            old_showtimes.columns = ["fandango_id", "theater_id", "show_datetime", "show_datetime_utc"]
        except KeyError:
            old_showtimes = pd.DataFrame(columns=["fandango_id", "theater_id", "show_datetime", "show_datetime_utc" ])
        old_showtimes["fandango_id"] = old_showtimes["fandango_id"].astype(int)
        old_showtimes["theater_id"] = old_showtimes["theater_id"].astype(str)
        old_showtimes["show_datetime"] = pd.to_datetime(old_showtimes["show_datetime"], format="%Y-%m-%d %H:%M:%S")
        old_showtimes["show_datetime_utc"] = pd.to_datetime(old_showtimes["show_datetime_utc"], format="%Y-%m-%d %H:%M:%S")
        # Timezones are fixed above.
        showtimes_to_add = pd.concat([old_showtimes, old_showtimes, showtimes], ignore_index=True).drop_duplicates(["fandango_id","theater_id", "show_datetime"], keep=False)
        showtimes = showtimes_to_add.reset_index(drop=True)
        showtimes_to_add.to_sql(name="showtimes",
                                schema="presales",
                                con=engine,
                                index=False,
                                if_exists="append",
                                dtype={"fandango_id": Integer(),
                                       "theater_id": Text(),
                                       "show_datetime": DateTime(),
                                       "show_datetime_utc": DateTime()})
        log.warning("Updated showtimes table. Added {} new showtimes.".format(len(showtimes_to_add.index)))
        del showtimes_to_add
    except IntegrityError:
        log.error("IntegrityError: unique constraint failed... something's very wrong")
        raise

    # add any new movies to the movies key table
    try:
        old_movies = pd.read_sql_table(table_name="movies", schema="presales", con=engine)
        movies = locked_tickets[["fandango_id", "title"]].drop_duplicates()
        movies = pd.concat([old_movies, old_movies, movies], ignore_index=True).drop_duplicates(["fandango_id"], keep=False)
        movies.to_sql(name="movies",
                      schema="presales",
                      con=engine,
                      index=False,
                      if_exists="append",
                      dtype={"fandango_id": Integer(), "title": Text()})
        log.info("Updated movie table. Added {} new movies.".format(len(movies.index)))
    except IntegrityError:
        log.error("IntegrityError: unique constraint failed... something's very wrong")


    df_to_db = locked_tickets[["fandango_id", "theater_id", "buy_datetime", "buy_datetime_utc", "show_datetime"]]

    query_string = ('WITH t AS ('
                   'SELECT * FROM presales."transactions" ORDER BY "buy_datetime_utc" DESC LIMIT 10000'
                   ') SELECT * FROM t ORDER BY buy_datetime_utc ASC;')

    last_update = pd.DataFrame.from_records(engine.execute(query_string))
    try:
        last_update = last_update.drop(columns=[0])
        last_update.columns = ["fandango_id", "showtime_id", "buy_datetime", "buy_datetime_utc"]

    except KeyError:
        last_update = pd.DataFrame(columns=["fandango_id", "showtime_id", "buy_datetime", "buy_datetime_utc"])
        log.warning("table empty, setting to empty df")
        pass

    # redo the showtimes query to get new stuff
    showtimes = pd.DataFrame.from_records(engine.execute(showtimes_query))
    showtimes.columns = ["showtime_id", "fandango_id", "theater_id", "show_datetime", "show_datetime_utc"]
    showtimes["theater_id"] = showtimes["theater_id"].astype(str)
    showtimes["fandango_id"] = showtimes["fandango_id"].astype(int)

    showtimes.set_index(["fandango_id", "theater_id", "show_datetime"], inplace=True)
    def get_showtime_id(row):
        try:
            return showtimes.loc[row["fandango_id"]].loc[row["theater_id"]].loc[row["show_datetime"]]["showtime_id"]
        except KeyError:
            log.debug("error in finding showtime_id, most likely an outdated showtime. recording and skipping")
            log.debug(row)
            return -1
    df_to_db["showtime_id"] = df_to_db.loc[:,:].apply(lambda row: get_showtime_id(row), axis=1)
    df_to_db = df_to_db[["fandango_id", "showtime_id", "buy_datetime", "buy_datetime_utc"]]
    try:
        df_to_db = pd.concat([last_update, last_update, df_to_db]).drop_duplicates(["fandango_id", "showtime_id", "buy_datetime_utc", "buy_datetime"], keep=False)
    except TypeError:
        print("df_to_db", df_to_db.dtypes, "last update", last_update.dtypes)
        raise

    df_to_db = df_to_db.sort_values(["buy_datetime_utc", "fandango_id"])
    try:
        df_to_db.to_sql(name="transactions",
                        schema="presales",
                        con=engine,
                        index=False,
                        if_exists="append",
                        dtype={"fandango_id": Integer(),
                               "showtime_id": BigInteger(),
                               "buy_datetime": DateTime(),
                               "buy_datetime_utc": DateTime()})
    except TypeError:
        log.error("TypeError in transactions insertion, something's wrong")


    log.warning("Updated ticket table. Added {} new tickets.".format(len(df_to_db.index)))
    # set subraction of processed_tickets - locked_tickets
    processed_tickets = pd.concat([processed_tickets, locked_tickets, locked_tickets], ignore_index=True, sort=True).drop_duplicates(["buy_datetime_utc", "show_datetime", "fandango_id", "theater_id"], keep=False)
    del old_theaters, old_movies, old_showtimes, theaters, movies, showtimes, df_to_db

db_string = "postgres://postgres@localhost/fml"
engine = create_engine(db_string)

log.warning("db_string: " + db_string)
scheduler = BlockingScheduler()
scheduler.add_job(get_tickets, 'cron', second="1,11-59/5")
scheduler.add_job(remove_duplicates, 'cron', second="2,32")
scheduler.add_job(insert_into_database, 'cron', second="3", minute="0,10,20,30,40,50")

log.info("Starting scheduler...")
scheduler.start()
