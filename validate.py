#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 17.3.2022

@author: Jaakko Ahola
@company:  Virnex Group Oy
"""
import os
import sys
import time
from datetime import datetime

sys.path.append(os.environ["LESMAINSCRIPTS"])
from Data import Data

from ConnectSnowFlake import ConnectSnowflake


def main():

    connection = ConnectSnowflake("tutorial")
    cursor = connection.get_cursor()
    setup = ["USE WAREHOUSE COMPUTE_WH",
             "USE DATABASE CITIBIKE",
             "USE SCHEMA PUBLIC"
             ]
    try:
        for setup_statement in setup:
            cursor.execute(setup_statement)

        cursor.execute("""SELECT datediff('day', STARTTIME, STOPTIME) AS DAYDIFF,
                       COUNT(DAYDIFF) FROM TRIPS GROUP BY 1 ORDER BY 1 LIMIT 1""")
        one_row = cursor.fetchone()
        day_length_zero = one_row[1]
        cursor.execute("SELECT count(*) from TRIPS")
        number_of_rows = cursor.fetchone()[0]
        print(f"""Number of rows: {number_of_rows}
Number of trips where trip duration is less than 1 day: {day_length_zero}
Percentage of trips where trip duration is less than 1 day: {day_length_zero/number_of_rows*100:.3´f}%
""")

    finally:
        connection.close_cursor()
    connection.close_ctx()


if __name__ == "__main__":
    start = time.time()
    now = datetime.now().strftime('%d.%m.%Y %H.%M')
    print(f"Script started {now}.")
    main()
    end = time.time()
    now = datetime.now().strftime('%d.%m.%Y %H.%M')
    print(f"Script completed {now} in {Data.timeDuration(end - start)}")
