import csv
import logging
import pymysql

logging.basicConfig(level=logging.INFO)
MYSQL_HOST = 'localhost'
MYSQL_USER = 'zac'
MYSQL_PASSWORD = 'imadfs618'
MYSQL_DATABASE = 'air_data'

type_dict = {
    "master.csv": {
        "Year": int,
        "Quarter": int,
        "Month": int,
        "DayofMonth": int,
        "DayOfWeek": int,
        "FlightDate": str,
        "Reporting_Airline": str,
        "DOT_ID_Reporting_Airline": str,
        "IATA_CODE_Reporting_Airline": str,
        "Tail_Number": str,
        "Flight_Number_Reporting_Airline": str,
        "OriginAirportID": str,
        "OriginAirportSeqID": str,
        "OriginCityMarketID": str,
        "Origin": str,
        "OriginCityName": str,
        "OriginState": str,
        "OriginStateFips": str,
        "OriginStateName": str,
        "OriginWac": str,
        "DestAirportID": str,
        "DestAirportSeqID": str,
        "DestCityMarketID": str,
        "Dest": str,
        "DestCityName": str,
        "DestState": str,
        "DestStateFips": str,
        "DestStateName": str,
        "DestWac": str,
        "CRSDepTime": str,
        "DepTime": str,
        "DepDelay": float,
        "DepDelayMinutes": float,
        "DepDel15": bool,
        "DepartureDelayGroups": str,
        "DepTimeBlk": str,
        "TaxiOut": float,
        "WheelsOff": str,
        "WheelsOn": str,
        "TaxiIn": float,
        "CRSArrTime": str,
        "ArrTime": str,
        "ArrDelay": float,
        "ArrDelayMinutes": float,
        "ArrDel15": str,
        "ArrivalDelayGroups": str,
        "ArrTimeBlk": str,
        "Cancelled": bool,
        "CancellationCode": str,
        "Diverted": bool,
        "CRSElapsedTime": float,
        "ActualElapsedTime": float,
        "AirTime": str,
        "Flights": float,
        "Distance": str,
        "DistanceGroup": str,
        "CarrierDelay": float,
        "WeatherDelay": float,
        "NASDelay": float,
        "SecurityDelay": float,
        "LateAircraftDelay": float,
        "FirstDepTime": str,
        "TotalAddGTime": str,
        "LongestAddGTime": str,
        "DivAirportLandings": str,
        "DivReachedDest": str,
        "DivActualElapsedTime": float,
        "DivArrDelay": float,
        "DivDistance": str,
        "Div1Airport": str,
        "Div1AirportID": str,
        "Div1AirportSeqID": str,
        "Div1WheelsOn": str,
        "Div1TotalGTime": str,
        "Div1LongestGTime": str,
        "Div1WheelsOff": str,
        "Div1TailNum": str,
        "Div2Airport": str,
        "Div2AirportID": str,
        "Div2AirportSeqID": str,
        "Div2WheelsOn": str,
        "Div2TotalGTime": str,
        "Div2LongestGTime": str,
        "Div2WheelsOff": str,
        "Div2TailNum": str,
        "Div3Airport": str,
        "Div3AirportID": str,
        "Div3AirportSeqID": str,
        "Div3WheelsOn": str,
        "Div3TotalGTime": str,
        "Div3LongestGTime": str,
        "Div3WheelsOff": str,
        "Div3TailNum": str,
        "Div4Airport": str,
        "Div4AirportID": str,
        "Div4AirportSeqID": str,
        "Div4WheelsOn": str,
        "Div4TotalGTime": str,
        "Div4LongestGTime": str,
        "Div4WheelsOff": str,
        "Div4TailNum": str,
        "Div5Airport": str,
        "Div5AirportID": str,
        "Div5AirportSeqID": str,
        "Div5WheelsOn": int,
        "Div5TotalGTime": str,
        "Div5LongestGTime": str,
        "Div5WheelsOff": str,
        "Div5TailNum": str,
    },
    "LAIRPORT.csv": {"Code": str, "Description": str},
    "LAIRPORTID.csv": {"Code": str, "Description": str},
    "LCITYMARKETID.csv": {"Code": str, "Description": str},
    "LDIVERSIONS.csv": {"Code": str, "Description": str},
    "LSTATEABRAVIATION.csv": {"Code": str, "Description": str},
    "LSTATEABRAVIATION.csv": {"Code": str, "Description": str},
    "LUNIQUECARRIERS.csv": {"Code": str, "Description": str},
    "LWORLDAREACODES.csv": {"Code": str, "Description": str},
}

DB = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE)
CURSOR = DB.cursor()

for filename, schema in type_dict.items():
    table = filename.split('.')[0].lower()
    columns = ', '.join(list(schema.keys()))
    values_placeholders = ', '.join(['%s'] * len(list(schema.keys())))
    MYSQL_INSERT_STRING = f'INSERT INTO {table} ({columns}) VALUES ({values_placeholders})'
    print(MYSQL_INSERT_STRING)

    with open(filename, "r") as f:
        reader = csv.DictReader(f)
        counter = 0

        # Establish Mysql

        for row in reader:
            one_row = {}
            if counter % 1000 == 0:
                logging.info(f"counter: {counter}")
            counter += 1
            for key, value in row.items():
                if len(key) == 0:
                    continue
                try:
                    one_row[key] = schema[key](value)
                except:
                    if value is None or (isinstance(value, str) and len(value) == 0):
                        value = None
                        one_row[key] = None
                    else:
                        print(key, value, "crap")

            CURSOR.execute(MYSQL_INSERT_STRING, args=list(one_row.values()))
            if counter % 1000 == 0:
                DB.commit()
        DB.commit()
