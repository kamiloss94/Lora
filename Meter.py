import DBconnection as DB
import query as Q
import pandas as pd
import psycopg2
from datetime import datetime


def postgresql_to_dataframe(conn,select_query, column_names):
    """
    Tranform a SELECT query into a pandas dataframe
    """
    cursor = DB.conn.cursor()
    try:
        cursor.execute(Q.select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    # Naturally we get a list of tupples
    tupples = cursor.fetchall()
    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    cursor.close()
    return df


class Meter:
    def __init__(self, SN):
        self.SN = SN

    def getdatarawbytes(SN):
        dff = postgresql_to_dataframe(conn = DB.conn, select_query= Q.select_query, column_names= Q.column_names)
        rawbytes = dff['data'].where(dff['device_name'] == SN)  # pandas dataframe (with index 0)
        rawbyteslist = rawbytes.loc[0]  # list of bytes converted from pandas dataframe
        # stringlist = [x.decode('utf-8') for x in rawbyteslist]
        # strData = codecs.decode(rawbytes.loc[0], 'UTF-8')
        return rawbyteslist

    def payloadtolist(rawbyteslist):
        n = 2
        payloadtolist = [rawbyteslist[i:i + n] for i in range(0, len(rawbyteslist), n)]
        return payloadtolist

    def getmeterunixtime(payloadtolist):
        time = payloadtolist[4] + payloadtolist[3] + payloadtolist[2] + payloadtolist[1]
        # print(DT.datetime.utcfromtimestamp(float(int(str(time), 16)) / 16 ** 4))
        #print(hex(int(time.mktime(time.strptime(time, '%Y-%m-%d %H:%M:%S')))))
        timed = int(time, base= 16)
        datetime_object = datetime.fromtimestamp(timed).strftime('%d-%m-%y  %H:%M:%S')
        return datetime_object
    def getlastvolume(payloadtolist):
        volume1 = payloadtolist[19] + payloadtolist[18] + payloadtolist[17] + payloadtolist[16]
        volume1dec = (int(volume1, base = 16)) / 1000
        return volume1dec