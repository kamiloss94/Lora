import DBconnection as DB
import query as Q
import pandas as pd
import psycopg2
from datetime import datetime


def postgresql_to_dataframe(connect,query, column_names):
    """
    Convert SELECT query into a pandas dataframe
    """
    cursor = connect.cursor()
    try:
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    # get a list of tupples
    tupples = cursor.fetchall()
    # turn into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    cursor.close()
    return df

def parsebinaryflag(flag): #parsing binary flags to actual namelist
    flaglist = tuple(flag)
    namelist = ['dry', 'leak', 'burst', 'tamper', 'absence of flow', 'malfunctioning', 'radio issue', 'dupa blada']
    actualflag = []
    i = 0
    for n in flaglist:
        if n == '1':
            actualflag.append(namelist[i]) #append active flag to list
        i = i + 1
    return actualflag

def getallserialnumbers():
    dff = postgresql_to_dataframe(connect=DB.conn, query =Q.select_query_serial_numbers, column_names= ["device_name"])
    rawnumbers = dff['device_name'].loc[0]
    return rawnumbers

class Meter:
    def __init__(self, SN):
        self.SN = SN

    def getdatarawbytes(SN):
        dff = postgresql_to_dataframe(connect = DB.conn, query = Q.select_query, column_names= Q.column_names)
        rawbytes = dff['data'].where(dff['device_name'] == SN)  # pandas dataframe (with index 0)
        rawbyteslist = rawbytes.loc[0]  # list of bytes converted from pandas dataframe
        # stringlist = [x.decode('utf-8') for x in rawbyteslist]
        # strData = codecs.decode(rawbytes.loc[0], 'UTF-8')
        return rawbyteslist

    def payloadtolist(rawbyteslist):
        n = 2
        payloadtolist = [rawbyteslist[i:i + n] for i in range(0, len(rawbyteslist), n)]
        return payloadtolist

    def getmeterunixtime(payloadtolist):    #get last log time from payload
        time = payloadtolist[4] + payloadtolist[3] + payloadtolist[2] + payloadtolist[1]
        # print(DT.datetime.utcfromtimestamp(float(int(str(time), 16)) / 16 ** 4))
        #print(hex(int(time.mktime(time.strptime(time, '%Y-%m-%d %H:%M:%S')))))
        timed = int(time, base= 16)
        datetime_object = datetime.fromtimestamp(timed).strftime('%d-%m-%y  %H:%M:%S')
        return datetime_object
    def getlastvolume(payloadtolist):       #get last volume log from payload
        volume1 = payloadtolist[19] + payloadtolist[18] + payloadtolist[17] + payloadtolist[16]
        volume1dec = (int(volume1, base = 16)) / 1000
        return volume1dec
    def getlastmonthfag(payloadtolist):
        flagbyte = payloadtolist[13]
        flagbin = bin(int(flagbyte, 16))[2:].zfill(8)
        return flagbin
    def getthismonthflag(payloadtolist):
        flagbyte = payloadtolist[14]
        flagbin = bin(int(flagbyte, 16))[2:].zfill(8)
        return flagbin
    def getcurrentflag(payloadtolist):
        flagbyte = payloadtolist[15]
        flagbin = bin(int(flagbyte, 16))[2:].zfill(8)
        return flagbin