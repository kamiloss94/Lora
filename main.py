
import Meter as M
import query as Q
from fastapi import FastAPI
import uvicorn
import DBconnection as DB

payload = M.Meter.getdatarawbytes(Q.serialnumber)
payloadtolist = M.Meter.payloadtolist(payload)
app = FastAPI()
lastvolume = M.Meter.getlastvolume(payloadtolist)

@app.get(f"/{Q.serialnumber}")
async def read_root():
    return {"Last Volume": f"{lastvolume} m3 || time {M.Meter.getmeterunixtime(payloadtolist)}"}

if __name__ == "__main__":

#bytes = df[['data']]
#print(df[['device_name','data']])

#ulsn = Meter.SerialNumber(SN = df[["device_name"]])
#print(df.at[30,'data']


    print(payload)
    print(M.Meter.payloadtolist(payload))
    print(M.Meter.getmeterunixtime(payloadtolist))
    print(f"Last Volume log {M.Meter.getlastvolume(payloadtolist)} m3")

    uvicorn.run(app, host="0.0.0.0", port=8000)