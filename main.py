
import Meter as M
import query as Q
from fastapi import FastAPI
import uvicorn

payload = M.Meter.getdatarawbytes(Q.serialnumber)
payloadtolist = M.Meter.payloadtolist(payload)
app = FastAPI()
lastvolume = M.Meter.getlastvolume(payloadtolist)

lastmontflags = M.parsebinaryflag(M.Meter.getlastmonthfag(payloadtolist))
thismonthflags = M.parsebinaryflag(M.Meter.getthismonthflag(payloadtolist))
currentflags = M.parsebinaryflag(M.Meter.getcurrentflag(payloadtolist))
allserialnumbers = M.getallserialnumbers()

@app.get("/")
async def read_root():
    return {"Available serial numbers": f"{allserialnumbers}"}

@app.get(f"/{Q.serialnumber}")
async def read_root():
    return {"Last Volume": f"{lastvolume} m3",
            "Time":f"{M.Meter.getmeterunixtime(payloadtolist)}",
            "Last month flag": f"{lastmontflags}",
            "This month flag": f"{thismonthflags}",
            "Current flag": f"{currentflags}",}

if __name__ == "__main__":

    print(payload)
    print(M.Meter.payloadtolist(payload))
    print(M.Meter.getmeterunixtime(payloadtolist))
    print(f"Last Volume log {lastvolume} m3")
    print(f"Last month flags: {lastmontflags}")
    print(f"This month flags: {thismonthflags}")
    print(f"Current flags: {currentflags}")
    print(M.getallserialnumbers())

    uvicorn.run(app, host="0.0.0.0", port=8000)