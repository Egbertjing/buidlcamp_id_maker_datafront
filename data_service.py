
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI

from utils import constants as CT
from buidlcamp_info import buidlcamp_data as BD
from starlette.requests import Request
app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/ip")
async def root(request: Request):
    result={
        "ip":request.client.host,
        "x-real-ip":request.headers.get("X-Real-Ip",""),
        "x-forwarded-for":request.headers.get("x-forwarded-for","")
    }
    return result 

@app.get('/')
# def index(request: Request):
#     client_host_ip = request.client.host
#     return {"ip": client_host_ip}
def index():
    return {'message': 'Welcome to Buidl Camp'}

@app.get('/budlecamp_get')
async def get_budlecamp(request: Request, database=CT.DATABASE, user_name=None):
    try:
        final_result = BD.fetch_buidlcamp_info(database, user_name, request.client.host)
        return {
            'success': True,
            'data': final_result
        }
    except:
        return {
            'success': False,
            'data': []
        }
        
@app.post('/budlecamp_post')
def budlecamp_post(request: Request, database=CT.DATABASE, user_name=None):
    try:
        BD.into_buidlcamp_info(database, user_name, request.client.host)
        return {
            'success': True,
        }
    except:
        return {
            'success': False,
        }