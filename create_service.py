import uvicorn

if __name__ == '__main__':
    uvicorn.run("data_service:app",
        host="0.0.0.0",
        port=3609,
        reload=True,
        ssl_keyfile="./furion.key",
        ssl_certfile="./furion.pem",
        forwarded_allow_ips ='*'
    )
