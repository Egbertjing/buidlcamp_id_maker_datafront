import uvicorn

if __name__ == '__main__':
    uvicorn.run("data_service:app",
        host="0.0.0.0",
        port=3609,
        reload=False,
        forwarded_allow_ips ='*',
        ssl_keyfile="./furion.key",
        ssl_certfile="./furion.pem"
    )
