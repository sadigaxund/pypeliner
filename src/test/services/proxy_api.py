from fastapi import FastAPI

# Create an instance of FastAPI
app = FastAPI()

ENDPOINTS = {
    'priceline' : {
        'reviews': {},
        'listings': {},
    }
}

@app.get("/query")
def root():
    return ENDPOINTS

@app.get("/query/{source}")
def query(source: str):
    return {"Requested Source": ENDPOINTS.get(source, "Does not exist!")}

@app.get("/query/{source}/{endpoint}")
def query(source: str, endpoint: str):
    return {"Requested Source": f"{source}@{endpoint}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
