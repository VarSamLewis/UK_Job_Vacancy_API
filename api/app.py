from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

app = FastAPI()

class Job(BaseModel):
    id: int
    title: str
    location: str

@app.get("/jobs", response_model=List[Job])
def get_jobs():
    jobs_data = [{"id": 1, "title": "Test Job", "location": "London"}]
    return jobs_data
