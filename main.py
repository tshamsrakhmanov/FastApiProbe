from fastapi import FastAPI
from pydantic import BaseModel


class Item(BaseModel):
    name: str
    price: float
    brand: str = None


app = FastAPI()

template_db = {1: {"name": "first", "type": "first name"}, 2: {"name": "second", "type": "second name"}}


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/about")
async def about():
    return {"message": "about"}


@app.get("/get-item/{item_id}")
async def get_item(item_id: int):
    return template_db[item_id]


@app.get("/get-by-name/{item_id}")
async def get_item(item_id: int, name: str = None):
    for item_id in template_db:
        if template_db[item_id]["name"] == name:
            return template_db[item_id]
    return {"Data": "Not found"}


@app.post("/create-item")
def create_item(item: Item):
    return {}
