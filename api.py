from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class Item(BaseModel):
    name: str
    price: float
    is_offer: bool = None


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    return {"item_id": item_id, "q": q}

@app.get("/predict")
def predict(sentence: str):
    # 需要和此前逻辑一样初始化模型
    seg_sentence = list(jieba.cut(sentence))
    # 模型输入多个 sample，但我们此处只用一个，所以要取出
    results = model.predict([seg_sentence])
    return {"label": results[0]}


@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    return {"item_name": item.name, "item_id": item_id}