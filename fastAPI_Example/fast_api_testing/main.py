import random
from string import hexdigits

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    hex_chars = "".join(random.choices(hexdigits.lower(), k=6))
    hex_color = f"#{0x6A8DB8:06x}"
    context = {
        "color": hex_color,
    }
    return templates.TemplateResponse(
        request=request, name="color.html", context=context
    )

#to run, type "fastapi dev" in terminal and visit website: http://127.0.0.1:8000