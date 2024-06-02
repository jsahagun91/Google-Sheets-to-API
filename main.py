from utils.google_sheets import get_worksheet_as_dataframe
from fastapi import FastAPI
import uvicorn
from fastapi.templating import Jinja2Templates
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from great_tables import GT

app = FastAPI()
templates = Jinja2Templates(directory="utils")

# Full sheet url with parameters, be sure #gid=[ID] is present
# e.g. https://docs.google.com/spreadsheets/d/[ID]/edit#gid=[GID]

WORKSHEET_URL = "https://docs.google.com/spreadsheets/d/1SHLkaI73Dvk8h6gAqHfuqthVm8htBXVZ-rnQ2tfHvWA/edit#gid=2044031673"

DF = get_worksheet_as_dataframe(spreadsheet_url=WORKSHEET_URL,
                                require_auth=False,
                                skip_rows=0,
                                skip_cols=0,
                                has_header=True)

TITLE = "U.S. National Parks 🌲"

INDEX_COL = "name"

# New FastAPI app
app = FastAPI()


@app.get("/", response_class=HTMLResponse)
async def main():

  gt_tbl = GT(DF, rowname_col=INDEX_COL).tab_header(title=TITLE)

  return gt_tbl.as_raw_html(make_page=True)


uvicorn.run(app, port=8080, host="0.0.0.0")