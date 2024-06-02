import json
import os
from urllib.parse import parse_qs, urljoin, urlparse

import gspread
import pandas as pd
from google.oauth2 import service_account


def validate_url(url: str):
  parsed = urlparse(url, allow_fragments=True)
  if (parsed.scheme != "https" \
      or not parsed.netloc.endswith("google.com")
      or not parsed.path.startswith("/spreadsheets/d/")
      or not parsed.path.endswith("/edit")
     ):

    raise ValueError("Invalid Google Sheets URL format")
  if "gid" not in parsed.fragment:
    raise ValueError("gid parameter missing in the URL")
  elif not parse_qs(parsed.fragment).get('gid'):
    raise ValueError("gid parameter missing in the URL")


def get_service_account() -> service_account.Credentials:
  scopes = [
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/drive',
  ]

  return service_account.Credentials.from_service_account_info(json.loads(
      os.environ['SERVICE_ACCOUNT_JSON']),
                                                               scopes=scopes)


def load_worksheet_from_url(spreadsheet_url: str,
                            worksheed_gid: str) -> pd.DataFrame:
  url = urljoin(spreadsheet_url, f'export?gid={worksheed_gid}&format=csv')

  return pd.read_csv(url, header=None)


def load_worksheet_from_api(spreadsheet_url: str,
                            worksheed_gid: str) -> pd.DataFrame:
  service_account = get_service_account()
  gc = gspread.authorize(service_account)

  sheet = gc.open_by_url(spreadsheet_url)
  worksheet = sheet.get_worksheet_by_id(worksheed_gid)

  data = worksheet.get_all_values()
  return pd.DataFrame(data)


def trim_dataframe(df: pd.DataFrame, skip_cols: int, skip_rows: int,
                   has_header: bool):
  if len(df) < skip_rows:
    raise ValueError(
        "skip_rows is greater than the number of rows in the dataframe.")
  if len(df.columns) < skip_cols:
    raise ValueError(
        "skip_cols is greater than the number of columns in the dataframe.")
  try:
    df = df.iloc[skip_rows:, skip_cols:]

    if has_header:
      df.columns = df.iloc[0]
      df = df[1:]

  except Exception as e:
    raise ValueError(
        f"Please check your skip_rows, skip_cols, & has_header values: {e}")
  return df


def get_worksheet_gid(spreadsheet_url: str) -> str:
  parsed = urlparse(spreadsheet_url, allow_fragments=True)

  return parse_qs(parsed.fragment)['gid'][0]


def get_worksheet_as_dataframe(spreadsheet_url: str,
                               require_auth: bool = True,
                               has_header: bool = True,
                               skip_rows: int = 0,
                               skip_cols: int = 0) -> pd.DataFrame:

  validate_url(spreadsheet_url)

  worksheed_gid = get_worksheet_gid(spreadsheet_url)

  if not require_auth:
    df = load_worksheet_from_url(spreadsheet_url, worksheed_gid)
  else:
    df = load_worksheet_from_api(spreadsheet_url, worksheed_gid)

  return trim_dataframe(df=df,
                        skip_cols=skip_cols,
                        skip_rows=skip_rows,
                        has_header=has_header)
