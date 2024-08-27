# Configuration

This Repl is entirely pre-configured and ready for you to get started! If you need authentication for your sheet (private, sensitive data) jump to Advanced, otherwise, you can start with Simple.

## Setup

This template fetches a Google Sheet, then uses FastAPI to render it as an HTML response. You can see what this looks like for U.S. National Parks by clicking "Run".

### Simple (no authentication)

`main.py` is the entrypoint for our app. Replace `WORKSHEET_URL` with the URL of your worksheet. If your worksheet is public, set `require_auth=False` and hit run! 

Note: a "worksheet" refers to a "tab" on a Google Sheet. If you only have one tab, you can just copy the URL, if not, you can click the proper tab to get it's url.

You should see your sheet data appear in the console as a dataframe. If you have sensitive data in your sheet, you'll need to enable the Google Sheets API. 

### Advanced (authentication required)

1. Enable the "Drive" and "Spreadsheets" APIs. This can be done by following the instructions [here](https://docs.gspread.org/en/v6.0.0/oauth2.html#enable-api-access-for-a-project).
2. Create a service account. That process is simple and can be done [here](https://docs.gspread.org/en/v6.0.0/oauth2.html#for-bots-using-service-account).
3. Open the `json` file you downloaded as a part of step 2.
4. Copy and paste _the entirety_ of it's contents into the Secret `SERVICE_ACCOUNT_JSON` variable in Replit.
5. "Share" your Google Sheet with the `client_email` in your service account json
6. Run the app!

## Operation

The `get_worksheet_as_dataframe` function takes the following parameters:

- `spreadsheet_url`: A string representing the full URL of the Google Sheets document. It must include the gid parameter to specify which worksheet/tab to access.
- `require_auth`: A boolean to indicate whether authentication is required to access the sheet. Set to True for private sheets that require Google Sheets API access through a service account. Set to False for public sheets that can be accessed directly.
- `has_header`: A boolean indicating if the first row after any skipped rows should be used as the header (column names) for the DataFrame.
- `skip_rows`: An integer specifying how many rows from the start of the sheet should be skipped before starting to read data into the DataFrame.
- `skip_cols`: An integer specifying how many columns from the left of the sheet should be skipped before starting to read data into the DataFrame.
These parameters allow for flexible data extraction, accommodating various data arrangements and access controls in Google Sheets.

```python
get_worksheet_as_dataframe(
    spreadsheet_url=WORKSHEET_URL,
    # bypass auth (only valid for public sheets)
    require_auth=False,
    skip_rows=0,
    skip_cols=0,
    has_header=True)
```

## More

- Follow me on [X](https://x.com/mattppal) or [LinkedIn](https://www.linkedin.com/in/matt-palmer/) for more great templates
- Check out https://replit.com/templates or https://replit.com/guides for the latest and greatest!# Google-Sheets-to-API
# Google-Sheets-to-API
