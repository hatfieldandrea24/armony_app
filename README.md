# Armony Shovels AI

Streamlit app to browse DuckDB/MotherDuck views, preview rows, and make quick charts (line, bar, scatter). It also includes ad‑hoc SQL, contractor insights, and a lead finder built on the MCP reporting data.

## Prerequisites
- Python 3.10+ installed
- Optional: a DuckDB database file or a MotherDuck connection string/DSN

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Run the app
```bash
streamlit run app.py
```
Streamlit will print a local URL (usually http://localhost:8501) to open in your browser.

## Run with Docker
Build from the `streamlit_app` directory:

```bash
docker build -t armony-streamlit .
```

Run with MotherDuck token + DSN passed as environment variables:

```bash
docker run --rm -p 8501:10000 \
  -e MOTHERDUCK_TOKEN=your_token_here \
  -e MOTHERDUCK_DSN='md:?motherduck_token=your_token_here' \
  armony-streamlit
```

Then open `http://localhost:8501`.

If you want to use a local DuckDB file from your machine:

```bash
docker run --rm -p 8501:10000 \
  -v "$(pwd):/app/data" \
  -e MOTHERDUCK_DSN='/app/data/my.duckdb' \
  armony-streamlit
```

## Configure the connection
- The text box defaults to `my.duckdb`; change it to your DuckDB file path, `:memory:` for an in‑memory session, or a MotherDuck DSN such as `md:my_db`.
- For MotherDuck, set `MOTHERDUCK_TOKEN` in your environment before running Streamlit:
  ```bash
  export MOTHERDUCK_TOKEN=your_token_here
  streamlit run app.py
  ```
  You can also pass the token inline: `md:?motherduck_token=…`.
  
The app shows a “Connection details” expander with the current database and attached databases (`pragma database_list`) so you can verify you’re pointed at the right warehouse.

## Using the UI
1) **Browse Views**: pick a view, set a row limit, preview data, and choose a chart type (line by index vs numeric, bar counts by category, scatter numeric vs numeric).  
2) **Custom Query**: paste any SQL; a limit slider keeps result size manageable.
3) **Permits by Jurisdiction**: runs the preset filtered query you provided. Pick the table/view (or type it manually), optionally choose the date column and start date, then click “Run permit summary” to see the table plus three charts (total by jurisdiction, stacked by type per jurisdiction, totals by type).

## Notes
- Identifiers are quoted for safety when selecting views.  
- If you point at a `.session.sql` file exported from MotherDuck, create a real DuckDB database first (e.g., `duckdb my.duckdb` then `attach 'md:your_db' as md; create view ...`).  
- Cached connections/data reduce repeated round trips; restart Streamlit if you change the underlying schema.
