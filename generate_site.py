from __future__ import annotations

import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DOCS_DIR = BASE_DIR / "docs"

POEM_PATH = DATA_DIR / "weather_poem.txt"
DB_PATH = DATA_DIR / "weather.db"
OUTPUT_HTML = DOCS_DIR / "index.html"


def escape_html(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def load_poem() -> str:
    if not POEM_PATH.exists():
        return "No poem found yet."
    return POEM_PATH.read_text(encoding="utf-8").strip()


def load_weather_rows() -> list[dict]:
    if not DB_PATH.exists():
        return []

    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row

    try:
        target_date_row = connection.execute(
            """
            SELECT DATE(MAX(forecast_datetime)) AS target_date
            FROM forecasts
            """
        ).fetchone()

        if not target_date_row or not target_date_row["target_date"]:
            return []

        target_date = target_date_row["target_date"]

        rows = connection.execute(
            """
            SELECT
                location_name,
                forecast_datetime,
                temperature_2m,
                cloud_cover,
                relative_humidity_2m
            FROM forecasts
            WHERE DATE(forecast_datetime) = ?
            ORDER BY location_name, forecast_datetime
            """,
            (target_date,),
        ).fetchall()

        return [dict(row) for row in rows]
    finally:
        connection.close()

def build_weather_html(rows: list[dict]) -> str:
    if not rows:
        return """
        <section class="card">
          <h2>Weather forecast</h2>
          <p>No weather data found yet.</p>
        </section>
        """

    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["location_name"], []).append(row)

    parts = [
        """
        <section class="card">
          <h2>Tomorrow's Weather Forecast</h2>
        """
    ]

    for location_name, location_rows in grouped.items():
        parts.append(f"<h3>{escape_html(location_name)}</h3>")
        parts.append(
            """
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Temperature (°C)</th>
                    <th>Cloud Cover (%)</th>
                    <th>Humidity (%)</th>
                  </tr>
                </thead>
                <tbody>
            """
        )

        for row in location_rows:
            dt_text = escape_html(row["forecast_datetime"])
            temp = escape_html(row["temperature_2m"])
            cloud = escape_html(row["cloud_cover"])
            humidity = escape_html(row["relative_humidity_2m"])

            parts.append(
                f"""
                <tr>
                  <td>{dt_text}</td>
                  <td>{temp}</td>
                  <td>{cloud}</td>
                  <td>{humidity}</td>
                </tr>
                """
            )

        parts.append(
            """
                </tbody>
              </table>
            </div>
            """
        )

    parts.append("</section>")
    return "\n".join(parts)


def generate_html(poem: str, weather_rows: list[dict]) -> str:
    poem_html = "<br>".join(escape_html(poem).splitlines())
    weather_html = build_weather_html(weather_rows)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Poem and Weather</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      background: #f6f7fb;
      color: #222;
      margin: 0;
      padding: 0;
    }}

    .container {{
      max-width: 1000px;
      margin: 0 auto;
      padding: 40px 20px;
    }}

    h1 {{
      color: #4b4f8a;
      margin-bottom: 10px;
    }}

    .subtitle {{
      color: #666;
      margin-bottom: 30px;
    }}

    .card {{
      background: white;
      border-radius: 12px;
      padding: 24px;
      margin-bottom: 20px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }}

    .poem {{
      line-height: 1.8;
      font-size: 1.1rem;
    }}

    h2 {{
      margin-top: 0;
      color: #4b4f8a;
    }}

    h3 {{
      margin-top: 24px;
      color: #333;
    }}

    .table-wrap {{
      overflow-x: auto;
      margin-top: 10px;
      margin-bottom: 20px;
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
    }}

    th, td {{
      padding: 10px 12px;
      border-bottom: 1px solid #ddd;
      text-align: left;
      font-size: 0.95rem;
    }}

    th {{
      background: #f0f2fa;
    }}
  </style>
</head>
<body>
  <div class="container">
    <h1>Poem and Weather Report</h1>
    <p class="subtitle">Automatically published with GitHub Pages.</p>

    <section class="card">
      <h2>Generated Poem</h2>
      <div class="poem">{poem_html}</div>
    </section>

    {weather_html}
  </div>
</body>
</html>
"""


def main() -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    poem = load_poem()
    weather_rows = load_weather_rows()

    html = generate_html(poem, weather_rows)
    OUTPUT_HTML.write_text(html, encoding="utf-8")

    print(f"Site written to: {OUTPUT_HTML}")


if __name__ == "__main__":
    main()