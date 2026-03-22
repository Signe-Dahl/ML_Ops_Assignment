import os
import sqlite3
from pathlib import Path

from groq import Groq


DB_PATH = Path(__file__).resolve().parent / "data" / "weather.db"
MODEL_NAME = "llama-3.3-70b-versatile"


def fetch_tomorrow_weather_summary(db_path: Path) -> str:
    """Read tomorrow's forecast from SQLite and build a compact summary for Groq."""
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row

    try:
        query = """
        SELECT
            location_name,
            DATE(forecast_datetime) AS forecast_date,
            ROUND(AVG(temperature_2m), 1) AS avg_temp,
            ROUND(MIN(temperature_2m), 1) AS min_temp,
            ROUND(MAX(temperature_2m), 1) AS max_temp,
            ROUND(AVG(cloud_cover), 1) AS avg_cloud_cover,
            ROUND(AVG(relative_humidity_2m), 1) AS avg_humidity
        FROM forecasts
        WHERE DATE(forecast_datetime) = (
            SELECT MIN(DATE(forecast_datetime)) FROM forecasts
        )
        GROUP BY location_name, DATE(forecast_datetime)
        ORDER BY location_name
        """
        rows = connection.execute(query).fetchall()
    finally:
        connection.close()

    if not rows:
        raise ValueError("No forecast data found in the database.")

    forecast_date = rows[0]["forecast_date"]

    lines = [f"Forecast date: {forecast_date}"]
    for row in rows:
        lines.append(
            f"- {row['location_name']}: "
            f"avg temp {row['avg_temp']}°C, "
            f"min {row['min_temp']}°C, "
            f"max {row['max_temp']}°C, "
            f"avg cloud cover {row['avg_cloud_cover']}%, "
            f"avg humidity {row['avg_humidity']}%"
        )

    return "\n".join(lines)


def generate_bilingual_poem(weather_summary: str) -> str:
    """Generate a short bilingual poem with Groq."""
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise EnvironmentError("GROQ_API_KEY is not set.")

    client = Groq(api_key=api_key)

    prompt = f"""
You are writing a short weather poem in TWO languages: English and Danish.

Use the forecast summary below:
{weather_summary}

Requirements:
- Compare the weather in the three locations.
- Describe the differences clearly but poetically.
- Suggest which location would be nicest to be in tomorrow.
- Write first in English, then in Danish.
- Keep it short: 4 lines total for each language.
- Mention the recommended location in both languages.
- Do not add explanations before or after the poem.

Output format:
English:
[line 1]
[line 2]
[line 3]
[line 4]

Dansk:
[line 1]
[line 2]
[line 3]
[line 4]
"""

    completion = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {
                "role": "system",
                "content": "You are a concise bilingual poet who writes clearly and beautifully."
            },
            {
                "role": "user",
                "content": prompt
            },
        ],
        temperature=0.8,
    )

    return completion.choices[0].message.content.strip()


def save_poem(poem: str) -> Path:
    """Save the poem to a text file."""
    output_path = Path(__file__).resolve().parent / "data" / "weather_poem.txt"
    output_path.parent.mkdir(exist_ok=True)
    output_path.write_text(poem, encoding="utf-8")
    return output_path


def main() -> None:
    weather_summary = fetch_tomorrow_weather_summary(DB_PATH)
    poem = generate_bilingual_poem(weather_summary)
    output_path = save_poem(poem)

    print("\nGenerated poem:\n")
    print(poem)
    print(f"\nSaved poem to: {output_path}")


if __name__ == "__main__":
    main()