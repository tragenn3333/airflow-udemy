from collections import defaultdict
import csv
import json
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from urllib.request import Request, urlopen

from airflow.sdk import dag, task


@dag(
    dag_id="currency_exchange_etl",
    start_date=datetime(2025, 1, 1),
    schedule="30 3 * * *",  # Daily at 9:00 AM India time
    catchup=False,
    tags=["etl", "api", "currency"],
)
def currency_exchange_etl():

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def extract_exchange_rates():
        currencies = "INR,EUR,GBP,JPY,CAD,AUD,CHF"
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=30)

        api_url = (
            "https://api.frankfurter.dev/v2/rates"
            f"?base=USD&quotes={currencies}"
            f"&from={start_date}&to={end_date}"
        )

        request = Request(
            api_url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            },
        )

        with urlopen(request, timeout=120) as response:
            data = json.loads(response.read().decode("utf-8"))

        print(f"Extracted {len(data)} exchange-rate records")
        return data

    @task
    def transform_rates(raw_rates):
        cleaned_rates = []

        for item in raw_rates:
            cleaned_rates.append(
                {
                    "date": item["date"],
                    "currency": item["quote"],
                    "usd_rate": round(float(item["rate"]), 4),
                }
            )

        cleaned_rates.sort(key=lambda item: (item["date"], item["currency"]))
        print(f"Transformed {len(cleaned_rates)} records")
        return cleaned_rates

    @task
    def analyze_rates(cleaned_rates):
        grouped_rates = defaultdict(list)

        for item in cleaned_rates:
            grouped_rates[item["currency"]].append(item)

        summary = []

        for currency, records in sorted(grouped_rates.items()):
            records.sort(key=lambda item: item["date"])

            first_rate = records[0]["usd_rate"]
            latest_rate = records[-1]["usd_rate"]
            percentage_change = round(
                ((latest_rate - first_rate) / first_rate) * 100,
                2,
            )

            summary.append(
                {
                    "currency": currency,
                    "first_rate": first_rate,
                    "latest_rate": latest_rate,
                    "percentage_change": percentage_change,
                }
            )

        print("30-day currency analysis:")
        for item in summary:
            print(item)

        return summary

    @task
    def save_analysis(cleaned_rates, summary):
        csv_path = Path("/opt/airflow/logs/currency_analysis.csv")
        html_path = Path("/opt/airflow/logs/currency_analysis.html")

        with csv_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=["date", "currency", "usd_rate"],
            )
            writer.writeheader()
            writer.writerows(cleaned_rates)

        grouped_rates = defaultdict(list)
        for item in cleaned_rates:
            grouped_rates[item["currency"]].append(item)

        all_dates = sorted({item["date"] for item in cleaned_rates})
        date_positions = {
            date: index for index, date in enumerate(all_dates)
        }

        normalized_rates = {}
        all_normalized_values = []

        for currency, records in grouped_rates.items():
            records.sort(key=lambda item: item["date"])
            first_rate = records[0]["usd_rate"]

            normalized_records = []
            for item in records:
                normalized_value = (item["usd_rate"] / first_rate) * 100

                normalized_records.append(
                    {
                        "date": item["date"],
                        "value": normalized_value,
                    }
                )

                all_normalized_values.append(normalized_value)

            normalized_rates[currency] = normalized_records

        min_value = min(all_normalized_values)
        max_value = max(all_normalized_values)

        if min_value == max_value:
            max_value += 1

        svg_width = 950
        svg_height = 500
        left_margin = 75
        right_margin = 35
        top_margin = 50
        bottom_margin = 85
        chart_width = svg_width - left_margin - right_margin
        chart_height = svg_height - top_margin - bottom_margin

        colors = [
            "#2563eb",
            "#dc2626",
            "#16a34a",
            "#9333ea",
            "#ea580c",
            "#0891b2",
            "#ca8a04",
        ]

        series_svg = []
        legend_html = []

        for index, currency in enumerate(sorted(normalized_rates)):
            color = colors[index % len(colors)]
            points = []

            for item in normalized_rates[currency]:
                x_position = left_margin + (
                    date_positions[item["date"]] / max(len(all_dates) - 1, 1)
                ) * chart_width

                y_position = top_margin + (
                    1 - ((item["value"] - min_value) / (max_value - min_value))
                ) * chart_height

                points.append(f"{x_position:.1f},{y_position:.1f}")

            series_svg.append(
                f'<polyline points="{" ".join(points)}" '
                f'fill="none" stroke="{color}" stroke-width="3" />'
            )

            legend_html.append(
                f'<span style="color:{color}; font-weight:bold;">'
                f"● {escape(currency)}</span>"
            )

        summary_rows = []
        for item in summary:
            summary_rows.append(
                f"""
                <tr>
                    <td>{escape(item["currency"])}</td>
                    <td>{item["first_rate"]}</td>
                    <td>{item["latest_rate"]}</td>
                    <td>{item["percentage_change"]}%</td>
                </tr>
                """
            )

        html_content = f"""
        <html>
        <head>
            <title>30-Day Currency Analysis</title>
        </head>
        <body style="font-family: Arial; margin: 40px;">
            <h1>30-Day Currency Exchange Analysis</h1>
            <p>Base currency: 1 USD | Each currency begins at index value 100.</p>

            <div style="margin-bottom: 15px;">
                {" &nbsp; ".join(legend_html)}
            </div>

            <svg width="{svg_width}" height="{svg_height}">
                <line x1="{left_margin}" y1="{top_margin + chart_height}"
                      x2="{left_margin + chart_width}"
                      y2="{top_margin + chart_height}"
                      stroke="black" />

                <line x1="{left_margin}" y1="{top_margin}"
                      x2="{left_margin}"
                      y2="{top_margin + chart_height}"
                      stroke="black" />

                <text x="10" y="{top_margin + 10}" font-size="14">
                    Indexed rate
                </text>

                <text x="{left_margin}" y="{svg_height - 25}" font-size="13">
                    {all_dates[0]}
                </text>

                <text x="{left_margin + chart_width - 85}"
                      y="{svg_height - 25}" font-size="13">
                    {all_dates[-1]}
                </text>

                {"".join(series_svg)}
            </svg>

            <h2>30-Day Change Summary</h2>

            <table border="1" cellpadding="8" cellspacing="0">
                <tr>
                    <th>Currency</th>
                    <th>First rate</th>
                    <th>Latest rate</th>
                    <th>30-day change</th>
                </tr>
                {"".join(summary_rows)}
            </table>
        </body>
        </html>
        """

        html_path.write_text(html_content, encoding="utf-8")

        print(f"CSV saved at: {csv_path}")
        print(f"Chart saved at: {html_path}")

    raw_rates = extract_exchange_rates()
    cleaned_rates = transform_rates(raw_rates)
    summary = analyze_rates(cleaned_rates)
    save_analysis(cleaned_rates, summary)


currency_exchange_etl()