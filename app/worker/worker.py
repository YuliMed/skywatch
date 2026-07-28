import os
import json
import time
import pika
import requests
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "skywatch-rabbitmq")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "user")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "password")

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

# Normalize common spellings + optional ISO country hint for ambiguous names
CITY_ALIASES = {
    "kiev": ("Kyiv", "UA"),
    "kyiv": ("Kyiv", "UA"),
    "tel aviv": ("Tel Aviv", "IL"),
    "tel-aviv": ("Tel Aviv", "IL"),
    "bangkok": ("Bangkok", "TH"),
    "london": ("London", "GB"),
    "moscow": ("Moscow", "RU"),
    "new york": ("New York", "US"),
    "nyc": ("New York", "US"),
}

COUNTRY_NAMES = {
    "IL": "Israel",
    "UA": "Ukraine",
    "GB": "United Kingdom",
    "TH": "Thailand",
    "RU": "Russia",
    "US": "United States",
    "FR": "France",
    "DE": "Germany",
    "ES": "Spain",
    "IT": "Italy",
    "JP": "Japan",
    "CN": "China",
    "IN": "India",
    "BR": "Brazil",
    "AU": "Australia",
    "CA": "Canada",
    "NL": "Netherlands",
    "PL": "Poland",
    "TR": "Turkey",
    "EG": "Egypt",
    "GR": "Greece",
    "AE": "United Arab Emirates",
}

WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Icy fog", 51: "Light drizzle", 53: "Moderate drizzle",
    55: "Dense drizzle", 61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Heavy thunderstorm",
}


def country_flag(country_code: str) -> str:
    """ISO 3166-1 alpha-2 → flag emoji."""
    if not country_code or len(country_code) != 2:
        return "🌍"
    code = country_code.upper()
    if not code.isalpha():
        return "🌍"
    return chr(0x1F1E6 + ord(code[0]) - ord("A")) + chr(0x1F1E6 + ord(code[1]) - ord("A"))


def country_display_name(country_code: str, fallback: str) -> str:
    if country_code:
        return COUNTRY_NAMES.get(country_code.upper(), fallback or country_code.upper())
    return fallback or "Unknown"


def resolve_city_query(city: str) -> tuple[str, str | None]:
    key = city.strip().lower()
    if key in CITY_ALIASES:
        name, code = CITY_ALIASES[key]
        return name, code
    return city.strip(), None


def pick_best_result(results: list, preferred_country: str | None) -> dict:
    if preferred_country:
        preferred = preferred_country.upper()
        in_country = [r for r in results if r.get("country_code", "").upper() == preferred]
        if in_country:
            return max(in_country, key=lambda r: r.get("population") or 0)
    return max(results, key=lambda r: r.get("population") or 0)


def get_coordinates(city: str):
    query, country_hint = resolve_city_query(city)
    params = {"name": query, "count": 10, "language": "en", "format": "json"}
    if country_hint:
        params["countryCode"] = country_hint

    resp = requests.get(GEOCODING_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results") or []
    if not results:
        raise ValueError(f"City '{city}' not found")

    r = pick_best_result(results, country_hint)
    country_code = (r.get("country_code") or "").upper()
    country_name = country_display_name(country_code, r.get("country", ""))
    return (
        r["latitude"],
        r["longitude"],
        r["name"],
        country_name,
        country_code,
    )


def get_weather_data(lat: float, lon: float) -> dict:
    resp = requests.get(
        WEATHER_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "current_weather": "true",
            "hourly": "relativehumidity_2m,apparent_temperature",
            "timezone": "auto",
            "forecast_days": 1,
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def process_request(ch, method, props, body):
    city = body.decode("utf-8").strip()
    logger.info("Processing request for: %s", city)
    try:
        lat, lon, name, country, country_code = get_coordinates(city)
        data = get_weather_data(lat, lon)
        current = data["current_weather"]
        code = int(current["weathercode"])
        result = {
            "city": name,
            "country": country,
            "country_code": country_code,
            "flag": country_flag(country_code),
            "latitude": round(lat, 4),
            "longitude": round(lon, 4),
            "temperature": current["temperature"],
            "windspeed": current["windspeed"],
            "weathercode": code,
            "condition": WMO_CODES.get(code, "Unknown"),
            "is_day": bool(current.get("is_day", 1)),
            "time": current["time"],
        }
        logger.info(
            "Success: %s, %s → %.1f°C, %s",
            name, country, result["temperature"], result["condition"],
        )
    except Exception as exc:
        logger.error("Error for '%s': %s", city, exc)
        result = {"error": str(exc)}

    if props.reply_to:
        ch.basic_publish(
            exchange="",
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
            body=json.dumps(result),
        )
    ch.basic_ack(delivery_tag=method.delivery_tag)


def connect_with_retry():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )
    while True:
        try:
            connection = pika.BlockingConnection(params)
            logger.info("Connected to RabbitMQ at %s", RABBITMQ_HOST)
            return connection
        except Exception as exc:
            logger.warning("RabbitMQ not ready (%s), retrying in 5s...", exc)
            time.sleep(5)


def main():
    while True:
        try:
            connection = connect_with_retry()
            channel = connection.channel()
            channel.queue_declare(queue="weather_requests", durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="weather_requests", on_message_callback=process_request)
            logger.info("Worker ready, waiting for requests...")
            channel.start_consuming()
        except Exception as exc:
            logger.error("Connection lost: %s. Reconnecting...", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
