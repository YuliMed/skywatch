import os
import uuid
import json
import pika
from flask import Flask, render_template, request

app = Flask(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "skywatch-rabbitmq")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "user")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "password")
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "15"))

DEFAULT_CITY_A = "Tel Aviv"
DEFAULT_CITY_B = "London"


def get_weather(city: str) -> dict:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials,
        connection_attempts=3,
        retry_delay=2,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    result_queue = channel.queue_declare(queue="", exclusive=True, auto_delete=True)
    callback_queue = result_queue.method.queue
    correlation_id = str(uuid.uuid4())
    response = {}

    def on_response(ch, method, props, body):
        if props.correlation_id == correlation_id:
            response["data"] = json.loads(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=callback_queue, on_message_callback=on_response)
    channel.basic_publish(
        exchange="",
        routing_key="weather_requests",
        properties=pika.BasicProperties(
            reply_to=callback_queue,
            correlation_id=correlation_id,
            delivery_mode=1,
        ),
        body=city.encode(),
    )

    connection.process_data_events(time_limit=REQUEST_TIMEOUT)
    connection.close()
    return response.get("data", {"error": f"Timeout: no response in {REQUEST_TIMEOUT}s"})


@app.route("/", methods=["GET", "POST"])
def index():
    city_a = DEFAULT_CITY_A
    city_b = DEFAULT_CITY_B
    weather_a = None
    weather_b = None
    error = None
    submitted = False

    if request.method == "POST":
        submitted = True
        city_a = request.form.get("city_a", "").strip() or DEFAULT_CITY_A
        city_b = request.form.get("city_b", "").strip() or DEFAULT_CITY_B
        try:
            weather_a = get_weather(city_a)
            weather_b = get_weather(city_b)
            errors = []
            if weather_a and "error" in weather_a:
                errors.append(f"{city_a}: {weather_a['error']}")
            if weather_b and "error" in weather_b:
                errors.append(f"{city_b}: {weather_b['error']}")
            if errors:
                error = " · ".join(errors)
        except Exception as exc:
            error = f"Connection error: {exc}"

    return render_template(
        "index.html",
        city_a=city_a,
        city_b=city_b,
        weather_a=weather_a,
        weather_b=weather_b,
        error=error,
        submitted=submitted,
    )


@app.route("/healthz")
def healthz():
    return {"status": "ok", "app": "raincheck"}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
