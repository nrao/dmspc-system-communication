from string import Template
from flask import Flask, request, redirect
from confluent_kafka import Producer, Consumer


app = Flask(__name__)

TOPIC = "topic_0"

app.config["consumer"] = None
app.config["messages"]= [] 


def read_config(filename):
    config  ={ }
    with open(filename) as f :
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()

    return config

def get_consumer():
    if app.config["consumer"] is None: 
        config = read_config("consumer.properties")
        config["group.id"] = "demo-group"
        config["auto.offset.reset"] = "latest"
        config["enable.metrics.push"] = False
        app.config["consumer"] = Consumer(config)
        app.config["consumer"].subscribe([TOPIC])

    return app.config["consumer"]


def render(filename, **kwargs):
    with open(f"templates/{filename}") as f : 
        return Template(f.read()).substitute(**kwargs)
    

@app.route("/")
def index():
    return redirect("/producer")


@app.route("/producer")
def producer():
    sent = request.args.get("sent", "")
    error = request.args.get("error", "")

    if sent : 
        status = f"<p>Sent: <strong>{sent}</strong></p>"
    elif error:
        status = f"<p>Error: {error}</p>"
    else:
        status = ""

    return render("producer.html", status=status)

@app.route("/send", methods = ["POST"])
def send():
    obs_id = request.form.get("obs_id", "").strip()
    if not obs_id:
        return redirect ("/producer?error=Obervation+ID+is+required")
    
    config = read_config("producer.properties")
    config["enable.metrics.push"] = False
    producer = Producer(config)
    producer.produce(TOPIC, key = obs_id, value=obs_id)
    producer.flush()

    return redirect(f"/producer?sent={obs_id}")


@app.route("/consumer")
def consumer_page():
    consumer = get_consumer()
    for _ in range(10):
        msg = consumer.poll(0.1)
        if msg and not msg.error():
            app.config["messages"].append(msg.value().decode("utf-8"))


    if app.config["messages"]:
        items = "".join(f"<li>{m}</li>" for m in app.config["messages"])
        messages = f"<ul>{items}</ul>"
    else:
        messages = "<p>No messages yet.</p>"

    return render("consumer.html", messages = messages)
    

if __name__ == "__main__":
    app.run(debug=True)
    






