# Python Client

This project contains a Python 3 application that subscribes to a topic on a Confluent Cloud Kafka cluster and sends a sample message, then consumes it and prints the consumed record to the console.

## Prerequisites

We assume that you already have Python 3 installed. The template was last tested against Python 3.12.5.

The instructions use `virtualenv` but you may use other virtual environment managers like `venv` if you prefer.


## Installation

Create and activate a Python environment, so that you have an isolated workspace:

```shell
virtualenv env
source env/bin/activate
```

Install the dependencies of this application:

```shell
pip3 install -r requirements.txt
```

## Usage

You can execute the consumer script by running:

```shell
python3 consumer.py
```

And you can execute the producer script by running:

```shell
python3 producer.py
```

## Troubleshooting

### Running `pip3 install -r requirements.txt` fails

If the execution of `pip3 install -r requirements.txt` fails with an error message indicating that librdkafka cannot be
found, please check if you are using a Python version for which a
[built distribution](https://pypi.org/project/confluent-kafka/2.3.0/#files) of `confluent-kafka` is available.


## Learn more

- For the Python client API, check out the [kafka-clients documentation](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- Check out the full [getting started tutorial](https://developer.confluent.io/get-started/python/)
