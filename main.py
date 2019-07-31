import json
import requests
from pprint import pprint
import argparse


def is_connected(**kwargs):
	if requests.get("http://{}:{}".format(kwargs["host"], kwargs["port"])).status_code == 200:
		return True
	return False

def is_index_exists(**kwargs):
	if requests.get("http://{}:{}/{}".format(kwargs["host"], kwargs["port"], kwargs["index"])).status_code == 200:
		return True
	return False

def delete_ES(**kwargs):
	return requests.delete("http://{}:{}/{}".format(kwargs["host"], kwargs["port"], kwargs["index"]))

def create_ES(**kwargs):
	headers = {
		"Content-type" : "application/json"
	}
	return requests.put("http://{}:{}/{}".format(kwargs["host"], kwargs["port"], kwargs["index"]))

def insert_to_ES(json_data, **kwargs):
	headers = {
		"Content-type" : "application/json"
	}
	return requests.post("http://{}:{}/{}/doc/".format(kwargs["host"], kwargs["port"], kwargs["index"]), headers=headers, data=json.dumps(json_data))

def load_json(json_file):
	with open(json_file) as f:
		data = json.load(f)
	return data

def get_source(data):
	for key, value in data.items():
		if isinstance(value, dict):
			yield from get_source(value)
		if isinstance(value, list):
			yield value
		elif key == "_source":
			yield value

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--host", help="Host server of the destination elasticsearch", default="localhost")
	parser.add_argument("--port", help="Port of the destination elasticsearch", default="9200")
	parser.add_argument("--index", help="Name of the index to be created", default="test")
	parser.add_argument("--json_file", help="Path of the json file input", required=True)
	parser.add_argument("--from_elastic", help="Set to true if json is from Elasticsearch query", default=False)
	args = parser.parse_args()

	host, port, index, json_file, from_elastic = args.host, args.port, args.index, args.json_file, args.from_elastic
	connection = {"host": host, "port":port, "index":index}

	status = lambda x : "Successful" if x.status_code == 200 else "Insert Successful" if x.status_code == 201 else x

	if is_connected(**connection):
		if is_index_exists(**connection):

			print("{} exists in Elasticsearch server.".format(connection["index"]))
			choice = input("Would you like to delete? [y/n]")
			
			if choice.lower() == "y":
				print("Deleting {} ...".format(connection["index"]))
				print(status(delete_ES(**connection)))
			elif choice.lower() == "n":
				pass
			else:
				raise ValueError("Invalid choice")
		else:
			print("Creating {} ...".format(connection["index"]))
			print(status(create_ES(**connection)))

		data = load_json(json_file) 
		if args.from_elastic:
			_data = get_source(data)

		data = next(_data)

		print("Insering to {} ...".format(connection["index"]))
		for d in data:
			print(status(insert_to_ES(d["_source"], **connection)))

