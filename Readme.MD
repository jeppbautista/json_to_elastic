# JSON to Elasticsearch
----------------------------

A simple script that inserts JSON data to Elasticsearch index. 

### How to run
```
python main.py --index test_elastic_index --json_file test_elastic.json --from_elastic True
```

### Command-line arguments
Here are the following arguments:
1. `--host` : Host server of the destination elasticsearch
**default: localhost**
2. `--port` : Port of the destination elasticsearch
**default: 9200**
3. `--index` : Name of the index where the data will be created. If the index exists the user will be prompted if the index will be recreated.
![screenshot](https://raw.githubusercontent.com/slapadasbas/json_to_elastic/master/Capture.PNG)
**default: test**
4. `--json_file` : Path of the json file input. 
**This argument is required**

5. `--from_elastic` : Set to `True` if JSON file is from Elasticsearch query.
**default: False**

