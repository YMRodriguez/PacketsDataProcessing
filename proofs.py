import pandas as pd
import os
import json

scenariosPath = os.path.dirname(
    __file__) + os.path.sep + 'scenarios' + os.path.sep

filename = '27114338-265-157-205-1.4-5-0-132-14.json'
with open(scenariosPath + os.path.sep + filename, 'r') as f:
    data = json.load(f)

print(type(data))
