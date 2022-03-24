import os
import glob
import json


def getFilepaths():
    return glob.glob(os.path.dirname(
        __file__) + os.path.sep + "scenarios" + os.path.sep + "description" + os.path.sep + "round2" + os.path.sep + "*.json")


for i in getFilepaths():
    print(i)
    with open(i, "r+") as f:
        data = json.load(f)
        data["perc_mult"] = round(
            data["n_mult_item_sub"]/data["packets_product"], 2)
        data["perc_prio"] = round(data["n_prio"]/data["packets_product"], 2)
        data["perc_unique"] = round(
            data["unique_dim"]/data["packets_product"], 2)
    with open(i, "w+") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
