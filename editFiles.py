import os
import glob
import json
from packetAdaptation import datasetDescription
import pandas as pd


def getFilepaths():
    return glob.glob(os.path.dirname(
        __file__) + os.path.sep + "scenarios" + os.path.sep + "datasets" + os.path.sep + "round3pso" + os.path.sep + "*.json")


# for i in getFilepaths():
#     print(i)
#     with open(i, "r+") as f:
#         data = json.load(f)
#         data["perc_mult"] = round(
#             data["n_mult_item_sub"]/data["packets_product"], 2)
#         data["perc_prio"] = round(data["n_prio"]/data["packets_product"], 2)
#         data["perc_unique"] = round(
#             data["unique_dim"]/data["packets_product"], 2)
#     with open(i, "w+") as f:
#         json.dump(data, f, indent=2, ensure_ascii=False)

scenariosPath = os.path.dirname(
    __file__) + os.path.sep + 'scenarios' + os.path.sep

filepaths = getFilepaths()

for i in filepaths:
    with open(i, "r+") as f:
        data = pd.DataFrame(json.load(f))
        cummulativeVolume = data["volume"].sum()
        meanVolume = data["volume"].mean()
        containerVolume = 81.6
        reducer = round((cummulativeVolume - containerVolume)/meanVolume) - 4
        partition = data[round(reducer/2):-round(reducer/2)]
    # Creo nuevo filepath para guardar el nuevo dataset
    with open(scenariosPath + os.path.sep + 'datasets' + os.path.sep + 'round3pso' + os.path.sep + 'RPSO' + os.path.basename(i), "w+") as f:
        json.dump(partition.to_dict(orient="records"),
                  f, indent=2, ensure_ascii=False)

    fId = os.path.basename(i).split("-")[0]
    filenameDescription = 'R' + fId + '.json'

    # Creo nuevo filepath para guardar el nuevo dataset
    with open(scenariosPath + 'description' + os.path.sep + "round3" + os.path.sep + filenameDescription, "w+") as f:
        partition["dimensionUnique"] = partition.apply(lambda x: tuple(
            sorted([x["width"], x["height"], x["length"]], reverse=True)), 1)
        description = datasetDescription(partition, fId)
        # Separe extension from filepath string
        json.dump(description, f, indent=2, ensure_ascii=False)
