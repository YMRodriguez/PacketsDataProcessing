import json
import os
import pandas as pd
import random
import pathlib
from generator import generator, getRelevantStats, getPartition, assignIDs
from datetime import datetime
# -------------- Generic functions --------------------------------


def assignFragility(data, fragileWords, where="description"):
    data["fragility"] = data.apply(lambda x: 1 if any(list(
        map(lambda y: y in fragileWords, x[where].split(" ")))) else 0, axis=1)
    return data


def volumeProcessor(data):
    data["volume"] = data.apply(
        lambda x: round((x["length"]*x["width"]*x["height"])/1000000, 5), axis=1)
    return data


def cilindricalToBox(data):
    """
    This method models a cilidrical packaging to a box.

    Args:
        data ([type]): data to provide a box packaging.
    """
    data.loc[data.rounded == 1, ['height', 'width']] = data.diameter
    return data.drop(columns=["diameter"])


def createRandomFeasibleOrientations(data, constrained=True, special=False, orientations=None):
    """
    Adds rotated and random feasible orientations to an item.

    Args:
        data ([type]): dataframe row.
    """
    if orientations is None:
        if not special:
            orientations = [1, 2, 3, 4, 5, 6]
        else:
            orientations = [3, 4, 5, 6]
    if random.getrandbits(1) and constrained:
        # Get next or previous depending on the scheme of rotations.
        currentRotated = data["or"] + 1 if data["or"] % 2 else data["or"] - 1
        # Get a random orientation from the rest of available orientations.
        randomOrientation = random.sample(
            [i for i in orientations if i not in [data["or"], currentRotated]], k=1)
        randomOrientationRotated = [randomOrientation[0] + 1] if randomOrientation[0] % 2 else [
            randomOrientation[0] - 1]
        feasibleOrientations = [data["or"], currentRotated] + \
            randomOrientation + randomOrientationRotated
        return sorted(feasibleOrientations)
    else:
        return orientations


def determineCurrentOrientation(data):
    """
    Determine the current orientation provided by the marketplace, based on some predefined orientations.

    Args:
        data ([type]): dataframe row.
    """
    dimensionsOrdered = sorted([data.width, data.height,
                                data.length], reverse=True)
    if [data.width, data.length, data.height] == dimensionsOrdered:
        data["or"] = 1
    elif [data.length, data.width, data.height] == dimensionsOrdered:
        data["or"] = 2
    elif [data.width, data.height, data.length] == dimensionsOrdered:
        data["or"] = 3
    elif [data.length, data.height, data.width] == dimensionsOrdered:
        data["or"] = 4
    elif [data.height, data.width, data.length] == dimensionsOrdered:
        data["or"] = 5
    else:
        data["or"] = 6
    return data


# ------ Ikea data manipulation ----------------------------------------------------
ikeaPath = os.path.dirname(__file__) + os.path.sep + 'ikeaData' + os.path.sep


def ikeaAdaptation():
    ikeaData = pd.read_json(ikeaPath + 'data.json')

    # Give fragility based on description and process the data.
    fragileWordsIkea = ["glass", "Glass", "Mirror",
                        "mirror", "lamp", "bulb", "LED", "Vase", "Tealight"]
    ikeaData = volumeProcessor(cilindricalToBox(assignFragility(
        assignIDs(ikeaData), fragileWordsIkea))).apply(lambda x: determineCurrentOrientation(x), 1)
    with open(ikeaPath + os.path.sep + 'ikea-noOrientationConstraints-noDst.json', 'w+') as f:
        ikeaData["f_or"] = ikeaData.apply(
            lambda x: createRandomFeasibleOrientations(x, constrained=False), 1)
        json.dump(ikeaData.to_dict(orient="records"),
                  f, indent=2, ensure_ascii=False)
    with open(ikeaPath + os.path.sep + 'ikea-orientationConstraints-noDst.json', 'w+') as f:
        ikeaData["f_or"] = ikeaData.apply(
            lambda x: createRandomFeasibleOrientations(x), 1)
        json.dump(ikeaData.to_dict(orient="records"),
                  f, indent=2, ensure_ascii=False)


# -------------- Mediamarkt data manipulation -------------------------------
mmPath = os.path.dirname(__file__) + os.path.sep + \
    'mediamarktData' + os.path.sep


def mediamarktAdaptation():
    mmData = pd.read_json(mmPath + 'data.json')

    # Bit of cleaning.
    mmData["description"] = mmData.apply(lambda x: x["description"].strip(), 1)
    mmData["name"] = mmData.apply(
        lambda x: x["name"].replace('&quot;', '')[:-6].strip(), 1)

    # Give fragility based on description and process the data.
    fragileWordsMediamarkt = ["Monitores",
                              "Figuras", "Iluminación inteligente", "TV"]
    mmData = volumeProcessor(assignFragility(assignIDs(mmData), fragileWordsMediamarkt, "name")).apply(
        lambda x: determineCurrentOrientation(x), 1)
    mmData.loc[(mmData["description"].str.contains("TV") | mmData["description"].str.contains("Monitores")) & ~(mmData["description"].str.contains(
        "Series") | mmData["description"].str.contains("Antena")) & ((mmData["or"] == 1) | (mmData["or"] == 2)), "or"] = 3
    mmData = mmData[mmData["description"] != "Juguetes sexuales"]
    with open(mmPath + os.path.sep + 'mm-orientationConstraints-noDst.json', 'w+') as f:
        def checkSpecial(description):
            special = any(list(map(lambda x: x in description, ["TV", "Monitores"]))) and all(
                list(map(lambda x: x not in description, ["Antena", "Series"])))
            return special
        mmData["f_or"] = mmData.apply(
            lambda x: createRandomFeasibleOrientations(x, special=checkSpecial(x["description"])), 1)
        json.dump(mmData.to_dict(orient="records"),
                  f, indent=2, ensure_ascii=False)
    with open(mmPath + os.path.sep + 'mm-noOrientationConstraints-noDst.json', 'w+') as f:
        mmData["f_or"] = mmData.apply(
            lambda x: createRandomFeasibleOrientations(x, constrained=False), 1)
        json.dump(mmData.to_dict(orient="records"),
                  f, indent=2, ensure_ascii=False)


# -------------- Mixed data ----------------------------------------------------
pathlib.Path(os.path.dirname(__file__) + os.path.sep +
             'mixedData').mkdir(parents=True, exist_ok=True)
mixedPath = os.path.dirname(__file__) + os.path.sep + \
    'mixedData' + os.path.sep


def mixedDataAdaptation():
    def cleanDensityMistakes(data):
        data["density"] = data["weight"]/data["volume"]
        data = data[data["density"] < 100].drop(columns=["density"])
        return data
    with open(mixedPath + 'data-noOrientationConstraints-noDst.json', 'w+') as f:
        mixedDataNoOrient = pd.concat([pd.read_json(
            mmPath + 'mm-noOrientationConstraints-noDst.json'), pd.read_json(ikeaPath + 'ikea-noOrientationConstraints-noDst.json')])
        mixedDataNoOrient["weight"] = mixedDataNoOrient.apply(
            lambda x: round(x["weight"], 3), 1)
        # Drop ridiculous dimensions items
        mixedDataNoOrient = mixedDataNoOrient[(mixedDataNoOrient["length"] >= 10) & (
            mixedDataNoOrient["width"] >= 10) & (mixedDataNoOrient["height"] >= 10) & (mixedDataNoOrient["volume"] < 15) & (mixedDataNoOrient["weight"] > 0.01)]
        mixedDataNoOrient = cleanDensityMistakes(mixedDataNoOrient)
        json.dump(assignIDs(mixedDataNoOrient.drop(columns=["id"]).reset_index(drop=True)).to_dict(orient="records"),
                  f, indent=2, ensure_ascii=False)
    with open(mixedPath + 'data-orientationConstraints-noDst.json', 'w+') as f:
        mixedDataOrient = pd.concat([pd.read_json(
            mmPath + 'mm-orientationConstraints-noDst.json'), pd.read_json(ikeaPath + 'ikea-orientationConstraints-noDst.json')])
        mixedDataOrient["weight"] = mixedDataOrient.apply(
            lambda x: round(x["weight"], 3), 1)
        # Drop ridiculous dimensions items
        mixedDataOrient = mixedDataOrient[(mixedDataOrient["length"] >= 10) & (
            mixedDataOrient["width"] >= 10) & (mixedDataOrient["height"] >= 10) & (mixedDataOrient["volume"] < 15) & (mixedDataOrient["weight"] > 0.01)]
        mixedDataOrient = cleanDensityMistakes(mixedDataOrient)
        json.dump(assignIDs(mixedDataOrient.drop(columns=["id"]).reset_index(drop=True)).to_dict(orient="records"),
                  f, indent=2, ensure_ascii=False)


# -------------- Scenarios dataset ------------
pathlib.Path(os.path.dirname(__file__) + os.path.sep +
             'scenarios').mkdir(parents=True, exist_ok=True)


def datasetDescription(dataset, ID):
    """
    Generates stats on given dataset.

    Args:
        dataset ([type]): [description]
        ID ([type]): identification of the dataset (timestamp).

    Returns:
        [type]: object containing relevant descritive data on the dataset.
    """

    return {"ID": ID,
            "packets_product": len(dataset),
            "orders_subgroups": dataset.groupby(["subgroupId"]).ngroups,
            "unique_dim": dataset.groupby(["dimensionUnique"]).ngroups,
            "unique_dim_weight": dataset.groupby(["dimensionUnique", "weight"]).ngroups,
            "max_dim": dataset[["width", "height", "length"]].max().max(), "min_dim": dataset[["width", "height", "length"]].min().min(),
            "max_w": dataset.weight.max(), "min_w": dataset.weight.min(),
            "w_mean": round(dataset.weight.mean(), 2), "w_median": round(dataset.weight.median(), 2),
            "w_std": round(dataset.weight.std(), 2), "t_weight": round(dataset.weight.sum(), 2),
            "max_v": dataset.volume.max(), "min_v": dataset.volume.min(),
            "v_mean": round(dataset.volume.mean(), 2), "v_median": round(dataset.volume.median(), 2),
            "v_std": round(dataset.volume.std(), 2), "t_vol": round(dataset.volume.sum(), 2),
            "n_dst": dataset.dstCode.unique().shape[0],
            "n_prio": dataset[dataset["priority"] == 1].shape[0],
            "n_frag": dataset[dataset["fragility"] == 1].shape[0],
            "n_adr": dataset[dataset["ADR"] == 1].shape[0]
            }


def scenarioGeneration(nDestinations, volumeOffset=1.2, adrDist=None, priorityDist=None, fragility=True, minVol=0.001, option=0, containerVolume=81.6):
    mdPath = mixedPath + 'data-orientationConstraints-noDst.json'
    mmdPath = mmPath + 'data-orientationConstraints-noDst.json'
    idPath = ikeaPath + 'data-orientationConstraints-noDst.json'
    paths = [mdPath, mmdPath, idPath]
    actualPath = paths[option]
    # Get the data with the specified path.
    with open(actualPath, 'r') as f:
        data = json.load(f)
    referenceData = pd.DataFrame(data)
    newData = generator(referenceData, nDestinations,
                        adrDist, priorityDist, fragility, minVol, minWeight=0.1)

    # volRatio is specially interesting to know how many packets volume/combinations has the experiment.
    # It is obvious that with a large ratio the algoritm may achieve better results because it allows to have more combinations
    # However, in real examples this may not be true, that's the importance of this parameter.
    partition, volRatio = getPartition(
        newData, volume=containerVolume, volumeOffset=volumeOffset, do=True)
    nPackets, nOrders, destinations, uniqueDim, ADRcount, priorityCount, fragilityCount, minVol = getRelevantStats(
        partition)
    filename = datetime.now().strftime('%d%H%M%S') + '-' + str(nPackets) + '-' + str(nOrders) + '-' + str(uniqueDim) + '-' + str(volRatio) + '-' + str(destinations) + \
        '-' + str(ADRcount) + '-' + str(priorityCount) + \
        '-' + str(fragilityCount) + '-' + str(round(minVol, 5)) + '-md-'
    # Dataset directory contains the dataset itself.
    filenameDataset = filename + '.json'
    # Description directory contains relevant data of the dataset for its use in tables and graphs.
    fId = filename.split("-")[0]
    filenameDescription = fId + '.json'
    scenariosPath = os.path.dirname(
        __file__) + os.path.sep + 'scenarios' + os.path.sep
    with open(scenariosPath + os.path.sep + 'datasets' + os.path.sep + filenameDataset, 'w+') as f:
        json.dump(partition.drop(columns=["f_or", "dimensionUnique"]).to_dict(orient="records"),
                  f, indent=2, ensure_ascii=False)
    with open(scenariosPath + os.path.sep + 'description' + os.path.sep + filenameDescription, 'w+') as f:
        description = datasetDescription(partition, fId)
        json.dump(description, f, indent=2, ensure_ascii=False)


scenarioGeneration(nDestinations=1, adrDist=[1, 0], priorityDist=[
                   1, 0], fragility=True, minVol=0.001, option=0, containerVolume=81.6)
