import json
import os
import pandas as pd
import random
from copy import deepcopy
import numpy as np

# -------------- Generic functions --------------------------------


def assignIDs(data):
    """
    This function's objetive is to normalize the ID for any kind of data that could enter.
    Args:
        data ([df]): data to provide a normalized ID.
    """
    data["id"] = data.index
    return data


def assignFragility(data, fragileWords, where="description"):
    data["fragility"] = data.apply(lambda x: 1 if any(list(
        map(lambda y: y in fragileWords, x[where].split(" ")))) else 0, axis=1)
    return data


def volumeProcessor(data):
    data["volume"] = data.apply(
        lambda x: (x["length"]*x["width"]*x["height"])/100000, axis=1)
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
        return feasibleOrientations
    else:
        return [1, 2, 3, 4, 5, 6]


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
ikeaData = pd.read_json(ikeaPath + 'data.json')

# Give fragility based on description and process the data.
fragileWordsIkea = ["glass", "Glass", "Mirror",
                    "mirror", "lamp", "bulb", "LED", "Vase", "Tealight"]
ikeaData = volumeProcessor(cilindricalToBox(assignFragility(
    assignIDs(ikeaData), fragileWordsIkea))).apply(lambda x: determineCurrentOrientation(x), 1)
with open(ikeaPath + os.path.sep + 'ikeaData-noOrientationConstraints-noDst.json', 'w+') as f:
    ikeaData["f_or"] = ikeaData.apply(
        lambda x: createRandomFeasibleOrientations(x, constrained=False), 1)
    json.dump(ikeaData.to_json(orient="records"),
              f, indent=2, ensure_ascii=False)
with open(ikeaPath + os.path.sep + 'ikeaData-orientationConstraints-noDst.json', 'w+') as f:
    ikeaData["f_or"] = ikeaData.apply(
        lambda x: createRandomFeasibleOrientations(x), 1)
    json.dump(ikeaData.to_json(orient="records"),
              f, indent=2, ensure_ascii=False)


# -------------- Mediamarkt data manipulation -------------------------------
mmPath = os.path.dirname(__file__) + os.path.sep + \
    'mediamarktData' + os.path.sep
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
mmData.loc[(mmData["name"].str.contains("TV")) & (
    mmData["description"].str.contains("TV")) & (mmData["or"] == 1), "or"] = 3

with open(mmPath + os.path.sep + 'mediamarktData-noOrientationConstraints-noDst.json', 'w+') as f:
    mmData["f_or"] = mmData.apply(
        lambda x: createRandomFeasibleOrientations(x, special=("TV" in x["name"])), 1)
    json.dump(mmData.to_json(orient="records"),
              f, indent=2, ensure_ascii=False)
with open(mmPath + os.path.sep + 'mediamarktData-orientationConstraints-noDst.json', 'w+') as f:
    mmData["f_or"] = mmData.apply(
        lambda x: createRandomFeasibleOrientations(x), 1)
    json.dump(mmData.to_json(orient="records"),
              f, indent=2, ensure_ascii=False)
