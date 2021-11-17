import random


def generator(data, nDestinations, adrDist=None, priorityDist=None, fragility=True):
    """
    Create dataset with desired conditions.

    Args:
        data ([df]): dataframe itself.
        nDestinations ([int]): number of destinations used to stablish dst_codes.
        adrDist ([sequence]): distribution for the packets selected as ADR. [a, b] where a non-ADR and b ADR.
        priorityDist ([sequence]): distribution for the packets selected as prioritary. [a, b] where a non-prioritary weight and b prioritary weight.
        fragility ([Bool], optional): Set it to false in case you want a relaxation of the condition in the whole dataset. Defaults to True.
    """
    if priorityDist is None:
        priorityDist = [1, 0]
    if adrDist is None:
        adrDist = [1, 0]

    # Assign a dst_code to each packet, keeping in mind that all the packets inside the same subgroupId should go in the same container.
    destinations = list(range(nDestinations))
    data["dst_code"] = data.groupby("subgroupId")["name"].transform(
        lambda x: random.choice(destinations))
    data["priority"] = data.groupby("subgroupId")["name"].transform(
        lambda x: random.choices([0, 1], priorityDist)[0])
    data["ADR"] = data.groupby("subgroupId")["name"].transform(
        lambda x: random.choices([0, 1], adrDist)[0])
    # Fragility should not be modified, but for the relaxation scenario we can modify it.
    if not fragility:
        data["fragility"].values[:] = 0
    return data


def getRelevantStats(data):
    """
    This function get relevant stats like number of unique dimensions or destinations.

    Args:
        data ([type]): [description]
    """
    data = dataFeasibleOrientationsTupleSerializer(data)
    destinations = data["dst_code"].nunique()
    uniqueDim = data.groupby(["width", "height", "length"]).ngroups
    ADRcount = len(data[data["ADR"] == 1].reset_index().index)
    priorityCount = len(data[data["priority"] == 1].reset_index().index)
    fragilityCount = len(data[data["fragility"] == 1].reset_index().index)
    print(uniqueDim)


def dataFeasibleOrientationsTupleSerializer(data):
    data["f_or"] = data.apply(lambda x: tuple(x["f_or"]), 1)
    return data
