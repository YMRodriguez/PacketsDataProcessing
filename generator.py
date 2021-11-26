import random
import pandas as pd


def assignIDs(data):
    """
    This function's objetive is to normalize the ID for any kind of data that could enter.
    Args:
        data ([df]): data to provide a normalized ID.
    """
    data["id"] = data.index
    return data


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
    data["dstCode"] = data.groupby("subgroupId")["id"].transform(
        lambda x: random.choice(destinations))
    data["priority"] = data.groupby("subgroupId")["id"].transform(
        lambda x: random.choices([0, 1], priorityDist)[0])
    data["ADR"] = data.groupby("subgroupId")["id"].transform(
        lambda x: random.choices([0, 1], adrDist)[0])
    # Fragility should not be modified, but for the relaxation scenario we can modify it.
    if not fragility:
        data["fragility"].values[:] = 0
    return data


def getPartition(data, volume, volumeOffset=1.5, do=True):
    if do:
        subgroupAndTotalVolumeDf = data.groupby(
            "subgroupId")["volume"].sum().reset_index()
        meanSubgroupsEstimation = round(
            (volume*volumeOffset)/subgroupAndTotalVolumeDf["volume"].mean())
        randomSubgroupsIds = subgroupAndTotalVolumeDf["subgroupId"].sample(
            n=meanSubgroupsEstimation)
        partition = data[data["subgroupId"].isin(randomSubgroupsIds)]
        return assignIDs(partition.drop(columns=["id"]).reset_index()), round(partition["volume"].sum()/(volume*volumeOffset), 1)
    else:
        return data, round(data["volume"].sum()/(volume*volumeOffset), 1)


def getRelevantStats(data):
    """
    This function gets relevant stats like number of unique dimensions or destinations.

    Args:
        data ([type]): dataFrame
    """
    data = dataFeasibleOrientationsTupleSerializer(data)
    destinations = data["dstCode"].nunique()
    data["dimensionUnique"] = data.apply(lambda x: tuple(
        sorted([x["width"], x["height"], x["length"]], reverse=True)), 1)
    uniqueDim = data.groupby(["dimensionUnique"]).ngroups
    ADRcount = data[data["ADR"] == 1].shape[0]
    priorityCount = data[data["priority"] == 1].shape[0]
    fragilityCount = data[data["fragility"] == 1].shape[0]
    nPackets = data.shape[0]
    nOrders = data.groupby(["subgroupId"]).ngroups
    return nPackets, nOrders, destinations, uniqueDim, ADRcount, priorityCount, fragilityCount


def dataFeasibleOrientationsTupleSerializer(data):
    data["feasibleOr"] = data.apply(lambda x: tuple(x["f_or"]), 1)
    return data.drop(columns=["f_or"])
