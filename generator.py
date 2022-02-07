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


def generator(data, nDestinations, adrDist=None, priorityDist=None, fragility=True, minVol=0.001, minWeight=0.1, minDim=5):
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
    data = data[(data["volume"] >= minVol) & (data["weight"] >= minWeight)]
    data = data[(data["width"] >= minDim) & (
        data["height"] >= minDim) & (data["length"] >= minDim)]
    # Fragility should not be modified, but for the relaxation scenario we can modify it.
    if not fragility:
        data["fragility"].values[:] = 0
    return data


def getPartition(data, subgroupingDist, volume, volumeOffset=1.2, do=True):
    """
    Gets a partition of the passed data with given conditions.

    Args:
        data ([type]): dataset.
        subgroupingDist ([type]): percentages of [only item subgroup, multiple items in subgroup]
        volume ([type]): volume of the container.
        volumeOffset (float, optional): Offset to improve approximation on volume. Defaults to 1,2.
        do (bool, optional): If we actually want to do the partition. Defaults to True.

    Returns:
        [type]: [description]
    """
    if do:
        # Indicates whether it is a only item subgroup (true) or a multiple items subgroup (false).
        data["subg"] = data.apply(
            lambda x: x.subgroupId == x.productId, axis=1)
        subgroupAndTotalVolumeDf = data.groupby(
            ["subgroupId", "subg"])["volume"].sum().reset_index()
        # Get the ponderated volume means considering distribution.
        onlyItemsVolMean = subgroupAndTotalVolumeDf[subgroupAndTotalVolumeDf["subg"]]["volume"].mean(
        ) * subgroupingDist[0]
        multItemsVolMean = subgroupAndTotalVolumeDf[subgroupAndTotalVolumeDf["subg"]]["volume"].mean(
        ) * subgroupingDist[1]
        meanSubgroupsEstimation = round(
            (volume*volumeOffset)/(onlyItemsVolMean+multItemsVolMean))
        # Estimation of the distribution for only-item subgroups and item with several items.
        onlyItemSubgroupEstimation = round(
            subgroupingDist[0] * meanSubgroupsEstimation)
        multItemSubgroupEstimation = round(
            subgroupingDist[1] * meanSubgroupsEstimation)
        # Get the subgroups ids.
        onlyItemsSubgroupsId = subgroupAndTotalVolumeDf[subgroupAndTotalVolumeDf["subg"]]["subgroupId"].sample(
            n=onlyItemSubgroupEstimation)
        multItemSubgroupsId = subgroupAndTotalVolumeDf[~subgroupAndTotalVolumeDf["subg"]]["subgroupId"].sample(
            n=multItemSubgroupEstimation)
        partition = data[data["subgroupId"].isin(
            pd.concat([onlyItemsSubgroupsId, multItemSubgroupsId]))]
        return assignIDs(partition.drop(columns=["id", "subg"]).reset_index(drop=True)), round(partition["volume"].sum()/volume, 2)
    else:
        return data, round(data["volume"].sum()/volume, 2)


def getRelevantStats(data):
    """
    This function gets relevant stats like number of unique dimensions or destinations.

    Args:
        data ([type]): dataFrame
    """
    data = dataFeasibleOrientationsTupleSerializer(data)
    destinations = data["dstCode"].nunique()
    # Consider unique dimension if ordered by value in descending order (dim1, dim2, dim3)
    # being dim any of [width, height, length].
    data["dimensionUnique"] = data.apply(lambda x: tuple(
        sorted([x["width"], x["height"], x["length"]], reverse=True)), 1)
    uniqueDim = data.groupby(["dimensionUnique"]).ngroups
    ADRcount = data[data["ADR"] == 1].shape[0]
    priorityCount = data[data["priority"] == 1].shape[0]
    fragilityCount = data[data["fragility"] == 1].shape[0]
    nPackets = data.shape[0]
    nOrders = data.groupby(["subgroupId"]).ngroups
    minVol = data["volume"].min()
    return nPackets, nOrders, destinations, uniqueDim, ADRcount, priorityCount, fragilityCount, minVol


def dataFeasibleOrientationsTupleSerializer(data):
    data["feasibleOr"] = data.apply(lambda x: tuple(x["f_or"]), 1)
    return data
