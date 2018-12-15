def mergeToDistributeClassesEqually(rddOld, rddNew):
    if rddOld is None:
        return rddNew
    # Merge the two rdd's so that we have a single rdd with all classes equally distributed.
    return rddOld.union(rddNew)


def cast_to_float(data_string):
    if data_string is None or data_string == '' or data_string == '?':
        return None
    else:
        return float(data_string)
