# def mergeToDistributeClassesEqually(rddOld, rddNew):
#     if rddOld is None:
#         return rddNew
#     # Merge the two rdd's so that we have a single rdd with all classes equally distributed.
#     return rddOld.union(rddNew)


def cast_to_float(data_string):
    if data_string is None or data_string == '' or data_string == '?':
        return None
    else:
        return float(data_string)

def get_least_quality_rdd(rdds_accuracy_tuples):
    # Returns the index of the least quality RDD.
    # Input format: [([1,2,3],9.8), ([1,6,7],8.6)]
    # Output: Index of the RDD that contributes to the least accuracy.
    rdd_contribution_dict = dict()
    for tuple in rdds_accuracy_tuples:
        for rdd_idx in tuple[0]:
            if rdd_idx in rdd_contribution_dict:
                rdd_contribution_dict[rdd_idx] = rdd_contribution_dict[rdd_idx] + tuple[1]
            else:
                rdd_contribution_dict[rdd_idx] = tuple[1]

    return min(rdd_contribution_dict, key=rdd_contribution_dict.get)

def remove_least_quality_rdd(list, rdds_accuracy_tuples, minimum_list_len=10):
    if (len(list) > minimum_list_len):
        rdd_idx = get_least_quality_rdd(rdds_accuracy_tuples)
        del list[rdd_idx:rdd_idx+1]
    return list