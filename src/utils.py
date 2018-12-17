import random


def cast_to_float(data_string):
    if data_string is None or data_string == '' or data_string == '?':
        return None
    else:
        return float(data_string)


def get_least_quality_rdd(rdds_accuracy_tuples, optimizing_fn=min):
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

    return optimizing_fn(rdd_contribution_dict, key=rdd_contribution_dict.get)


def remove_least_quality_rdd(rdd_list, rdds_accuracy_tuples, minimum_list_len=10, optimizing_fn=min):
    if len(rdd_list) > minimum_list_len:
        rdd_idx = get_least_quality_rdd(rdds_accuracy_tuples, optimizing_fn)
        del rdd_list[rdd_idx:rdd_idx + 1]
    return rdd_list


def get_k_rdds_from_list(rdd_list, k=3):
    result = list()
    if len(rdd_list) > 0:
        indices = random.sample(range(0, len(rdd_list)), min(len(rdd_list), k))
        for index in indices:
            result.append(rdd_list[index])
    return result


def merge_k_rdds(rdds):
    result_rdd = rdds[0]
    for i in range(1, len(rdds)):
        result_rdd.union(rdds[i])
    return result_rdd


def get_merged_rdd(empty_rdd, rdd_list):
    for rdd in rdd_list:
        empty_rdd = empty_rdd.union(rdd)
    return empty_rdd
