from pysyncobj.syncobj_admin import Utility


def get_node_info(address):
    args = ["-conn", address, "-status"]
    util = Utility(args)
    data = util.getResult()

    data = dict(i.split(': ', 1) for i in data.splitlines())

    res_key = ['commit_idx', 'last_applied', 'leader', 'leader_commit_idx', 'log_len', 'partner_nodes_count',
               'raft_term', 'self', 'uptime', 'state']

    res = {k: data[k] for k in res_key}
    res['state'] = {
        "0": "folower",
        "1": "candidate",
        "2": "leader",
    }.get(res['state'], "unknown")

    res['partner_nodes'] = [
        k[len("partner_node_status_server_"):]
        for k in data.keys() if k.startswith("partner_node_status_server_")
    ]

    return res


def join_cluster(self_address, cluster_node_address):
    """ 加入Raft集群

    :param address: 集群中任意一个节点的地址
    :return: 集群中节点地址列表(不包含当前节点)，失败时返回 False
    """
    args = ['-conn', cluster_node_address, '-add', self_address]
    util = Utility(args)
    if "SUCCESS" not in util.getResult():
        return False

    nodes = set(get_node_info(cluster_node_address)['partner_nodes'] + [cluster_node_address])
    nodes.remove(self_address)

    return nodes


if __name__ == '__main__':
    import sys, json

    res = get_node_info(sys.argv[1])
    print(json.dumps(res, indent=4))
