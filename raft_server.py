from pysyncobj.syncobj_admin import Utility


def get_node_info(address):
    """获取节点信息

    :param address: 节点地址
    :return: {
        "commit_idx": 已知的最大的已经被提交的日志条目的索引值
        "last_applied": 最后被应用到状态机的日志条目索引值
        "leader": 领导人地址
        "leader_commit_idx": 领导人commit_idx
        "log_len": 本节点日志长度
        "raft_term": 服务器最后一次知道的任期号（初始化为 0，持续递增）
        "self": 本节点地址
        "uptime": number of seconds that node process is alive
        "state": 本节点角色
        "partner_nodes": {
            节点邻居地址: 邻居状态（"disconnected", "connecting", "connected"）
        },
        "partner_nodes_count": 节点邻居数量
    }
    """
    args = ["-conn", address, "-status"]
    util = Utility(args)
    data = util.getResult()

    data = dict(i.split(': ', 1) for i in data.splitlines())

    res_key = ['commit_idx', 'last_applied', 'leader', 'leader_commit_idx', 'log_len',
               'raft_term', 'self', 'uptime', 'state']

    res = {k: data[k] for k in res_key}
    res['state'] = {
        "0": "folower",
        "1": "candidate",
        "2": "leader",
    }.get(res['state'], "unknown")

    res['partner_nodes'] = {
        k[len("partner_node_status_server_"):]: ["disconnected", "connecting", "connected"][int(v)]
        for k, v in data.items() if k.startswith("partner_node_status_server_")
        if k[len("partner_node_status_server_"):] != address
    }

    res['partner_nodes_count'] = len(res['partner_nodes'])

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

    nodes = set(get_node_info(cluster_node_address)['partner_nodes'].keys())
    nodes.add(cluster_node_address)
    nodes.remove(self_address)

    return nodes


if __name__ == '__main__':
    import sys, json

    res = get_node_info(sys.argv[1])
    print(json.dumps(res, indent=4))
