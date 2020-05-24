import asyncio
from functools import partial

from pysyncobj import SyncObj, SyncObjConf
from pysyncobj.batteries import ReplDict, ReplList

from pywebio import start_server
from pywebio.input import *
from pywebio.output import *
from pywebio.session import *
from raft_server import join_cluster, get_node_info

# 最大消息记录保存
MAX_MESSAGES_CNT = 10 ** 4

# 管理员账户名
ADMIN_USER = '📢'

chat_msgs = ReplList()  # 聊天记录 (name, msg)
node_user_cnt = ReplDict()  # 每个节点的用户数

local_online_users = set()  # 本节点在线用户

raft_server = None


def onStateChanged(oldState, newState, node):
    """节点角色发生变化时的回调函数"""
    states = ["folower", "candidate", "leader"]
    send_msg(ADMIN_USER, '节点`%s`角色发生变化, `%s` -> `%s`' % (node, states[oldState], states[newState]),
             instant_output=False)


async def setup_raft(raft_addr, cluster):
    """初始化/连接 Raft 集群

    :param raft_addr: 本节点用于Raft集群通信的地址；为None时表示加入现有集群，本节点地址由本节点第一位用户输入
    :param cluster: 集群节点地址列表；为None时表示加入现有集群，集群节点地址由本节点第一位用户输入
    :return: 本节点Raft集群通信地址
    """
    global raft_server

    mode = 'init'
    if not raft_addr:  # raft_addr 为None时，表示加入Raft集群
        mode = 'join'
        data = await input_group("加入Raft集群", [
            input("当前节点的Raft通信端口", name="port"),
            input("当前节点的Host地址", name="host", value='127.0.0.1', help_text="其他节点需要可以通过此Host与当前节点通信"),
            input("集群节点地址", name="remote", placeholder='host:ip', help_text="填入集群中任一节点的地址即可")
        ])
        raft_addr = '%s:%s' % (data['host'], data['port'])
        cluster = join_cluster(raft_addr, data['remote'])
        if not cluster:
            put_markdown("### 加入集群失败")
            return

    cfg = SyncObjConf(dynamicMembershipChange=True, fullDumpFile=raft_addr + '.data',
                      onStateChanged=partial(onStateChanged, node=raft_addr))
    raft_server = SyncObj(raft_addr, cluster,
                          consumers=[chat_msgs, node_user_cnt],
                          conf=cfg)
    if mode == 'join':
        send_msg(ADMIN_USER, '节点`%s`加入集群' % raft_addr, instant_output=False)

    return raft_addr


async def refresh_msg(my_name):
    """刷新聊天消息

    将全局聊天记录列表中新增的聊天记录发送到当前会话，但排除掉当前用户的消息，当前用户的消息会在用户提交后直接输出
    """
    global chat_msgs
    last_idx = len(chat_msgs)
    while True:
        await asyncio.sleep(0.5)
        for m in chat_msgs[last_idx:]:
            if m[0] != my_name:  # 仅刷新其他人的新信息
                put_markdown('`%s`: %s' % m)

        # 清理聊天记录
        if len(chat_msgs) > MAX_MESSAGES_CNT:
            chat_msgs.reset(chat_msgs[len(chat_msgs) // 2:])

        last_idx = len(chat_msgs)


def send_msg(user, content, instant_output=True, sync=False):
    """向聊天室发送消息

    :param str user: 消息发送者
    :param str content: 消息内容，markdown格式字符串
    :param bool instant_output: 是否立即向当前会话输出此消息
    """
    chat_msgs.append((user, content), sync=sync)
    if instant_output:
        put_markdown('`%s`: %s' % (user, content))


def show_cluster_info(node_addr):
    """显示集群信息"""
    info = get_node_info(node_addr)
    popup("集群信息", [
        put_markdown("#### 当前节点信息"),
        put_table([
            ["数据项", "值"],
            ["节点地址", node_addr],
            ["节点角色", info['state']],
            ["启动时长", "%s秒" % info['uptime']],
            ["日志长度", info['log_len']],
            ["任期号", info['raft_term']],
            ["已提交的日志条目索引值", info['commit_idx']],
            ["以应用的日志条目索引值", info['last_applied']],
        ]),

        put_markdown("#### 集群信息"),
        put_table([
            ["数据项", "值"],
            ["集群Leader节点", info['leader']],
            ["集群节点数量", info['partner_nodes_count'] + 1],
        ]),

        put_markdown("#### 当前节点的相邻节点列表"),
        put_table([
            ["节点地址", "连接状态"],
            *info['partner_nodes'].items()
        ]),
    ])


async def main(raft_addr, cluster):
    global chat_msgs, raft_server

    if raft_server is None:
        raft_addr = await setup_raft(raft_addr, cluster)
        if not raft_addr:
            return
        node_user_cnt.set(raft_addr, 0, sync=True)

    node_name = raft_addr

    set_output_fixed_height(True)
    set_title("Raft Chat Room")
    put_markdown("""欢迎来到聊天室，你可以和当前Raft集群所有节点上在线的用户聊天\n
    """, lstrip=True)

    nickname = await input("请输入你的昵称", required=True,
                           valid_func=lambda n: '昵称已被使用' if n in local_online_users or n == ADMIN_USER else None)
    nickname = '%s@%s' % (nickname, node_name)

    local_online_users.add(nickname)
    node_user_cnt.set(node_name, node_user_cnt[node_name] + 1, sync=True)

    msg = '`%s`加入聊天室. 所在节点在线人数 %s, 全节点在线人数 %s' % (
        nickname, len(local_online_users), sum(node_user_cnt.values()))
    send_msg(ADMIN_USER, msg, sync=True)

    @defer_call
    def on_close():
        local_online_users.remove(nickname)
        node_user_cnt.set(node_name, node_user_cnt[node_name] - 1, sync=True)
        send_msg(ADMIN_USER, '`%s`退出聊天室. 所在节点在线人数 %s, 全节点在线人数 %s' % (
            nickname, len(local_online_users), sum(node_user_cnt.values())), instant_output=False)

    # 启动后台任务来刷新聊天消息
    refresh_task = run_async(refresh_msg(nickname))

    while True:
        data = await input_group('发送消息', [
            input(name='msg', help_text='消息内容支持Markdown语法, 回车即可发送'),
            actions(name='cmd', buttons=['发送', "集群信息", {'label': '退出', 'type': 'cancel'}])
        ], valid_func=lambda d: ('msg', '请输入要发送的内容') if d and d['cmd'] == '发送' and not d['msg'] else None)
        if data is None:
            break

        if data['cmd'] == '集群信息':
            show_cluster_info(raft_addr)
        elif data['cmd'] == '发送' and data['msg']:
            send_msg(nickname, data['msg'])

    # 关闭后台任务
    refresh_task.close()

    put_text("你已经退出聊天室")


if __name__ == '__main__':
    """
    两种启动模式：
    
    1. 初始化Raft集群：
    
        python3 app.py <本节点地址> <其他节点地址> <其他节点地址> ...
        
    注意：集群至少含有两个节点才可以正常工作
    
    示例：
        # 初始化两个节点的Raft集群 
        python3 app.py 127.0.0.1:8101 127.0.0.1:8100
        python3 app.py 127.0.0.1:8100 127.0.0.1:8101
    
    2. 加入现有的Raft集群：
        
        python3 app.py  # 在弹出的浏览器中设置本节点和集群节点地址
    
    """
    import sys

    cluster = sys.argv[1:]

    if cluster:
        raft_addr = cluster[0]
        cluster = cluster[1:]
    else:
        raft_addr = None
        cluster = None

    start_server(partial(main, raft_addr=raft_addr, cluster=cluster),
                 debug=False, auto_open_webbrowser=True)
