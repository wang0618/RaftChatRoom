import asyncio
from functools import partial

from pysyncobj import SyncObj, SyncObjConf
from pysyncobj.batteries import ReplDict, ReplList

from pywebio import start_server
from pywebio.input import *
from pywebio.output import *
from pywebio.session import *
from raft_server import join_cluster

# 最大消息记录保存
MAX_MESSAGES_CNT = 10 ** 4

chat_msgs = ReplList()  # 聊天记录 (name, msg)
node_user_cnt = ReplDict()  # 每个节点的用户数

local_online_users = set()  # 本节点在线用户

raft_server = None


async def setup_raft(raft_addr, cluster):
    global raft_server

    # raft_addr 为None时，加入Raft集群
    if not raft_addr:
        data = await input_group("加入Raft集群", [
            input("当前节点的Raft通信端口", name="port"),
            input("当前节点的Host地址", name="host", value='127.0.0.1', help_text="其他节点需要可以通过此Host与当前节点通信"),
            input("集群节点地址", name="remote", placeholder='host:ip', help_text="填入集群中任一节点的地址即可")
        ])
        raft_addr = '%s:%s' % (data['host'], data['port'])
        cluster = join_cluster(raft_addr, data['remote'])

    cfg = SyncObjConf(dynamicMembershipChange=True, fullDumpFile=raft_addr + '.data')
    raft_server = SyncObj(raft_addr, cluster,
                          consumers=[chat_msgs, node_user_cnt],
                          conf=cfg)


async def refresh_msg(my_name):
    """刷新聊天消息"""
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


async def main(raft_addr, cluster):
    global chat_msgs, raft_server
    node_name = raft_addr
    if raft_server is None:
        await setup_raft(raft_addr, cluster)
        node_user_cnt[node_name] = 0

    set_output_fixed_height(True)
    set_title("PyWebIO Chat Room")
    put_markdown("""欢迎来到聊天室，你可以和当前Raft集群所有节点上在线的用户聊天\n
    """, lstrip=True)

    nickname = await input("请输入你的昵称", required=True,
                           valid_func=lambda n: '昵称已被使用' if n in local_online_users or n == '📢' else None)
    nickname = '%s@%s' % (nickname, node_name)

    local_online_users.add(nickname)
    node_user_cnt.set(node_name, node_user_cnt[node_name] + 1, sync=True)

    msg = ('📢', '`%s`加入聊天室. 所在节点在线人数 %s, 全节点在线人数 %s' % (
        nickname, len(local_online_users), sum(node_user_cnt.values())))
    chat_msgs.append(msg, sync=True)
    put_markdown('`%s`: %s' % msg)

    @defer_call
    def on_close():
        local_online_users.remove(nickname)
        node_user_cnt.set(node_name, node_user_cnt[node_name] - 1, sync=True)
        chat_msgs.append(('📢', '`%s`退出聊天室. 所在节点在线人数 %s, 全节点在线人数 %s' % (
            nickname, len(local_online_users), sum(node_user_cnt.values()))))

    refresh_task = run_async(refresh_msg(nickname))

    while True:
        data = await input_group('发送消息', [
            input(name='msg', help_text='消息内容支持Markdown 语法', required=True),
            actions(name='cmd', buttons=['发送', {'label': '退出', 'type': 'cancel'}])
        ])
        if data is None:
            break

        if data['msg'].startswith('!'):
            try:
                eval(data['msg'][1:], globals(), globals())
            except Exception as e:
                put_text('%s' % e)

        put_markdown('`%s`: %s' % (nickname, data['msg']))
        chat_msgs.append((nickname, data['msg']))

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
