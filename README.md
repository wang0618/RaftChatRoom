# Raft Chat Room
一个基于Raft协议的分布式聊天室



## Running

```bash
git clone https://github.com/wang0618/RaftChatRoom.git
cd RaftChatRoom
pip3 install -r requirements.txt

# 初始化两个节点的Raft集群 （集群至少含有两个节点才可以正常工作）
python3 app.py app.py 127.0.0.1:8101 127.0.0.1:8100 &  
python3 app.py app.py 127.0.0.1:8100 127.0.0.1:8101 &

# 新节点动态加入集群
python3 app.py
```