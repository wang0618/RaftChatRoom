# Raft Chat Room
一个基于Raft协议的分布式聊天室



## Running

```bash
git clone https://github.com/wang0618/RaftChatRoom.git
cd RaftChatRoom
pip3 install -r requirements.txt

python3 app.py --raft_port 4100 --cluster "4101 4102" &
python3 app.py --raft_port 4101 --cluster "4100 4102" &
python3 app.py --raft_port 4102 --cluster "4101 4100" 
```