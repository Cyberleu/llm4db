class Config:
    def __init__(self):
        self.pghost = "127.0.0.1"
        self.pgport = "5732"
        self.server = "9999"
        self.username = "postgres"
        self.password = "postgres"
        self.database = "artemis"
        self.device = "cpu"
        self.max_time_out = 120*1000
        self.query_path = "query.txt"
        self.JOIN_TYPES = ["Nested Loop", "Hash Join", "Merge Join"]
        self.LEAF_TYPES = ["Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Index Scan"]
        self.max_mcts_height = 3
        self.UCB_para = 1
        self.max_mutate_count = 10
        self.max_mutate_time = 6000