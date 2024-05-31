from plan_extractor import PlanInfo
import random
from config import Config
from connector import PGConnector
import math
config = Config()
pgrunner = PGConnector(config.database, config.username, config.password, config.pghost, config.pgport)
# 一个node对应一个pg_plan,父子节点只有一个算子的变异，变异为自顶向下
class Node:
    def __init__(self,pg_plan, sql, parent_node = None, mutate_text = None):
        self.pg_plan = pg_plan
        self.sql = sql
        self.plan_info = PlanInfo(pg_plan,sql)
        self.visits = 0
        self.reward = 0
        self.parent = parent_node
        self.left_children_type = [] 
        self.right_children_type = []
        self.children = []
        self.half_expanded = False # 完成了对左子树的全部拓展
        self.fully_expanded = False 
        self.mutate_text = [] # 对变异的描述（父亲到孩子和孩子到父亲）
        
    def get_plan(self):
        return self.pg_plan
class MCTS:
    def __init__(self,init_pg_plan,sql):
        self.root = Node(init_pg_plan,sql)
        self.sql = sql
        self.init_plan = PlanInfo(init_pg_plan,sql)
        self.bfs2dfs = self.init_plan.bfs2dfs
        self.BFS_result, self.count_map, self.child_locate = self.init_plan.BFS()
    # 表示对树中第k个算子进行变异（按BFS顺序）
    def expand(self,k,node,is_left):
        if k >= len(self.BFS_result):
            return
        op = self.BFS_result[k]
        types = config.JOIN_TYPES if op["Node Type"] in config.JOIN_TYPES else config.LEAF_TYPES
        children_type = node.left_children_type if is_left else node.right_children_type
        for to_type in types:
            if to_type not in children_type:
                children_type.append(to_type)
                dfs_idx = node.plan_info.bfs2dfs[k]
                child_pg_plan = node.plan_info.mutate(dfs_idx, to_type)
                op_name = node.plan_info.dfs2op[dfs_idx]
                mutate_text = ["将算子{op_name}从{old}变成{new}".format(op_name = op_name, old = op["Node Type"], new = to_type),"将算子{op_name}从{old}变成{new}".format(op_name = op_name, old = to_type, new = op["Node Type"])]
                new_node = Node(child_pg_plan,node.sql,node,mutate_text)
                node.children.append(new_node)
                if (k == 0 and len(children_type) == len(types)) or (is_left == False and len(children_type) == len(types) ):
                    node.fully_expanded = True
                elif is_left and len(children_type) == len(types):
                    node.half_expanded = True
                return new_node
                    

    def select(self,node):
        bestValue = float("-inf")
        bestNodes = []
        for child in node.children:
            nodeValue = child.reward / child.visits + config.UCB_para * math.sqrt(
                2 * math.log(node.visits) / child.visits) # UCB公式
            if nodeValue > bestValue:
                bestValue = nodeValue
                bestNodes = [child]
            elif nodeValue == bestValue:
                bestNodes.append(child)
        return random.choice(bestNodes)

    def update(self, node):
        if node is None:
            return
        reward = 1/node.pg_plan['Execution Time']
        while node is not None:
            node.visits += 1
            node.reward += reward
            node = node.parent

    def search(self):
        mutate_count = 0
        k = 0
        node = self.root
        while(mutate_count < config.max_mutate_count):
            new_node = None
            if node.fully_expanded :
                node = self.select(node)
                k = self.child_locate[k][0]
            elif node.half_expanded:
                new_node = self.expand(k+1,node,False)
                new_node.plan_info.visualize(mutate_count)
                node = self.root
                k = 0
                mutate_count += 1
            else:
                new_node = self.expand(k,node,True)
                new_node.plan_info.visualize(mutate_count)
                node = self.root
                k = 0
                mutate_count += 1
            self.update(new_node)

# --test--
sql = "select  avg( + table_4.col_4 * 1) as result from table_1, table_3, table_4, table_5 where table_3.fk_1 = table_4.primaryKey and table_4.fk_2 = table_5.primaryKey and table_5.fk_0 = table_1.primaryKey and table_3.col_1 >= 1535075.8764659166859830  and table_4.col_2 <= 7735092.3681367094822000  and table_5.col_10 >= 877470634.7392565792256180  and table_1.col_1 <= 1123084.84172012164390456; "
init_pg_plan = pgrunner.getPGPlan(sql)
mcts = MCTS(init_pg_plan,sql)
mcts.search()
      
