from config import Config
from connector import PGConnector
from graphviz import Digraph
import subprocess
config = Config()
pgrunner = PGConnector(config.database, config.username, config.password, config.pghost, config.pgport)   
Type2Hint = {"Nested Loop":"nestloop", "Hash Join":"hashjoin", "Merge Join":"mergejoin","Seq Scan":"seqscan", "Index Scan":"indexscan", "Index Only Scan":"indexonlyscan", "Bitmap Index Scan":"bitmapscan"}
class node:
    def __init__(self,pg_plan):
        self.plan = pg_plan
        self.children = []
# 用作查询计划相关的查找和修改
class PlanInfo:
    def __init__(self,pg_plan,sql):
        self.plan = pg_plan
        self.sql = sql
        self.join_order = []
        self.root = self.transform(self.plan)
        self.children_count = {} # 记录每个节点的孩子数
        self.bfs2dfs = {} # 记录bfs顺序到dfs顺序的映射
        self.dfs2op = {} # 记录dfs顺序到算子名称的映射
        self.join_count = 0
        self.scan_count = 0
        self.count = 0 # 对深度遍历顺序的计数
        self.child_count()
        self.get_bfs2dfs()
        self.get_dfs2op()

    # 对查询计划进行简化，提取出join和scan结构
    def transform(self, plan):
        if "Plan" in plan:
            plan = plan["Plan"]
        children = plan["Plan"] if "Plan" in plan else (plan["Plans"] if "Plans" in plan else [])
        # len(children) == 1 表示node type为gather，是并行时产生的结果，没有实际意义
        if len(children) == 1:
            return self.transform(children[0])
        if plan["Node Type"] in config.JOIN_TYPES:
            left = self.transform(children[0])
            right = self.transform(children[1])
            pos = node(plan)
            pos.children.append(left)
            pos.children.append(right)
            return pos
        if plan["Node Type"] in config.LEAF_TYPES:
            return node(plan)
    def get_join_order_helper(self, plan):
        if "Plan" in plan:
            plan = plan["Plan"]
        children = plan["Plan"] if "Plan" in plan else (plan["Plans"] if "Plans" in plan else [])
        # len(children) == 1 表示node type为gather，是并行时产生的结果，没有实际意义
        if len(children) == 1:
            return self.get_join_order_helper(children[0])
        if plan["Node Type"] in config.JOIN_TYPES:
            left = self.get_join_order_helper(children[0])
            right = self.get_join_order_helper(children[1])
            return '(' + left + ' ' + right + ')'
        if plan["Node Type"] in config.LEAF_TYPES:
            return plan["Relation Name"]
    def get_join_order(self):
        text = "Leading("
        text += self.get_join_order_helper(self.plan)
        text = text + ')'
        return text
    def get_operators_helper(self, plan):
        if "Plan" in plan:
            plan = plan["Plan"]
        children = plan["Plan"] if "Plan" in plan else (plan["Plans"] if "Plans" in plan else [])
        # len(children) == 1 表示node type为gather，是并行时产生的结果，没有实际意义
        if len(children) == 1:
            return self.mutate_helper(children[0])
        if plan["Node Type"] in config.JOIN_TYPES:
            left, left_tables = self.mutate_helper(children[0])
            right, right_tables = self.mutate_helper(children[1])
            all_tables_txt = ""
            for table in left_tables:
                all_tables = all_tables + table + ' '
            for table in right_tables:
                all_tables = all_tables + table + ' '
            pos = Type2Hint[plan["Node Type"]] + '(' + all_tables + ')'
            return left + right + pos + ' ', left_tables + right_tables
        if plan["Node Type"] in config.LEAF_TYPES:
            return Type2Hint[plan["Node Type"]] + '(' + plan["Relation Name"] + ')', plan["Relation Name"]
    def get_operators(self):
        return self.get_operators_helper(self.plan)
    # 进行层次遍历，返回一个数组（层次遍历的内容），一个map（每层的节点数）, i个节点的儿子节点所在位置
    def BFS(self):
        ans = []
        count_map = {}
        child_locate = {}
        height = 0
        count = 1
        q = []
        q.append(self.root)
        while(len(q) != 0):
            count_map[height] = len(q)
            height += 1
            k = len(q)
            for i in range(k):
                tmp = q[0]
                q.pop(0)
                ans.append(tmp.plan)
                if tmp.plan["Node Type"] in config.JOIN_TYPES:
                    left_node = tmp.children[0]
                    right_node = tmp.children[1]
                    q.append(left_node)
                    q.append(right_node)
                    child_locate[len(ans)-1] = [count,count+1]
                    count += 2
        return ans, count_map, child_locate
    # 后序遍历
    def DFS(self,node):
        if node.plan["Node Type"] in config.LEAF_TYPES:
            return [node.plan]
        left = self.DFS(node.children[0])
        right = self.DFS(node.children[1])
        return left + right + [node.plan]
    def child_count(self):
        self.child_count_helper(self.root)
        self.count = 0
    def child_count_helper(self,node):
        if node.plan["Node Type"] in config.LEAF_TYPES:
            self.children_count[self.count] = 1
            self.count += 1
            return 1
        left = self.child_count_helper(node.children[0])
        right = self.child_count_helper(node.children[1])
        self.children_count[self.count] = [left,right]
        self.count += 1
        return left + right + 1
    def get_bfs2dfs(self):
        BFS_order,_,_ = self.BFS()
        DFS_order = self.DFS(self.root)
        for i in range(len(BFS_order)):
            for j in range(len(DFS_order)):
                if BFS_order[i]["Actual Startup Time"] == DFS_order[j]["Actual Startup Time"]:
                    self.bfs2dfs[i] = j

    def mutate_helper(self, node, k, to_type):
        
        if node.plan["Node Type"] in config.JOIN_TYPES:
            left, left_tables = self.mutate_helper(node.children[0],k,to_type)
            right, right_tables = self.mutate_helper(node.children[1],k,to_type)
            all_tables = left_tables + right_tables
            # 此节点是需要替换的位置
            if self.count == k:
                pos =  Type2Hint[to_type] + '(' + all_tables + ') '
            else:
                pos = Type2Hint[node.plan["Node Type"]] + '(' + all_tables + ') '
            self.count += 1
            return left + right + pos + ' ', left_tables + right_tables
        if node.plan["Node Type"] in config.LEAF_TYPES:
            if self.count == k:
                self.count += 1
                return Type2Hint[to_type] + '(' + node.plan["Relation Name"] + ') ', node.plan["Relation Name"] + ' '
            else:
                self.count += 1
                return Type2Hint[node.plan["Node Type"]] + '(' + node.plan["Relation Name"] + ') ', node.plan["Relation Name"]+ ' '

             
    # 找到原始的pg_plan按dfs遍历的第k个节点，将当前plan的root节点变成to_type
    def mutate(self, k, to_type):
        hint = "/*+ "
        mutate_text,_ = self.mutate_helper(self.root, k, to_type)
        join_order_text = self.get_join_order()
        hint = hint + mutate_text + join_order_text + ' */ '
        self.count = 0
        return pgrunner.getPGPlan(hint + self.sql)
    def visualize_helper(self, node, g):
        if node.plan["Node Type"] in config.LEAF_TYPES:
            g.node(str(self.count),node.plan["Relation Name"])
            self.count += 1
            return str(self.count-1)
        left = self.visualize_helper(node.children[0],g)
        right = self.visualize_helper(node.children[1],g)
        g.node(str(self.count),node.plan["Node Type"])
        g.edge(str(self.count), left)
        g.edge(str(self.count), right)
        self.count += 1
        return str(self.count-1)
    def visualize(self,idx):
        g = Digraph('G', filename='./pic/plan{idx}.dot'.format(idx = idx))
        self.visualize_helper(self.root,g)
        g.view()
        command = "dot -Tpng ./pic/plan{idx1}.dot -o ./pic/plan{idx2}.png".format(idx1 = idx, idx2 = idx)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.communicate()
        self.count = 0

    def dfs2op_helper(self,node):
        if node.plan["Node Type"] in config.LEAF_TYPES:
            op = 'S' + str(self.scan_count)
            self.dfs2op[self.count] = op
            self.scan_count += 1
            self.count += 1
            return
        self.dfs2op_helper(node.children[0])
        self.dfs2op_helper(node.children[1])
        op = 'J'+str(self.join_count)
        self.dfs2op[self.count] = op
        self.join_count += 1
        self.count += 1

    def get_dfs2op(self):
        self.dfs2op_helper(self.root)
        self.count = 0
        self.join_count = 0
        self.scan_count = 0
    
    # 以自然语言描述树
    def plan2text_helper(self,node):
        if node.plan["Node Type"] in config.LEAF_TYPES:
            new_op = self.dfs2op[self.count]
            self.count += 1
            return "通过{scantype}对{relation}进行扫描，得到扫描算子{new_op}，".format(scantype = Type2Hint[node.plan["Node Type"]],relation = node.plan["Relation Name"], new_op = new_op), new_op
        left_text, left_op = self.plan2text_helper(node.children[0])
        right_text, right_op = self.plan2text_helper(node.children[1])
        new_op = self.dfs2op[self.count]
        pos_text = "将算子{left_op}和算子{right_op}通过{jointype}进行连接，得到连接算子{new_op}，".format(left_op = left_op, right_op = right_op, jointype = Type2Hint[node.plan["Node Type"]],new_op = new_op)
        text = left_text + right_text + pos_text if  self.children_count[self.count][0] > self.children_count[self.count][1] else right_text + left_text + pos_text
        self.count += 1
        return text, new_op
    
    def plan2text(self):
        self.child_count()
        text = self.plan2text_helper(self.root)[0]
        text = text[:-1] + "。"
        self.join_count = 0
        self.scan_count = 0
        return text
        
# --test--
# sql = "select  avg( + table_4.col_4 * 1) as result from table_1, table_3, table_4, table_5 where table_3.fk_1 = table_4.primaryKey and table_4.fk_2 = table_5.primaryKey and table_5.fk_0 = table_1.primaryKey and table_3.col_1 >= 1535075.8764659166859830  and table_4.col_2 <= 7735092.3681367094822000  and table_5.col_10 >= 877470634.7392565792256180  and table_1.col_1 <= 1123084.84172012164390456; "
# pg_plan = pgrunner.getPGPlan(sql)
# plan_info = PlanInfo(pg_plan, sql)
# print(plan_info.get_join_order())
# ans, count_map, child_locate = plan_info.BFS()
# ans = plan_info.DFS(plan_info.root)
# plan_info.get_bfs2dfs()
# new_pg_Plan = plan_info.mutate(4,"Merge Join")
# plan_info.visualize()
# ans = plan_info.plan2text()
