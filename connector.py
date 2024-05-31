import socket
import psycopg2

class PGConnector:
    def __init__(self, dbname='', user='', password='', host='', port=''):

        self.con = psycopg2.connect(database=dbname, user=user,
                                    password=password, host=host, port=port)
        self.cur = self.con.cursor()
        self.cur.execute("Load 'pg_hint_plan'")
    def getPGPlan(self,sql):
        self.cur.execute("explain (COSTS, FORMAT JSON, ANALYSE) "+sql)
        rows = self.cur.fetchall()
        PGPlan = rows[0][0][0]
        return PGPlan
    def getPGLatency(self,sql):
        return self.getPGPlan(sql)['Execution Time']
    def getPGSelectivity(self,table,predicates):
        totalQuery = "select * from "+table+";"
        self.cur.execute("EXPLAIN "+totalQuery)
        rows = self.cur.fetchall()[0][0]
        total_rows = int(rows.split("rows=")[-1].split(" ")[0])
        resQuery = "select * from "+table+" Where "+predicates+";"
        self.cur.execute("EXPLAIN  "+resQuery)
        rows = self.cur.fetchall()[0][0]
        select_rows = int(rows.split("rows=")[-1].split(" ")[0])
        return select_rows/total_rows
    def getAllTables(self):
        allTables = []
        table2index = {}
        self.cur.execute("select * from pg_tables;")
        rows = self.cur.fetchall()
        for row in rows:
            if row[0] == "public":
                allTables.append(row[1])
            else :
                break
        count = 0
        for table in allTables:
            table2index[table] = count
            count = count+1
        return table2index, allTables