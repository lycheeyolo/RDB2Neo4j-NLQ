import json
import os
import sys
import decimal
import datetime
from dotenv import load_dotenv

parent_path = os.path.dirname(__file__)

# 提前导入数据库驱动，以便在异常捕获时可用
try:
    import mysql.connector
    from py2neo import Graph
except ImportError:
    print("错误: 未找到所需的库。请运行 'pip install mysql-connector-python py2neo python-dotenv'。")
    sys.exit(1)

# -----------------------------------------------------------------------------
# 数据库连接器
# -----------------------------------------------------------------------------
def get_db_connection():
    """根据 .env 文件中的配置建立并返回一个关系型数据库连接。"""
    load_dotenv()
    db_type = os.getenv("DB_TYPE")

    if db_type == "mysql":
        try:
            return mysql.connector.connect(
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_NAME")
            )
        except mysql.connector.Error as err:
            print(f"关系型数据库连接失败: {err}")
            sys.exit(1)
    else:
        print(f"错误: 不支持的数据库类型 '{db_type}'。请检查 .env 文件。")
        sys.exit(1)

# -----------------------------------------------------------------------------
# 步骤 1: 从真实的关系型数据库中抽取元数据
# -----------------------------------------------------------------------------
def extract_relational_schema():
    """连接到数据库并从 INFORMATION_SCHEMA 中查询元数据。"""
    conn = get_db_connection()
    cursor = conn.cursor()
    db_name = os.getenv("DB_NAME")
    
    schema = {
        "tables": [],
        "foreign_keys": []
    }

    try:
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (db_name,))
        tables = [row[0] for row in cursor.fetchall()]

        for table_name in tables:
            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (db_name, table_name))
            columns = [row[0] for row in cursor.fetchall()]

            cursor.execute("""
                SELECT k.column_name FROM information_schema.table_constraints t
                JOIN information_schema.key_column_usage k USING(constraint_name,table_schema,table_name)
                WHERE t.constraint_type = 'PRIMARY KEY'
                  AND t.table_schema = %s AND t.table_name = %s
            """, (db_name, table_name))
            pk_result = cursor.fetchone()
            primary_key = pk_result[0] if pk_result else None
            
            schema["tables"].append({
                "name": table_name,
                "columns": columns,
                "primary_key": primary_key
            })

        cursor.execute("""
            SELECT kcu.table_name AS from_table, kcu.column_name AS from_column,
                   kcu.referenced_table_name AS to_table, kcu.referenced_column_name AS to_column
            FROM information_schema.key_column_usage AS kcu
            JOIN information_schema.table_constraints AS tc
              ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = %s
        """, (db_name,))
        
        fk_columns = [desc[0] for desc in cursor.description]
        fks_list = [dict(zip(fk_columns, row)) for row in cursor.fetchall()]
        schema["foreign_keys"] = fks_list

    except mysql.connector.Error as err:
        print(f"数据库操作出错: {err}")
    except Exception as e:
        print(f"从数据库抽取元数据时出错: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
    return schema

# -----------------------------------------------------------------------------
# 步骤 2: 基于元数据，生成一个初始的配置文件
# -----------------------------------------------------------------------------
def generate_initial_config(schema):
    """根据抽取的元数据生成一个初始的JSON配置文件。"""
    config = {
        "nodes": [],
        "relationships": []
    }
    
    for table in schema["tables"]:
        fk_columns = [fk['from_column'] for fk in schema['foreign_keys'] if fk['from_table'] == table['name']]
        
        if len(table['columns']) == len(set(fk_columns)) and table['primary_key'] is None:
            print(f"检测到表 '{table['name']}' 可能是纯关系表，跳过节点映射。")
            continue

        node_mapping = {
            "source_table": table["name"],
            "label": ''.join(word.capitalize() for word in table["name"].split('_')),
            "properties": {col: col for col in table["columns"]},
            "primary_key": table["primary_key"]
        }
        config["nodes"].append(node_mapping)

    for fk in schema["foreign_keys"]:
        from_table_is_entity = any(
            node['source_table'] == fk['from_table'] for node in config['nodes']
        )
        
        if not from_table_is_entity:
            related_fks = [f for f in schema['foreign_keys'] if f['from_table'] == fk['from_table']]
            if len(related_fks) >= 2:
                fk1, fk2 = related_fks[0], related_fks[1] 
                
                link_table_cols = next((t['columns'] for t in schema['tables'] if t['name'] == fk['from_table']), [])
                link_fk_cols = [f['from_column'] for f in related_fks]
                prop_cols = [col for col in link_table_cols if col not in link_fk_cols]

                rel = {
                    "source_link_table": fk['from_table'],
                    "type": f"HAS_{fk2['to_table'].upper()}",
                    "from_node_table": fk1['to_table'],
                    "to_node_table": fk2['to_table'],
                    "properties": {col: col for col in prop_cols}
                }
                if not any(r.get("source_link_table") == rel["source_link_table"] for r in config["relationships"]):
                    config["relationships"].append(rel)
        else:
            rel = {
                "source_foreign_key": f"{fk['from_table']}.{fk['from_column']}",
                "type": f"HAS_{fk['to_table'].upper()}",
                "from_node_table": fk["from_table"],
                "to_node_table": fk["to_table"],
                "direction": "OUT",
                "properties": {}
            }
            config["relationships"].append(rel)

    with open(os.path.join(parent_path, "config.json"), "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4, ensure_ascii=False)

    print("已生成初始配置文件 'config.json'。请打开并根据您的业务需求进行修改。")

# -----------------------------------------------------------------------------
# 数据类型转换工具函数
# -----------------------------------------------------------------------------
def convert_value_for_neo4j(value):
    """
    将一些特定的Python数据类型转换为Neo4j兼容的类型。
    """
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    # 可以根据需要添加其他类型转换
    return value

# -----------------------------------------------------------------------------
# 步骤 3 & 4: 读取配置并执行数据导入
# -----------------------------------------------------------------------------
class Neo4jImporter:
    def __init__(self):
        load_dotenv()
        self.neo4j_uri = os.getenv("NEO4J_URI")
        self.neo4j_user = os.getenv("NEO4J_USER")
        self.neo4j_pass = os.getenv("NEO4J_PASS")
        self.neo4j_graph = None
        self.batch_size = 1000  # 批量处理大小

    def connect(self):
        """连接到 Neo4j 数据库。"""
        try:
            self.neo4j_graph = Graph(self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_pass))
            print("Neo4j 数据库连接成功。")
            return True
        except Exception as e:
            print(f"Neo4j 数据库连接失败: {e}")
            return False

    def import_data(self, config_path="config.json"):
        """根据配置文件从关系型数据库导入数据到Neo4j。"""
        if not self.neo4j_graph:
            print("无法执行导入，Neo4j 数据库未连接。")
            return

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                schema_config = json.load(f)
        except FileNotFoundError:
            print(f"错误: 配置文件 '{config_path}' 不存在。请先运行脚本生成它。")
            return

        rdb_conn = get_db_connection()
        rdb_cursor = rdb_conn.cursor(dictionary=True, buffered=True)

        # 1. 导入节点（批量处理）
        print("\n--- 开始批量导入节点 ---")
        for node_def in schema_config["nodes"]:
            table_name = node_def["source_table"]
            node_label = node_def["label"]
            id_property = node_def["primary_key"]
            properties_map = node_def["properties"]

            print(f"正在从表 '{table_name}' 导入节点...")
            rdb_cursor.execute(f"SELECT * FROM `{table_name}`")
            
            node_props_list = []
            for row in rdb_cursor:
                node_props = {
                    neo4j_prop: convert_value_for_neo4j(row.get(rdb_col))
                    for rdb_col, neo4j_prop in properties_map.items()
                }
                node_props_list.append(node_props)
                
                # 当达到批处理大小时，执行批量导入
                if len(node_props_list) >= self.batch_size:
                    self._merge_nodes_batch(node_label, id_property, node_props_list)
                    node_props_list = []
            
            # 处理剩余的节点
            if node_props_list:
                self._merge_nodes_batch(node_label, id_property, node_props_list)

            print(f"从表 '{table_name}' 导入节点完成。")

        # 2. 导入关系（批量处理）
        print("\n--- 开始批量导入关系 ---")
        for rel_def in schema_config["relationships"]:
            from_table = rel_def["from_node_table"]
            to_table = rel_def["to_node_table"]
            rel_type = rel_def["type"]
            
            from_node_config = next((n for n in schema_config['nodes'] if n['source_table'] == from_table), None)
            to_node_config = next((n for n in schema_config['nodes'] if n['source_table'] == to_table), None)
            
            if not from_node_config or not to_node_config:
                print(f"警告: 无法找到关系 '{rel_type}' 的源或目标节点配置，跳过。")
                continue
                
            from_label = from_node_config['label']
            from_pk = from_node_config['primary_key']
            to_label = to_node_config['label']
            to_pk = to_node_config['primary_key']

            if "source_foreign_key" in rel_def:
                source_table_name = rel_def["source_foreign_key"].split('.')[0]
                fk_column = rel_def["source_foreign_key"].split('.')[1]
                
                print(f"正在基于表 '{source_table_name}' 批量创建关系 '{rel_type}'...")
                rdb_cursor.execute(f"SELECT `{from_pk}`, `{fk_column}` FROM `{source_table_name}`")
                
                rel_data_list = []
                for row in rdb_cursor:
                    rel_data_list.append({
                        "from_id": row.get(from_pk),
                        "to_id": row.get(fk_column)
                    })
                    if len(rel_data_list) >= self.batch_size:
                        self._merge_rels_batch(from_label, from_pk, to_label, to_pk, rel_type, rel_data_list)
                        rel_data_list = []
                if rel_data_list:
                    self._merge_rels_batch(from_label, from_pk, to_label, to_pk, rel_type, rel_data_list)
            
            elif "source_link_table" in rel_def:
                link_table_name = rel_def["source_link_table"]
                
                from_fk_col = next((fk['from_column'] for fk in schema_data['foreign_keys'] if fk['from_table'] == link_table_name and fk['to_table'] == from_table), None)
                to_fk_col = next((fk['from_column'] for fk in schema_data['foreign_keys'] if fk['from_table'] == link_table_name and fk['to_table'] == to_table), None)

                if not from_fk_col or not to_fk_col:
                    print(f"警告: 无法找到中间表 '{link_table_name}' 的外键列，跳过关系 '{rel_type}'。")
                    continue
                
                print(f"正在基于中间表 '{link_table_name}' 批量创建关系 '{rel_type}'...")
                rdb_cursor.execute(f"SELECT * FROM `{link_table_name}`")
                
                rel_data_list = []
                for row in rdb_cursor:
                    rel_props = {
                        neo4j_prop: convert_value_for_neo4j(row.get(rdb_col))
                        for rdb_col, neo4j_prop in rel_def.get("properties", {}).items()
                    }
                    rel_data_list.append({
                        "from_id": row.get(from_fk_col),
                        "to_id": row.get(to_fk_col),
                        "props": rel_props
                    })
                    if len(rel_data_list) >= self.batch_size:
                        self._merge_rels_batch(from_label, from_pk, to_label, to_pk, rel_type, rel_data_list)
                        rel_data_list = []
                if rel_data_list:
                    self._merge_rels_batch(from_label, from_pk, to_label, to_pk, rel_type, rel_data_list)
        
        print("所有数据导入完成。")
        rdb_cursor.close()
        rdb_conn.close()

    def _merge_nodes_batch(self, label, id_prop, prop_list):
        """使用UNWIND批量创建/更新节点。"""
        if not prop_list:
            return
        
        # 确保主键存在
        valid_props = [p for p in prop_list if p.get(id_prop) is not None]
        if not valid_props:
            return

        query = f"""
        UNWIND $props AS map
        MERGE (n:`{label}` {{`{id_prop}`: map.`{id_prop}`}})
        SET n += map
        """
        try:
            self.neo4j_graph.run(query, props=valid_props)
        except Exception as e:
            print(f"批量创建节点 {label} 失败: {e}")

    def _merge_rels_batch(self, from_label, from_pk, to_label, to_pk, rel_type, data_list):
        """使用UNWIND批量创建关系。"""
        if not data_list:
            return
        
        query = f"""
        UNWIND $data AS map
        MATCH (a:`{from_label}` {{`{from_pk}`: map.from_id}})
        MATCH (b:`{to_label}` {{`{to_pk}`: map.to_id}})
        MERGE (a)-[r:`{rel_type}`]->(b)
        ON CREATE SET r = map.props
        """
        try:
            self.neo4j_graph.run(query, data=data_list)
        except Exception as e:
            print(f"批量创建关系 {rel_type} 失败: {e}")


if __name__ == "__main__":
    print("步骤 1 & 2: 正在从数据库抽取元数据并生成初始配置文件 'config.json'...")
    schema_data = extract_relational_schema()
    
    if not schema_data or not schema_data["tables"]:
        print("未能从数据库中获取任何表信息，请检查您的 .env 配置。")
    else:
        generate_initial_config(schema_data)
        print("\n初始配置文件 'config.json' 已生成。")
        input("\n请打开并修改 'config.json' 以符合您的业务模型，完成后按Enter键继续...")
        
        # 新增: 步骤 3 & 4
        neo4j_importer = Neo4jImporter()
        if neo4j_importer.connect():
            neo4j_importer.import_data()
