import os
import json
from py2neo import Graph
from Models.LLMs import DoubaoModel

# 初始化 Neo4j 连接
graph = Graph(os.getenv("NEO4J_URI"), auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS")))
llm_client = DoubaoModel()

def find_founders_cypher(company_name):
    """根据公司名生成查找创始人的 Cypher 查询"""
    return f"""
    MATCH (c:Company {{name: '{company_name}'}})<-[:FOUNDED_BY]-(p:Person)
    RETURN p.name AS founder_name
    """

def run_query_and_format_result(query):
    """执行 Cypher 查询并格式化结果"""
    result = graph.run(query).data()
    if not result:
        return None
    
    founders = [r['founder_name'] for r in result]
    return f"检索到的信息: 该公司的创始人是 {', '.join(founders)}。"

def answer_question_with_kg(user_question):
    """主函数：利用知识图谱回答用户问题"""

    # 步骤一：定义大模型可以使用的工具
    tools = [
        {
            "name": "get_company_founders",
            "description": "获取某个公司的创始人信息。用于回答'谁创立了公司X'类型的问题。",
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {
                        "type": "string",
                        "description": "要查询的公司名称。"
                    }
                },
                "required": ["company_name"]
            }
        }
    ]

    # 步骤二：让大模型决定是否调用工具
    llm_response = llm_client.call_with_tools(user_question, tools)
    
    if llm_response.tool_call:
        tool_call = llm_response.tool_call
        if tool_call.name == "get_company_founders":
            company = tool_call.parameters["company_name"]
            
            # 步骤三：根据工具调用生成并执行 Cypher
            cypher_query = find_founders_cypher(company)
            kg_context = run_query_and_format_result(cypher_query)
            
            if kg_context:
                # 步骤四：结合图谱结果和大模型生成最终答案
                final_answer = llm_client.generate_answer_with_context(user_question, kg_context)
                return final_answer
            else:
                return f"抱歉，知识图谱中没有找到关于 {company} 创始人的信息。"
    
    # 如果大模型没有选择调用工具，可以直接让它生成答案
    return llm_client.generate_answer(user_question)

if __name__ == '__main__':
    # 示例调用
    question = "谷歌公司的创始人是谁？"
    answer = answer_question_with_kg(question)
    print(f"用户问题: {question}")
    print(f"最终答案: {answer}")
