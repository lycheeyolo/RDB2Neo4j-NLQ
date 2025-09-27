import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()
class DoubaoModel(ChatOpenAI):
    def __init__(self):
        super().__init__(# 环境变量中配置您的API Key
            openai_api_key=os.getenv("DOUBAO_KEY"), 
            # 替换为您需要调用的模型服务Base Url
            openai_api_base=os.getenv("DOUBAO_URL"),
            # 替换为您创建推理接入点 ID
            model_name=os.getenv("DOUBAO_MODEL_NAME"))