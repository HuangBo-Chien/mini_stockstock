from langchain_ollama import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv
import os

load_dotenv()

model = OllamaLLM(
    model = "gpt-oss:20b",
    base_url = os.getenv("OLLAMA_LOCAL_HOST")
    )

private_placement_template = ChatPromptTemplate.from_template(
    """
    請你幫我分析以下的資料表中，哪些是跟募資(private placement)或是面額變更(Stock Par Value Change)有關，並且將有關的index給列出來，如果沒有找到相關的index，請回答-1，謝謝!

    資料表:
    {df}

    """
)

if __name__ == "__main__":
    import pandas as pd
    info_df = pd.read_csv("test.csv")

    chain = private_placement_template | model
    res = chain.invoke(input = {"df": info_df.to_string()})

    print(res)