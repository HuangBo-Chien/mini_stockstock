from langchain_ollama import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv
import os
from io import StringIO

load_dotenv()

model = OllamaLLM(
    # model = "gpt-oss:20b",
    model = "gemma3:27b",
    base_url = os.getenv("OLLAMA_LOCAL_HOST")
    )

private_placement_template = ChatPromptTemplate.from_template(
    """
    你是一個 CSV 分析助理。我會給你一段轉換成dataFrame的 CSV 資料，欄位如下：
    發言日期,公司代號,主旨

    請幫我找出主旨跟「私募(private placement)」或是「面額變更(Stock Par Value Change)」相關的條目。 
 
    請只回傳符合的列，並輸出以下資訊：**非常重要**
    - 公司代碼 (公司代號/公司Id)
    - 發言日期
    - 該條目在 CSV 中的列號（第一列標題為第1列，資料從第2列開始算）
    - 輸出 CSV 標題固定為：公司代碼,發言日期,列號
    - 如果找不到任何符合條件的列，請只輸出標題行，下面不加資料行
    - 不要輸出任何其他文字，包含說明、前言、後記、總結、引號、註解等

    輸入格式範例:
    發言日期,公司代號,主旨
    111/01/01,1769,公告子公司資金貸與達「公開發行公司資金貸與及背書保證處理準則」第22條第1項第3款標準
    111/05/15,8964,公告本公司董事會決議112年度以貨幣債權抵繳方式增資發行私募普通股之發行股數及發行價格等相關事宜
    112/06/25,1234,本公司辦理私募普通股案暨修訂公司章程部分條文案
    112/02/19,2002,本公司一一三年股東常會重要決議事項
    113/03/20,9487,公告本公司董事會決議私募發行普通股定價相關事宜
    113/03/23,6353,公告本公司股東常會通過董事競業禁止解除案
    113/04/21,9009,公告本公司113年股東常會重要決議事項

    輸出格式範例：
    公司代碼,發言日期,列號
    8964,111/05/15,2
    1234,112/06/25,3
    9487,113/03/20,5

    這是轉換成dataFrame的 CSV 資料表:
    {df}

    讓我們一步步思考，並完成這個任務。
    """
)

if __name__ == "__main__":

    import pandas as pd
    info_df = pd.read_csv("/mnt/d/python/trading/mops_2024_H1.csv")

    chain = private_placement_template | model
    
    ## if len of info_df is too long, we split it into several parts
    output_lines = []
    max_len = 10
    for i in range(0, 10000, max_len):
        info_df_part_chunk = info_df.iloc[i:i+max_len][["發言日期", "公司代號", "主旨"]]
        print(f"Processing rows {i} to {i+max_len - 1}")
        res = chain.invoke(input = {"df": info_df_part_chunk.to_string()})
        print(res)
        print("-----")
        lines = res.strip().splitlines()
        for line in lines:
            if line.startswith("公司代碼,發言日期,列號"):
                continue
            if line.strip() == "":
                continue
            # adjust the row number
            one_line_split = line.split(",")
            one_line_split[-1] = str(int(one_line_split[-1]) % i + i) if i > 0 else one_line_split[-1] # adjust the row number
            output_lines.append(",".join(one_line_split))

    output_str = "\n".join(output_lines)
    const_header = "公司代碼,發言日期,列號\n"
    output_str = const_header + output_str
    pd.read_csv(StringIO(output_str)).to_csv("private_placement_2024_H1.csv", mode = "w", index = False)