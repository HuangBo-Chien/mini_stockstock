import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import date

my_session = requests.session()
my_session.headers.update({
    "User-Agent": r"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Accept": r"*/*"
})

def parse_mops_response(response_dict: dict) -> pd.DataFrame:
    # response_dict["result"]["data"] # data
    # response_dict["result"]["titles"][0]["main"] # columns titles
    columns_titles = [ele["main"] for ele in response_dict["result"]["titles"]]
    list_of_dict = []
    for one_row in response_dict["result"]["data"]:
        out_dict = {columns_titles[ii]: one_row[ii] for ii in range(5)}
        last_dict: dict = one_row[-1]
        """ e.g. 
        {'parameters': 
            {'companyId': '2308',
             'marketKind': 'sii',
             'enterDate': '1140121', 
             'serialNumber': 1
            },
         'apiName': 't05st02_detail'
        }
        """
        out_dict.update(last_dict["parameters"])
        out_dict.update({"apiName": last_dict["apiName"]})
        list_of_dict.append(out_dict)
    info_df = pd.DataFrame(list_of_dict)
    return info_df

def query_mops_info(query_date: date) -> dict:

    MOPS_URL = "https://mops.twse.com.tw/mops/api/t05st02"    
    res = my_session.post(
        url = MOPS_URL,
        json = {"year": str(query_date.year),
                "month": str(query_date.month),
                "day": str(query_date.day)},
        allow_redirects = True
    )
    if res.status_code == 200:
        return res.json()
    else:
        raise requests.RequestException()

if __name__ == "__main__":
    
    response_dict = query_mops_info(date.today())
    info_df = parse_mops_response(response_dict)
