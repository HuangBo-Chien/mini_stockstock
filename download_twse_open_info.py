import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import date
from time import sleep

YEAR_CONST = 1911

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
        json = {"year": str(query_date.year - YEAR_CONST),
                "month": str(query_date.month),
                "day": str(query_date.day)},
        allow_redirects = True
    )
    if res.status_code == 200:
        return res.json()
    else:
        raise requests.RequestException()
    
def export_mops_info_in_this_date_range(date_start: date, date_end: date) -> pd.DataFrame:
    delta = date_end - date_start
    all_info_df_list = []
    for ii in range(delta.days + 1):
        one_date = date_start + pd.Timedelta(days = ii)
        try:
            print(f"Processing {one_date}")
            response_dict = query_mops_info(one_date)
            info_df = parse_mops_response(response_dict)
            all_info_df_list.append(info_df)
        except Exception as e:
            print(f"Error on {one_date}, {e}")
        sleep(10) # avoid too frequent access
    all_info_df = pd.concat(all_info_df_list, ignore_index = True)
    return all_info_df

if __name__ == "__main__":
    
    # response_dict = query_mops_info(date.today())
    # info_df = parse_mops_response(response_dict)
    # info_df.to_csv(f"test_{date.today()}.csv", index = False)

    export_mops_info_in_this_date_range(date(2024, 1, 1), date(2024, 6, 30)).to_csv("mops_2024_H1.csv", index = False)
