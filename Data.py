import yfinance as yf
from pathlib import Path
from typing import Optional
import datetime
import pandas as pd
from loguru import logger
import requests
import time

_my_logger = logger.bind(name = "Data.log")
_default_path = Path(__file__).with_name("default_stock_folder")

def getData(StockID: str, Start_Date: datetime.date, End_Date: datetime.date, saved_path: Optional[Path] = None, force: bool = False, **kwargs) -> pd.DataFrame:
    """
    Download OHLC of StockID from yahoo finance.
    """
    if saved_path is None:
        saved_path = _default_path.joinpath(f"{StockID}_{Start_Date}_{End_Date}.xlsx")
        _default_path.mkdir(exist_ok = True)

    if not force and saved_path.exists():
        _my_logger.debug(f"Stock: {StockID} has already existed.")
        # Read back, telling Pandas that first 2 rows are headers for MultiIndex
        data_df = pd.read_excel(saved_path, header = [0, 1], index_col = 0)
        return data_df
    
    data_df = yf.download(
        tickers = StockID,
        start = Start_Date,
        end = End_Date
    )

    if data_df is None or len(data_df) == 0:
        logger.error(f"Stock: {StockID} is empty.")
        return pd.DataFrame() # return empty dataframe
    data_df.to_excel(saved_path) # save df as a local excel
    return data_df

def getBrokerInfo():
    """
    Download broker infomation from TWSE
    ### Reference:
    https://www.twse.com.tw/zh/products/broker/infomation/list.html
    https://www.twse.com.tw/zh/products/broker/infomation/detail.html?1230
    """

    def Fubon_BrokerID_conversion(BrokerID: str) -> str:
        """
        Convert BrokerID into another string acceptable by Fubon
        (https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm?)

        Example
        =====
        >>> BrokerID = "102A"
        >>> output = Fubon_BrokerID_conversion(BrokerID)
        >>> output
        >>> "0031003000320041"
        """
        if "/" in BrokerID:
            BrokerID = BrokerID.split("/")[0] # 8888/8889 --> 8888
        if BrokerID.isnumeric(): # e.g. 8880
            return BrokerID
        output_str_list = []
        for ss in BrokerID:
            tmp_ss = hex(ord(ss)) # 0x??
            tmp_list = list(tmp_ss)
            tmp_list[1] = "0"
            tmp_ss = "".join(tmp_list)
            output_str_list.append(tmp_ss)
        return "".join(output_str_list)

    TWSE_BROKER_SERVICE_AUDIT_URL = r"https://www.twse.com.tw/rwd/zh/brokerService/brokerServiceAudit?"

    local_broker_csv_path = _default_path.joinpath("Broker_Info.csv")
    if local_broker_csv_path.exists():
        all_broker_info_df = pd.read_csv(local_broker_csv_path)
        return all_broker_info_df
    
    res = requests.get(url = f"{TWSE_BROKER_SERVICE_AUDIT_URL}showType=main&response=json")
    data = res.json()
    broker_info_df = pd.DataFrame(data["data"])
    # broker_info_df.columns = data["fields"]
    broker_info_df.columns = ["BrokerID", "Name", "Opening_Date", "Address", "Phone"]
    broker_info_df["BrokerID_HQ"] = broker_info_df["BrokerID"]
    broker_info_df["BrokerID_Fubon"] = broker_info_df["BrokerID"]
    all_broker_info_list = [broker_info_df]
    for idx, one_row in broker_info_df.iterrows():
        BrokerID = one_row["BrokerID"]
        res = requests.get(url = f"{TWSE_BROKER_SERVICE_AUDIT_URL}showType=list&stkNo={BrokerID}&response=json")
        data = res.json()
        if len(data["data"]) == 0:
            continue
        logger.info(f"BrokerID: {BrokerID} is on going")
        time.sleep(30)
        broker_branch_info_df = pd.DataFrame(data["data"])
        broker_branch_info_df.columns = ["BrokerID", "Name", "Opening_Date", "Address", "Phone"]
        ## convert BrokerID into the format acceptable by Fubon
        Fubon_BrokerID_list = [Fubon_BrokerID_conversion(str(one_BrokerID)) for one_BrokerID in broker_branch_info_df["BrokerID"].to_list()]
        broker_branch_info_df["BrokerID_HQ"] = BrokerID
        broker_branch_info_df["BrokerID_Fubon"] = Fubon_BrokerID_list
        all_broker_info_list.append(broker_branch_info_df)
    all_broker_info_df = pd.concat(all_broker_info_list)
    all_broker_info_df.to_csv(_default_path.joinpath("Broker_Info.csv"), index = False)
    return all_broker_info_df

if __name__ == "__main__":
    # stockid = "0050.TW"
    # st = datetime.date(2020, 1, 1)
    # ed = datetime.date(2021, 1, 1)
    # data_df = getData(stockid, st, ed, force = True)
    # print(data_df)

    getBrokerInfo()