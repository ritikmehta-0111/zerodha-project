from urllib.request import urlopen as uReq
from django.shortcuts import render
import pandas as pd
import zipfile
import requests
import re
import bs4
import redis
import os

# r = redis.from_url(url, port=6379, db=0, decode_responses=True)
r_server = redis.from_url(os.environ.get("REDIS_URL"))

#r_server = redis.Redis("localhost",port=6379)

my_url = "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx"
uclient = uReq(my_url)
page_html = uclient.read()
uclient.close()

soup = bs4.BeautifulSoup(page_html, "html.parser")
link = soup.find_all('ul', {'class': 'ullist'})[0].find('a')

# Extracting a link for zip file
# <a href="http://www.bseindia.com/download/BhavCopy/Equity/EQ201219_CSV.ZIP" id="ContentPlaceHolder1_btnhylZip"
# target="_self">Equity - 20/12/2019</a>
reg_pat = '<a href="(.*?)"(.*?)(?=</)'
link = re.findall(reg_pat, str(link))

csv_url = str(link[0][0])
r = requests.get(csv_url)


with open("data_equity.zip", "wb")as f:
    f.write(r.content)

with zipfile.ZipFile("data_equity.zip", "r") as data_zip:
    csv_file_name = data_zip.namelist()
    data_zip.extractall()

# data_1 = pd.read_csv("C:/Users/Yash/PycharmProjects/untitled3/django/trial/EQ241219.CSV")

data_1 = pd.read_csv(csv_file_name[0])
df = pd.DataFrame(data_1)
#stock_dict = {k1.strip(): [v1, v2, v3, v4, v5] for k1, v1, v2, v3, v4, v5 in zip(df["SC_NAME"], df["SC_CODE"], df["OPEN"], df["HIGH"], df["LOW"], df["CLOSE"])}

for k1, v1, v2, v3, v4, v5 in zip(df["SC_NAME"], df["SC_CODE"], df["OPEN"], df["HIGH"], df["LOW"], df["CLOSE"]):
    r_server.hset(k1.strip(), "name", k1)
    r_server.hset(k1.strip(), "code", v1)
    r_server.hset(k1.strip(), "open", v2)
    r_server.hset(k1.strip(), "high", v3)
    r_server.hset(k1.strip(), "low", v4)
    r_server.hset(k1.strip(), "close", v5)


df = df.sort_values('HIGH', ascending=False)
df = df[["SC_CODE", "SC_NAME", "OPEN", "HIGH", "LOW", "CLOSE"]][:10]
#print(df)

stock_by_name = [[v1.strip(), v2, v3, v4, v5, v6] for v1, v2, v3, v4, v5, v6 in zip(df["SC_NAME"], df["SC_CODE"], df["OPEN"], df["HIGH"], df["LOW"], df["CLOSE"])]
#print(stock_by_name)
result = {}


def zerodha(request):
    return render(request, "view.html", {"stock_by_name_dict": stock_by_name})


def search(request):
    q = ""
    try:
        q = request.GET["query"]
        result.clear()
        d = r_server.hgetall(q.strip())
        #print(d)
        result[d[b'name'].decode("utf-8")] = [d[b'code'].decode("utf-8"), d[b'open'].decode("utf-8"), d[b'high'].decode("utf-8"), d[b'low'].decode("utf-8"), d[b'close'].decode("utf-8")]
        print(result)

        alldata = {"result": result, "stock_by_name_dict": stock_by_name}
        return render(request, "view.html", alldata)


    except:
        s = str(q) + "  DATA is not present in Database please enter valid data"
        alldata = {"s": s, "stock_by_name_dict": stock_by_name}
        return render(request, "view.html", alldata)
