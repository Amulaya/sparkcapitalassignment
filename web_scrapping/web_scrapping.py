import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine

req_headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Host": "www.bseindia.com",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/114.0"
}
result = {'Deal_Date': [], 'Security_Code': [], 'Security_Name': [], 'Client_Name': [], 'Deal_Type': [], 'Quantity': [],
          'Price': []}
res = requests.get("https://www.bseindia.com/markets/equity/EQReports/bulk_deals.aspx", headers=req_headers)

soup = BeautifulSoup(res.text, 'html.parser')
table = soup.find('table', {'id': 'ContentPlaceHolder1_gvbulk_deals'})
rows = table.find_all('tr')[1:]

# Iterate over each row and extract the data
for row in rows:
    cells = row.find_all('td')
    result['Deal_Date'].append(cells[0].text)
    result['Security_Code'].append(cells[1].text)
    result['Security_Name'].append(cells[2].text)
    result['Client_Name'].append(cells[3].text)
    result['Deal_Type'].append(cells[4].text)
    result['Quantity'].append(cells[5].text)
    result['Price'].append(cells[6].text)

# create a dataframe to store data and then pushhing it to the daily_run_data table
df = pd.DataFrame.from_dict(result)


engine = create_engine('mysql+pymysql://root:password@localhost:3306/test')

df.to_sql(con=engine, name='daily_run_data', if_exists='append', index=False)
