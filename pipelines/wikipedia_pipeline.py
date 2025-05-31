import json

import requests
import pandas as pd


NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'
def get_wikipedia_page(url):
    import requests

    print("Getting wikipedia page...",url)

    try:
        response = requests.get(url,timeout=10,verify=False)
        response.raise_for_status() #chech if request is successfull or not

        return response.text
    except requests.RequestException as e:
        print(f"An error occurred {e}")





def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup=BeautifulSoup(html,"html.parser")
    table=soup.find("table",attrs={"class":"wikitable sortable sticky-header"})
    table_rows=table.find_all('tr')
    return table_rows

def clean_text(text):
    text=str(text).strip()
    text=text.replace("&nbsp","")
    if text.find('♦'):
        text=text.split('♦')[0]
    if text.find('[')!=-1:
        text=text.split('[')[0]
    if text.find(' (formerly)')!=-1:
        text=text.split(' (formerly)')[0]
    text.replace('\xa0', '')
    return text.replace('\n','')


def extract_wikipedia_data(**kwargs):
    import pandas as pd
    url=kwargs['url']
    html=get_wikipedia_page(url)
    rows=get_wikipedia_data(html)
    print(rows)
    data=[]
    for i in range(1,len(rows)):
        tds=rows[i].find_all('td')
        values={
            'rank':i,
            'stadium':clean_text(tds[0].text),
            'capacity':clean_text(tds[1].text).replace(',',''),
            'region':clean_text(tds[2].text),
            'country':clean_text(tds[3].text),
            'city':clean_text(tds[4].text),
            'images':'https://'+tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team':clean_text(tds[6].text),

        }

        data.append(values)

    # data_df=pd.DataFrame(data)
    # data_df.to_csv('data/wikipedia_data.csv',index=False)
    json_rows=json.dumps(data)
    kwargs['ti'].xcom_push(key='rows',value=json_rows)
    return "OK"


def transform_wikipedia_data(**kwargs):

    data=kwargs['ti'].xcom_pull(key='rows',task_ids='extract_wikipedia_data')
    data=json.loads(data)

    stadiums_df=pd.DataFrame(data)

    stadiums_df['images']=stadiums_df['images'].apply(lambda x:x if x not in ['NO_IMAGE','',None] else NO_IMAGE)
    stadiums_df['capacity']=stadiums_df['capacity'].astype(int)



    kwargs['ti'].xcom_push(key='rows',value=stadiums_df.to_json())

    return "OK"

def write_wikipedia_data(**kwargs):
    from datetime import datetime
    data=kwargs['ti'].xcom_pull(key='rows',task_ids='transform_wikipedia_data')
    data=json.loads(data)
    data=pd.DataFrame(data)

    file_name=("stadium_cleaned"+str(datetime.now().date())
               + "_" + str(datetime.now().time()).replace(":","_")+".csv")
    data.to_csv('data/' + file_name, index=False)
    data.to_csv('abfs://footballdataeng@footballdataengsa.dfs.core.windows.net/data/'+file_name,
                storage_options={
                    'account_key':'QN+Un6q8wWYNDhEs/yo1jY6xh6ykY9MGIZL73feNr6q6/QkLUYvmYrmnDqvm1oCPjjlUHOkSPN2t+AStS4E6Aw=='
                },index=False
                )
   #QN+Un6q8wWYNDhEs/yo1jY6xh6ykY9MGIZL73feNr6q6/QkLUYvmYrmnDqvm1oCPjjlUHOkSPN2t+AStS4E6Aw==
    return "OK"




