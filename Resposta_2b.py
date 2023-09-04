import requests
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import zipfile
import os

xpath = '//*[@id="lista_arquivosea"]/table/tbody' 
base_url = 'https://web3.antaq.gov.br/ea'
url = base_url + '/sense/download.html#pt'
date_selector = '//*[@id="anotxt"]'
start_date = 2017
end_date = 2019

def unzip_file(path):
    with zipfile.ZipFile(path, 'r') as zip:
        zip.extractall(path='dados/')

def get_links(page):
    soup = BeautifulSoup(page, 'html.parser')
    results = soup.find_all('a', href=True)
    return [base_url+e['href'].lstrip('..') for e in results]

def get_file(url):
        path ='dados/' + url.split('/').pop()
        res = requests.get(url)
        if res.status_code == requests.codes.OK:
            with open(path, 'wb') as file:
                file.write(res.content)
            if '.zip' in path:
                unzip_file(path)
                os.remove(path)

def save_table(df, table):
    # df.to_sql(table, con, if_exists='append')
    df.to_csv(f'{table}.csv',mode='a')
    print(f'table {table} saved')

def generate_and_save_tables(year):
    atracacao = pd.read_csv(f'dados/{year}Atracacao.txt', sep=';')
    t_atracacao = pd.read_csv(f'dados/{year}TemposAtracacao.txt', sep=';')
    fato_atracacao = pd.merge(atracacao,t_atracacao, on='IDAtracacao')
    save_table(fato_atracacao, 'fato_atracacao')
    carga = pd.read_csv(f'dados/{year}Carga.txt', sep=';')
    carga['VLPesoCargaBruta'].str.replace(',','.').apply(float)
    carga = pd.merge(carga,atracacao[['Mes','Ano','IDAtracacao']], on='IDAtracacao')
    cc = pd.read_csv(f'dados/{year}Carga_Conteinerizada.txt', sep=';')
    cc['VLPesoCargaConteinerizada'] = cc['VLPesoCargaConteinerizada'].str.replace(',','.').apply(float)
    cc = cc.groupby('IDCarga').agg({'CDMercadoriaConteinerizada':lambda x: list(x), 'VLPesoCargaConteinerizada': lambda x:sum(x)}).reset_index()
    carga = pd.merge(carga, cc, on='IDCarga', how = 'left')
    carga['Peso líquido da carga'] = carga.apply(lambda x: x['VLPesoCargaConteinerizada'] if x['Carga Geral Acondicionamento'] == 'Conteinerizada' else x['VLPesoCargaBruta'],axis=1)
    carga['CDMercadoria'] = carga.apply(lambda x: x['CDMercadoriaConteinerizada'] if x['Carga Geral Acondicionamento'] == 'Conteinerizada' else x['CDMercadoria'],axis=1)
    fato_carga = carga.drop(['CDMercadoriaConteinerizada','VLPesoCargaConteinerizada'],axis=1)
    save_table(fato_carga, 'fato_carga')

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(accept_downloads=True)
    page.goto(url)

    while True:
        date = str(start_date)
        page.select_option(date_selector, value=date)
        articles = page.locator(f'xpath={xpath}')
        try:
            links = get_links(articles.inner_html())
        except Exception as e:
            print(f'{e}')
        with open('links.txt', 'a') as file:
            file.write('\n'.join(links)) 
        for e in links:
            get_file(e)
        generate_and_save_tables(start_date)
        start_date = start_date+1
        if start_date>end_date:
            break
    browser.close()
print(f'Files extracted and saved from {url}')