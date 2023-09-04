from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator 
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email
from airflow.operators.bash import BashOperator
from datetime import datetime


import requests
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import zipfile
import os

fato_atracacao = pd.DataFrame([])
fato_carga = pd.DataFrame([])

def unzip_file(path):
    with zipfile.ZipFile(path, 'r') as zip:
        zip.extractall(path='dados/')

def get_links(page, base_url):
    soup = BeautifulSoup(page, 'html.parser')
    results = soup.find_all('a', href=True)
    return [base_url+e['href'].lstrip('..') for e in results]

def _extract_data(**kwargs):
    date = kwargs['params']['start_date']
    end_date = kwargs['params']['end_date']
    xpath = '//*[@id="lista_arquivosea"]/table/tbody' 
    base_url = 'https://web3.antaq.gov.br/ea'
    url = base_url + '/sense/download.html#pt'
    date_selector = '//*[@id="anotxt"]'
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(accept_downloads=True)
        page.goto(url)

        while True:
            page.select_option(date_selector, value=str(date))
            articles = page.locator(f'xpath={xpath}')
            try:
                links = get_links(articles.inner_html(), base_url)
            except Exception as e:
                 return []        
            date = date+1
            if date>end_date:
                break
        browser.close()
    return len(links)

def _get_files():
    links = ti.xcom_pull(task_ids='extract_data')
    count_files = 0
    for url in links:
        path ='dados/' + url.split('/').pop()
        res = requests.get(url)
        if res.status_code == requests.codes.OK:
            with open(path, 'wb') as file:
                file.write(res.content)
            if '.zip' in path:
                unzip_file(path)
                os.remove(path)
            count_files+=1
    return count_files

def _links_validator(ti):
    links = ti.xcom_pull(task_ids='extract_data')
    if len(links)>0:
        return 'get_files'
    else:
        send_email(task_id='send_email', to=['josedossantos@outlook.com'], subject='ERROR ON DAG (ANTAQ) - Apache Airflow',
                   html_content=f"Os links extraídos estão vazios ou são inválidos.<br>Links: {links}.")
        return 'error_edata'

def _files_validator(ti):
    count_files = ti.xcom_pull(task_ids='get_files')
    if count_files==0:
        send_email(task_id='send_email', to=['josedossantos@outlook.com'], subject='ERROR ON DAG (ANTAQ) - Apache Airflow',
            html_content=f"Nenhum foi possível fazer o download dos arquivos.")
        return 'error_files'
    return 'generate_tables'

def _save_validator(ti):
    if not ti.xcom_pull(key='fato_carga'):
        return 'fail_carga'
    elif not ti.xcom_pull(key='fato_atracacao'):
        return 'fail_atracacao'
    return 'success'

def _save_tables(ti):
    global fato_carga,fato_atracacao
    if len(fato_atracacao)!=0:
        fato_atracacao.to_csv('fato_atracacao.csv',mode='a')
        # df.to_sql(fato_atracacao, con, if_exists='append')
    else:
        ti.xcom_push(key='fato_atracacao', value=False)

    if len(fato_carga)!=0:
        fato_carga.to_csv('fato_carga.csv',mode='a')
        # df.to_sql(fato_carga, con, if_exists='append')
    else:
        ti.xcom_push(key='fato_carga', value='save_tables')

def _generate_tables(**kwargs):
    global fato_atracacao, fato_carga
    year = kwargs['params']['start_date']
    end_date = kwargs['params']['end_date']    
    while(year>end_date):
        atracacao = pd.read_csv(f'dados/{year}Atracacao.txt', sep=';')
        t_atracacao = pd.read_csv(f'dados/{year}TemposAtracacao.txt', sep=';')
        fato_atracacao = pd.concat(fato_atracacao,pd.merge(atracacao,t_atracacao, on='IDAtracacao'))
        carga = pd.read_csv(f'dados/{year}Carga.txt', sep=';')
        carga['VLPesoCargaBruta'].str.replace(',','.').apply(float)
        carga = pd.merge(carga,atracacao[['Mes','Ano','IDAtracacao']], on='IDAtracacao')
        cc = pd.read_csv(f'dados/{year}Carga_Conteinerizada.txt', sep=';')
        cc['VLPesoCargaConteinerizada'] = cc['VLPesoCargaConteinerizada'].str.replace(',','.').apply(float)
        cc = cc.groupby('IDCarga').agg({'CDMercadoriaConteinerizada':lambda x: list(x), 'VLPesoCargaConteinerizada': lambda x:sum(x)}).reset_index()
        carga = pd.merge(carga, cc, on='IDCarga', how = 'left')
        carga['Peso líquido da carga'] = carga.apply(lambda x: x['VLPesoCargaConteinerizada'] if x['Carga Geral Acondicionamento'] == 'Conteinerizada' else x['VLPesoCargaBruta'],axis=1)
        carga['CDMercadoria'] = carga.apply(lambda x: x['CDMercadoriaConteinerizada'] if x['Carga Geral Acondicionamento'] == 'Conteinerizada' else x['CDMercadoria'],axis=1)
        fato_carga = pd.concat(fato_carga,carga.drop(['CDMercadoriaConteinerizada','VLPesoCargaConteinerizada'],axis=1))
        year+=1


with DAG(dag_id='ANTAQ', start_date= datetime(2023, 9, 2), catchup=False) as dag:

    extract_data = PythonOperator(task_id='extract_data', python_callable =_extract_data, params={'start_date':2017, 'end_date': 2019})
    get_files = PythonOperator(task_id='get_files', python_callable =_get_files)
    error_edata = BashOperator(task_id= 'Error_edata', bash_command='echo "ERROR TO EXTRACT DATA"')
    error_files = BashOperator(task_id= 'Error_files', bash_command='echo "ERROR TO DOWNLOAD DATA"')
    fail_carga = EmailOperator(task_id= 'fail_carga', to=['josedossantos@outook.com'], subject='ERROR ON DAG (ANTAQ) - Apache Airflow', html_content='Nenhum registro foi armazenado na tabela "FATO_CARGA".')
    fail_atracacao = EmailOperator(task_id= 'fail_atracacao', to=['josedossantos@outook.com'], subject='ERROR ON DAG (ANTAQ) - Apache Airflow', html_content='Nenhum registro foi armazenado na tabela "FATO_ATRACACAO"')
    success = EmailOperator(task_id= 'success', to=['josedossantos@outook.com'], subject='SUCCESS ON DAG (ANTAQ) - Apache Airflow', html_content='Todo o processo foi realizado com sucesso.')
    links_validator = BranchPythonOperator(task_id='links_validator', python_callable =_links_validator)
    files_validator = BranchPythonOperator(task_id='files_validator', python_callable =_files_validator)
    generate_tables = PythonOperator(task_id='generate_tables', python_callable =_generate_tables)
    save_tables = PythonOperator(task_id='save_tables', python_callable =_save_tables)
    save_validator = BranchPythonOperator(task_id='save_validator', python_callable =_save_validator)


    extract_data >> links_validator >> [get_files, error_edata] >> files_validator >> [generate_tables, error_files] >> save_tables >> save_validator >> [success, fail_carga, fail_atracacao]


