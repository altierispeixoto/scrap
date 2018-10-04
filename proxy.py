import requests
from itertools import cycle
import pandas as pd

# import ssl
# from functools import partial
# ssl.wrap_socket = partial(ssl.wrap_socket, ssl_version=ssl.PROTOCOL_TLSv1)


def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    df = pd.read_html(response.text)
    df_proxies = df[0]
    df_proxies.columns = ['ip_address', 'port', 'code', 'country', 'anonymity','google', 'https', 'last_checked']
    df_proxies['port'] = df_proxies['port'].astype(str).str[:-2]
    df_proxies['address'] = df_proxies['ip_address']+':'+df_proxies['port']
    proxy_ips = df_proxies[(df_proxies.anonymity=='elite proxy') & (df_proxies.https == 'yes')].address.values.T.tolist()
    return proxy_ips


def call_url(url, proxies):
    proxy_pool = cycle(proxies)
    status_code = 0
    while status_code != 200:
        proxy = next(proxy_pool)
        print("tentando com proxy {}".format(proxy))
        response = requests.get(url, proxies={"https": proxy})
        status_code = response.status_code
        print("proxy {} retornou status code {}".format(proxy,status_code))
    return response

url = 'https://www.bcb.gov.br/api/relatorio/pt-br/txjuros?path=conteudo/txcred/Reports/TaxasCredito-Consolidadas-porTaxasAnuais-Historico.rdl&parametros=undefined'

proxies = get_proxies()
response_call = call_url(url, proxies)
print(response_call.json())