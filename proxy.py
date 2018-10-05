import requests
from itertools import cycle
import pandas as pd
from random import randint
from time import sleep
from random import choice


class ScrapBase:

    def __init__(self, url):
        self.url = url
        self.proxies = self.get_proxy_list()
        self.proxy_pool = cycle(self.proxies)
        self.whitelist = set()
        self.blacklist = set()

    @staticmethod
    def random_headers():

        desktop_agents = [
            # Chrome
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
            'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
            # Firefox
            'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
            'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
            'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
            'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
        ]

        return {'User-Agent': choice(desktop_agents)}

    def get_proxy_list(self):
        url_proxy = 'https://free-proxy-list.net/'
        header = self.random_headers()
        response = requests.get(url_proxy, headers=header)
        df = pd.read_html(response.text)
        df_proxies = df[0]
        df_proxies.columns = ['ip_address', 'port', 'code', 'country', 'anonymity','google', 'https', 'last_checked']
        df_proxies['port'] = df_proxies['port'].astype(str).str[:-2]
        df_proxies['address'] = df_proxies['ip_address']+':'+df_proxies['port']
        proxy_ips = df_proxies[(df_proxies.anonymity == 'elite proxy') & (df_proxies.https == 'yes')].address.values.T.tolist()
        return proxy_ips


    def url_call(self):

        status_code = 0
        cnt_blacklist = 0
        while status_code != 200:
            proxy = next(self.proxy_pool)

            while proxy in self.blacklist:
                proxy = next(self.proxy_pool)
                cnt_blacklist += 1
                if cnt_blacklist == 5:
                    proxy = choice(self.whitelist)
                    cnt_blacklist = 0

            header = self.random_headers()
            response, proxy = self.req(proxy, header)
            if response.status_code != 200:
                self.blacklist.add(proxy)
            else:
                self.whitelist.add(proxy)
                return response


    def req(self, proxy, header):
        try:
            print("tentando com proxy {}".format(proxy))
            response = requests.get(self.url, headers=header, proxies={"https": proxy})
            status_code = response.status_code
            print("proxy {} retornou status code {}".format(proxy, status_code))
        except Exception as err:
            print(err)
            return response, proxy
        return response, proxy


# url = 'https://www.bcb.gov.br/api/relatorio/pt-br/txjuros?path=conteudo/txcred/Reports/TaxasCredito-Consolidadas-porTaxasAnuais-Historico.rdl&parametros=undefined'
# scrap = ScrapBase(url)
# response_call = scrap.url_call()
# print(response_call.json())
