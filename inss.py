from scraping import *

# Crédito consignado INSS
scrap = BacenScraper()
codes = ['218', '220'] # 218 INSS, 220 Publico
filepath = '/home/altieris/git_repositories/ga-feat-sel/src/scrap_bacen/data'
scrap.run_scrap(codes,filepath,'2018-07-02')
