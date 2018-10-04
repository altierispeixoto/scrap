from scraping import *

# Crédito consignado INSS
scrap = BacenScraper()
scrap.run_scrap('./data', '218', './data/consig_inss.csv')  # cdmodalidade = "218"
