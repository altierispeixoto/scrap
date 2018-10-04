from scraping import *

# Crédito consignado Público
scrap = BacenScraper()
scrap.run_scrap('./data', '220', './data/consig_pub.csv')  # cdmodalidade = "220"  # Crédito consignado Público
