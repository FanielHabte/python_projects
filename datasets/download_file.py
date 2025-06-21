import kaggle as kg

kg_api = kg.api
kg_api.authenticate()
kg_api.dataset_download_files('chaudharisanika/datasets', path='', unzip=True)

