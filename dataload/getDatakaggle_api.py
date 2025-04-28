import loadSparkSession as load

#https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020
#https://www.youtube.com/watch?v=DgGFhQmfxHo

#!pip install kaggle

import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi

api=KaggleApi()
api.authenticate()
destination_path = './Kaggle'

# download all csv 
api.dataset_download_files('rohanrao/formula-1-world-championship-1950-2020', path=destination_path, unzip=True)

