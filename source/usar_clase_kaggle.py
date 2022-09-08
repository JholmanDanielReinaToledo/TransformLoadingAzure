# -*- coding: utf-8 -*-
"""
@author: Hugo Franco, Roberto Arias
"""
try:
    import os, sys
    from pathlib import Path as p


except Exception as exc:
            print('Module(s) {} are missing.:'.format(str(exc)))

dir_root = p(__file__).parents[1]
sys.path.append(str(p(dir_root) /'source' / 'clases'))

from cls_extract_data_mf import extract_data_mf as data_extractor
from cls_transform_data import transform_data as data_transformer


#%%
''' Crear una instancia de la clase (objeto) '''
extractor = data_extractor()
extractor.path = dir_root

transform=data_transformer()

#%%
'''Autenticación en el api de Kaggle'''
path_auth = str(p(extractor.path) / 'kaggle')
extractor.set_kaggle_api(path_auth)


#%%
''' Listar datasets'''
extractor.list_dataset_kaggle('youtube')
extractor.show_kaggle_datasets()

#%%
''' Descargar todos los archivos de un dataset alojado kaggle '''

path_data = str(p(extractor.path) / 'Dataset' / 'YouTube-New')
extractor.get_data_from_kaggle_d(path_data,'datasnaek/youtube-new')


#%%
''' Listar archivos según el tipo'''
path_data = str(p(extractor.path) / 'Dataset' / 'YouTube-New')
extractor.get_lst_files(path_data,'csv')
print('{}Instancia kaggle - archivos formato {}:'.format(os.linesep,'csv'))
extractor.muestra_archivos()

#%%
''' Ejemplo: Cargar datos "Canada" a memoria, i.e. CAvideos.csv'''
extractor.get_data_csv(extractor.lst_files[0])
extractor.data.drop_duplicates(subset='video_id', keep='last', inplace=True)


#%%
''' Mostrar datos para información'''
print(extractor.data)

#%%

transform.set_data(extractor.data.copy())
print("{}Nombres de las columnas:".format(os.linesep))
[print ("\'{}\'".format(fn)) for fn in transform.data.columns.values]


#%% 
transform.drop_columns(['thumbnail_link', 'comments_disabled', 'ratings_disabled', 'video_error_or_removed','description'])
print("{}Nombres de las columnas:".format(os.linesep))
[print ("\'{}\'".format(fn)) for fn in transform.data.columns.values]

#%%
transform.normalize_trending_date('trending_date')

#%%
output_data = str(p(extractor.path) / 'output' / 'youtube-transformed.csv')
print (output_data)
transform.save_data_csv(output_data)
