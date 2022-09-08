# -*- coding: utf-8 -*-
__author__ = "Hugo Franco, Roberto Arias"
__maintainer__ = "Asignatura: Data Analytics - ETL"
__copyright__ = "Copyright 2021 - Asignatura Big Data"
__version__ = "0.0.1"

try:
    #from pathlib import Path as p
    import pandas as pd
    from datetime import datetime
    import os
    
    
except Exception as exc:
            print('Module(s) {} are missing.:'.format(str(exc)))

#%%
class transform_data(object):
    def __init__(self, path=None,percent=None):
        self.data = None
        self.status = False
        self.extractor = None
   
#%%    
    def set_data(self, data=None, path=None):
        if path is None:
            self.data=data            
        else:
            try:
                self.data=pd.read_csv(path)
            except Exception as exc:
                self.show_error(exc)
 
#%%    
    def drop_columns(self, col_list):
        if len(col_list)>0:
               self.data=self.data.drop(columns=col_list)
               print("columnas borradas")
 
#%%    
    def normalize_trending_date(self, column_name):
        self.data[column_name]=self.data[column_name].apply(lambda date:self.format_date_us_to_latam(str('20'+date)))
 
#%%    
    def format_date_us_to_latam(self, date_str):
        return datetime.strptime(date_str, "%Y.%d.%m").strftime('%d/%m/%Y')
  
#%%   
    def save_data_csv(self, path):
        if self.data is not None:
            self.data.to_csv(path)
 
#%%
    # Control de excepciones
    def show_error(self,ex):
        '''
        Captura el tipo de error, su description y localización.

        Parameters
        ----------
        ex : Object
            Exception generada por el sistema.

        Returns
        -------
        None.

        '''
        trace = []
        tb = ex.__traceback__
        while tb is not None:
            trace.append({
                          "filename": tb.tb_frame.f_code.co_filename,
                          "name": tb.tb_frame.f_code.co_name,
                          "lineno": tb.tb_lineno
                          })
            
            tb = tb.tb_next
            
        print('{}Something went wrong:'.format(os.linesep))
        print('---type:{}'.format(str(type(ex).__name__)))
        print('---message:{}'.format(str(type(ex))))
        print('---trace:{}'.format(str(trace)))
             
