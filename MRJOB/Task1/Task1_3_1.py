# Task1-3-1
import urllib
import nltk
import xml.etree.ElementTree as ET
import sys
import logging
from bs4 import BeautifulSoup
from mrjob.step import MRStep
from mrjob.job import MRJob
class MRmyjob (MRJob):
    
    def mapper1(self,_,line):
    
       # change xml line input to suitable format
        try:
            if line.find('<?xml') == -1:
                if line.find('<posts>') == -1:
                        
                        root = ET.fromstring(line)                       
                        if root.tag == 'row':
                            id = root.get('OwnerUserId')
                            body = root.get('Body')  
                            mystring = BeautifulSoup(body)
                            body_string = mystring.get_text() 
                            if "socket" in body_string:               
                                yield str(id), 1
                                            
        except: 
             yield 'er',-1 

    def combiner(self, key,value):
        yield key,sum(value)

    def reducer1(self,key,list_of_values):       
       yield None,(sum(list_of_values),key)
    
    def reducer2(self,_,list_of_values):
        N = 10
        try:
            value = sorted(list(list_of_values),reverse=True)
            value_N = value[:N]
            return value_N
        except:
            return -1

    def steps(self): 
        return [MRStep(mapper=self.mapper1, reducer=self.reducer1),MRStep(reducer=self.reducer2)]   
       
if __name__ == '__main__':
    MRmyjob.run()