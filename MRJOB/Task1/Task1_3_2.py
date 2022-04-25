# Task1-3-2
import re
WORD_RE = re.compile(r"[\w']+")
import urllib
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import string
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
                            id = root.get('Id')
                            body = root.get('Body')  
                            mystring = BeautifulSoup(body)
                            body_string = mystring.get_text() 
                            if "socket" in body_string:               
                              
                                    tokens = word_tokenize(body_string)
                                    #convert to lowercase
                                    tokens = [w.lower() for w in tokens]
                                    # remove punctuation from each word      
                                    table = str.maketrans('','',string.punctuation)
                                    stripped = [w.translate(table) for w in tokens]
                                    # remove remaining tokens that are not alphabetic
                                    words = [word for word in stripped if word.isalpha()]
           
                                    # filter out stop words
                                    stop_words = set(stopwords.words('english'))
                                    words = [w for w in words if not w in stop_words]

                                    for word in words:
                                        yield (str(id),word) , 1
                        
                        
        except: 
             yield 'er',-1 
    
    def combiner(self, key,value):
        yield key,sum(value)

    def reducer1(self,key,list_of_values):       
       yield key[0],(sum(list_of_values),key[1])
    
    def reducer2(self,key,list_of_values):
        N = 10
        try:
            value = sorted(list(list_of_values),reverse=True)
            value_N = value[:N]
            word_list = [value_w[1] for value_w in value_N]
            yield key , word_list
        except:
            return -1
    
    def steps(self):      
        return [MRStep(mapper=self.mapper1, reducer=self.reducer1),MRStep(reducer=self.reducer2)]   
       
if __name__ == '__main__':
    MRmyjob.run()