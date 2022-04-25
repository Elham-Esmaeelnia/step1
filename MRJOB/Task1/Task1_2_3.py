# Task1-2-3
import re
WORD_RE = re.compile(r"[\w']+")
from datetime import datetime
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep
from mrjob.job import MRJob
class MRmyjob (MRJob):
 
    def mapper1(self,_,line):
            data = line.split() 
            if len(data)> 9 :    
                #parse line
                ip = data[0].strip()
                date = data[3].strip()
                url = data[6].strip()
                if  len(url) > 1 :
                    #Extract year from data
                    year = date[8:12]
                    month = date[4:7]
                    day = date[1:3]
                    time = date[13:21]
                    if year.isdigit() and len(year) > 3:                 
                            yield (year,month,day,ip),time
    
    def reducer1(self,key,list_of_values): 
        
        day_list = list(list_of_values)
        s1 = day_list[0]
        s2 = day_list[-1]
        FMT = '%H:%M:%S'
        tdelta = datetime.strptime(s2, FMT) - datetime.strptime(s1, FMT) 
        tdelta_sec =  tdelta.total_seconds() 
        yield (key[0],key[1],key[3]), tdelta_sec 
        
    def reducer2(self,key,list_of_values):       
        yield (key[0]),(sum(list((list_of_values))),key[2]) 
    
    def reducer3(self,key,list_of_values):
        N = 10
        value = sorted(list(list_of_values),reverse=True)      
        value_N = value[:N]
        
        yield key,value_N
    def steps(self):
        return [MRStep(mapper=self.mapper1,reducer=self.reducer1),MRStep(reducer=self.reducer2),MRStep(reducer=self.reducer3)]


if __name__ == '__main__':
    MRmyjob.run()