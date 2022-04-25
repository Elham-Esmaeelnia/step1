# Task1-1-2
from unidecode import unidecode
import nltk
#nltk.download('all-nltk')
nltk.data.path.append("/home/elham/nltk_data")
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import string
from mrjob.step import MRStep
from mrjob.job import MRJob
class MRmyjob (MRJob):
    def mapper1(self,_,line):
        line = unidecode(line)
        tokens = word_tokenize(line)
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
            yield (word, 1)
    def combiner(self, key,value):
        yield key,sum(value)
    def reducer1(self,key,value):
        total_count = sum(value)
        yield None,(total_count,key)
    def reducer2(self,_,value):
        N = 20
        value = sorted(list(value),reverse=True)
        return value[:N]
    def steps(self):
        return [MRStep(mapper=self.mapper1,reducer=self.reducer1),MRStep(reducer=self.reducer2)]
if __name__ == '__main__':
    MRmyjob.run()