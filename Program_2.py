from mrjob.job import MRJob
from mrjob.step import MRStep
import re
#from stop_words import get_stop_words
import math

WORD_REGEXP = re.compile(r"[\w']+")
Stopwords = ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves']
#Stopwords =get_stop_words('english')
reviewNum = 1
threshold = 0.15
TotalReviews = 550

class MRWordFrequencyCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_1,
                   reducer=self.reducer_1),
            MRStep(mapper=self.mapper_2,
            reducer=self.reducer_2)
        ]
    
    def mapper_1(self, _, line):
        global reviewNum
        words = WORD_REGEXP.findall(line)
        totalWords=len(words)
        stopWordsRemoved = [x.lower() for x in words if x.lower() not in Stopwords]
        uniqueWords = set(stopWordsRemoved)
        for word in uniqueWords:
            termCount = 0
            for term in stopWordsRemoved:
                if word == term:
                    termCount += 1
            
            tf = termCount/totalWords
            yield word, (reviewNum, tf) # lower function is used to coverst all the word to lower case, if try to run code without these similaritites, unique keys are not grouped together
            
        reviewNum += 1
          
    def mapper_2(self,key, values):
        yield key, values
         
                
    def reducer_1(self, key, values):
        global TotalReviews
        mylist = list(values)
        term = key
       # print(term, mylist)# used to print tdidf
        ni = len(mylist)
        N = TotalReviews
        idf = 1
        idf = math.log(N/ni,2)
        tfdict = {}
       
        for x in mylist:
            doc = x[0]
            tf = x[1]
            score = tf*idf
            if(score != 0):
                
                tfdict[doc] = score
        
        for doc, score in tfdict.items():### validate
            for y in range(1,N+1):
                if(doc != y):
                    if y in tfdict.keys():
                        if(doc < y):
                            outkey   = (doc, y)
                            outvalue = (score, tfdict[y])
                            yield outkey, outvalue
                            
                    elif(doc < y):
                        outkey = (doc, y)
                        outvalue = (score, 0)
                        yield outkey, outvalue
                        
                    else:
                        outkey = (y, doc)
                        outvalue = (0, score)
                        yield outkey, outvalue
                
    def reducer_2(self, key, values):# doc has term, y is the variable for every other doc with does not has term
        global threshold
        keylist = list(key)
        tmp = list(values)
    #     if(keylist[0] == 5 and keylist[1]==6 ):
    #          print("-----------",tmp)
    # 
        dotproduct = 0
        mag1 = 0
        mag2 = 0
        sim = 0
      
        for x in tmp:
            v1 = x[0]
            v2 = x[1]
            dotproduct += v1 * v2
          
            mag1 += v1**2
            mag2 += v2**2
        
        denominator = math.sqrt(mag1*mag2)
        # if(keylist[0] == 5 and keylist[1]==6 ):
        #       print("dotprod",dotproduct, mag1, mag2)
        
        if(denominator != 0):
            sim = dotproduct/denominator
            # if(keylist[0] == 5 and keylist[1]==6 ):
            #     print("sim",sim)
            
            #check threshold
            if(sim > threshold):
            #     yield key, sim
            
                yield key, sim
        else:
            print("completely different documents:", keylist,(mag1, mag2))
    #     
        

        

  


if __name__ == '__main__':
    MRWordFrequencyCount.run()
