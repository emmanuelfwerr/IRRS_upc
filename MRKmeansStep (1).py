"""
.. module:: MRKmeansDef

MRKmeansDef
*************

:Description: MRKmeansDef

    

:Authors: bejar
    

:Version: 

:Created on: 17/07/2017 7:42 

"""

from mrjob.job import MRJob
from mrjob.step import MRStep

__author__ = 'bejar'


class MRKmeansStep(MRJob):
    prototypes = {}

    def jaccard(self, prot, doc):
        """
        Compute here the Jaccard similarity between  a prototype and a document
        prot should be a list of pairs (word, probability)
        doc should be a list of words
        Words must be alphabeticaly ordered

        The result should be always a value in the range [0,1]
        """
        return 1

    def configure_args(self):
        """
        Additional configuration flag to get the prototypes files

        :return:
        """
        super(MRKmeansStep, self).configure_args()
        self.add_file_arg('--prot')

    def load_data(self):
        """
        Loads the current cluster prototypes
        cluster1: lol+100, edf+234
        :return:
        there is a file for the prototypes 

        """
        f = open(self.options.prot, 'r')
        for line in f:
            #we loop threw each line because each line is a different cluster 
            cluster, words = line.split(':')
            #split cluster1(prototype1) and the words of this prototype1
            cp = []
            for word in words.split():
                #loop all the words in the prototype1 and append in the list the word  in position 0(for example :have)+frequency of this word in position 1
                cp.append((word.split('+')[0], float(word.split('+')[1])))
                #in the list prototypes, we will have key:cluster1 , and the cp is  lol+100, edf+234
                #prototypes look like :  cluster1: lol+100, edf+234,....
            self.prototypes[cluster] = cp

    def assign_prototype(self, _, line):
        """
        This is the mapper it should compute the closest prototype to a document

        Words should be sorted alphabetically in the prototypes and the documents

        This function has to return at list of pairs (prototype_id, document words)
 
        You can add also more elements to the value element, for example the document_id
        
        """
        
        #in input, each line is the document : doc1:word1,word2,word3....
        #  
        # Each line is a string docid:wor1 word2 ... wordn
        doc, words = line.split(':')
        lwords = words.split()
        #we calculate here distance jaccard between each line (each document) in the input and the prototypes in the prototypes list
        #if the distance is inferior to min_dist (we fix a min dist before)
        #min_dist become hthe new dist jaccard

        min = float('inf') 
        #current_clust=999
        current_clust=None  
        for clusterId,prototype in self.prototypes.items():
            similarity_score=self.jaccard(prototype,lwords)
            if(similarity_score < min):
                min=similarity_score
                #update the new cluster assigned to the document
                current_clust=clusterId
        # Return pair key, value
        #yield cluster and point = [document,words]
        yield current_clust, (doc,words)

    def aggregate_prototype(self, key, values):
        """
        input is cluster and all the documents it has assigned
        #WHERE DO WE GET THE INPUT FROM ?DO WE GET THE UNPU FROM THE MMAPPER?
        Outputs should be at least a pair (cluster, new prototype)

        It should receive a list with all the words of the documents assigned for a cluster

        The value for each word has to be the frequency of the word divided by the number
        of documents assigned to the cluster

        Words are ordered alphabetically but you will have to use an efficient structure to
        compute the frequency of each word

        :param key:
        :param values:
        :return:
        """
        #INPUT :(cluster id,documents of this cluster)
        #for a cluster : receive list of all words of the documents in this cluster 
        #value of each word=frequency of word/number of documents in the cluster 
        #efficient 
        #OUTPUT:(cluster id,new prototype/centroid)
        #values are going to be the pairs [document, words] resulting from the mapper in the output cluster and point = [document,words]
        #we create a dictionnary for the word and its frequency
        freq_prototype={}
        #create a list to save documents  assigned to a prototype
        documents_prototype=[]
        #iterate over each values (input) 'doc,words'
        #we have to calculate the frequency of words in the document (values)
        #iterate over the words in the list and append the mean of this word
        for (document,words) in values:
            documents_prototype.append(document)
            for word in words:
                if word in freq_prototype:
                    #if the word is already present in the freq dictonnary , we add +1 to the current frequency of the word
                    freq_prototype[word]=freq_prototype[word]+1.0
                else:
                    #if the word shows for the first time, frequency=1
                    freq_prototype[word]=1.0
            


        #calculate the mean by :frequency of word divided by total number of documents
        #number of total document is
        number_documents=float(len(values))
        #we should iterate over the freq dictionnary to compute the mean (frequency of word divided by total number of documents) for each word in dictionnary
        mean_freq={}
        for word,frequency in freq_prototype:
            mean_freq[word]=frequency/number_documents
        #maybe it is better to have a list of mean_freq and not a dictionnary
        #in this case we will have the following code
        #mean_freq=[]
        # for word,frequency in freq_prototype:
            #mean_freq.append(word,frequency/number_documents)
        

        # output is (cluster,new prototype)    
        yield key, (documents_prototype,freq_prototype)

    def steps(self):
        return [MRStep(mapper_init=self.load_data, mapper=self.assign_prototype,
                       reducer=self.aggregate_prototype)
            ]


if __name__ == '__main__':
    MRKmeansStep.run()