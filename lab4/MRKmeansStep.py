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
        prot_set = set([tupl[0] for tupl in set(prot)])

        intersection = len(list(prot_set.intersection(doc)))
        union = (len(prot_set) + len(set(doc))) - intersection

        return float(intersection) / union


    def in_prototype(self, key, word):
        for clusterId, prototype in self.prototypes.items():
            if clusterId != key:
                continue
            if word in [tupl[0] for tupl in set(prototype)]:
                return True
        return False


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

        :return:
        """
        f = open(self.options.prot, 'r')
        for line in f:
            cluster, words = line.split(':')
            cp = []
            for word in words.split():
                cp.append((word.split('+')[0], float(word.split('+')[1])))
            self.prototypes[cluster] = cp


    def assign_prototype(self, _, line):
        """
        This is the mapper it should compute the closest prototype to a document

        This function has to return a list of pairs (prototype_id, document words)
        """
        # Each line is a string docid:wor1 word2 ... wordn
        doc, words = line.split(':')
        lwords = words.split()

        max_score = float('-inf')
        assigned_cluster = None  
        for clusterId, prototype in self.prototypes.items():
            jaccard_score = self.jaccard(prototype, lwords)
            if (jaccard_score > max_score):
                max_score = jaccard_score
                assigned_cluster = clusterId # update the new cluster assigned to the document

        yield (assigned_cluster, (doc, lwords))


    def aggregate_prototype(self, key, values):
        '''
        Returns new re-calculated prototypes for each clusterId in the form of 
        a pair (clusterId, new_prototype), where new_prototype is a list

                Parameters:
                        self (): ...
                        key (str): assigned clusterId of doc by mapper
                        values (tuple): where value[0] = docId, and value[1] = list of words contained in doc

                Returns:
                        a pair (key, (assigned_docs, new_prototype)) where:
                                - key = clusterId
                                - assigned_docs = list of all docId assigned to clusterId
                                - new_prototype = list of top words by sorted by descending frequency/n_docs
        '''

        assigned_docs = [] # list to save documents assigned to a prototype
        word_freq = {} # dict to save words and their frequencies
        # iterate through input and count word frequencies
        for (doc, lwords) in values:
            assigned_docs.append(doc)
            for word in lwords:
                if word in word_freq: # if word exists in dict
                    word_freq[word] += 1
                else: # if word does not exist in dict
                    word_freq[word] = 1

        # calculate the mean by :frequency of word divided by total number of documents
        doc_words = []
        n_docs = len(assigned_docs) # number of total documents

        # creating new list of words and their frequencies for new prototypes
        for word, frequency in word_freq.items():
            #if self.in_prototype(key, word):
            doc_words.append((word, frequency / int(n_docs)))

        #word_limit = len([for clusterId, prototype in self.prototypes.items():])

        # sorting all output descending by frequency --> top [:69]
        assigned_docs = sorted(assigned_docs) 
        new_prototype = sorted(doc_words, key=lambda x: x[1], reverse=True)[:69]

        yield (key, (assigned_docs, new_prototype))


    def steps(self):
        return [MRStep(mapper_init=self.load_data, mapper=self.assign_prototype,
                       reducer=self.aggregate_prototype)
            ]


if __name__ == '__main__':
    MRKmeansStep.run()