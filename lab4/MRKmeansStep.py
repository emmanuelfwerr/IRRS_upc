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
        new_prot = set([tupl[0] for tupl in set(prot)])

        intersection = len(list(new_prot.intersection(doc)))
        union = (len(new_prot) + len(set(doc))) - intersection

        return float(intersection) / union


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

        minimum = float('inf')
        current_clust = None  
        for clusterId, prototype in self.prototypes.items():
            jaccard_score = self.jaccard(prototype, lwords)
            if (jaccard_score < minimum):
                minimum = jaccard_score
                current_clust = clusterId # update the new cluster assigned to the document

        yield (current_clust, (doc, lwords))


    def aggregate_prototype(self, key, values):
        """
        input is cluster and all the documents it has assigned
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
        all_words = []
        n_docs = len(assigned_docs) # number of total documents

        # creating new list of words and their frequencies for new prototypes
        for word, frequency in word_freq.items():
            all_words.append((word, frequency / int(n_docs)))

        # sorting all ouput alphabetically
        out_docs = sorted(assigned_docs) 
        out_proto = sorted(all_words, key=lambda x: x[0])

        yield (key, (out_docs, out_proto))


    def steps(self):
        return [MRStep(mapper_init=self.load_data, mapper=self.assign_prototype,
                       reducer=self.aggregate_prototype)
            ]


if __name__ == '__main__':
    MRKmeansStep.run()