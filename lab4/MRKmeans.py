"""
.. module:: MRKmeans

MRKmeans
*************

:Description: MRKmeans

    Iterates the MRKmeansStep script

:Authors: bejar
    

:Version: 

:Created on: 17/07/2017 10:16 

"""

from MRKmeansStep import MRKmeansStep
import shutil
import argparse
import os
import time
from mrjob.util import to_lines

__author__ = 'bejar'

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--prot', default='prototypes.txt', help='Initial prototypes file')
    parser.add_argument('--docs', default='documents.txt', help='Documents data')
    parser.add_argument('--iter', default=5, type=int, help='Number of iterations')
    parser.add_argument('--ncores', default=2, type=int, help='Number of parallel processes to use')

    args = parser.parse_args()
    assign = {}

    # Copies the initial prototypes
    cwd = os.getcwd()
    shutil.copy(cwd + '/' + args.prot, cwd + '/prototypes0.txt')

    nomove = False  # Stores if there has been changes in the current iteration
    for i in range(args.iter):
        tinit = time.time()  # For timing the iterations

        # Configures the script
        print('Iteration %d ...' % (i + 1))
        # The --file flag tells to MRjob to copy the file to HADOOP
        # The --prot flag tells to MRKmeansStep where to load the prototypes from
        mr_job1 = MRKmeansStep(args=['-r', 'local', args.docs,
                                     '--file', cwd + '/prototypes%d.txt' % i,
                                     '--prot', cwd + '/prototypes%d.txt' % i,
                                     '--num-cores', str(args.ncores)])

        # Runs the script
        with mr_job1.make_runner() as runner1:
            runner1.run()
            new_assign = {}
            new_proto = {}

            # Process the results of the script iterating the (key, value) pairs
            for key, value in mr_job1.parse_output(runner1.cat_output()):
                # You should store things here probably in a datastructure
                new_assign[key] = value[0]
                new_proto[key] = value[1]

            # If your scripts returns the new assignments you could write them in a file here
            with open(cwd + '/assignments{}.txt'.format(i + 1), 'w') as f:
                for clust, assignment in new_assign.items():
                    docs = ' '.join([doc for doc in assignment])
                    f.write('{}:{}\n'.format(clust, docs))
                f.flush()
                f.close()

            # You should store the new prototypes here for the next iteration
            with open(cwd + '/prototypes{}.txt'.format(i + 1), 'w') as f:
                for clust, proto_l in new_proto.items():
                    s = ' '.join(['{}+{}'.format(type, str(norm_freq)) for (type, norm_freq) in proto_l])
                    f.write('{}:{}\n'.format(clust, s))
                f.flush()
                f.close()

            # If you have saved the assignments, you can check if they have changed from the previous iteration
            if os.path.exists(cwd + '/assignments{}.txt'.format(i)):
                docs_changed = True
                with open(cwd + '/assignments{}.txt'.format(i), 'r') as f:
                    for line in f:
                        clust, docs = line.split(':')
                        assigned_docs = docs.split()

                        if new_assign[clust] != assigned_docs:
                            docs_changed = False
                            break

        #print(f"Time= {(time.time() - tinit)} seconds" % )
        print('Time: {} seconds'.format(time.time() - tinit))

        if nomove:  # If there is no changes in two consecutive iteration we can stop
            print("Algorithm converged")
            break

    # Now the last prototype file should have the results