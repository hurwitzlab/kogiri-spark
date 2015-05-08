#!/usr/bin/env python

import os
from pyspark import SparkContext
from Bio import SeqIO
#import argparse
import sys
#import pprint

# --------------------------------------------------
def usage(msg=""):
  if len(msg):
    print(msg)

  print('%s /input/file.fa /path/to/out/dir' % sys.argv[0])
  sys.exit(2)

# --------------------------------------------------
def main(argv):                         
#  parser = argparse.ArgumentParser()
#  parser.add_argument('-i', '--in')
#  parser.add_argument('-o', '--out')
#  args = parser.parse_args()
#
#  pprint.pprint(args)
  
  if len(argv) != 2:
    usage()

  in_file = argv[0] 
  out_dir = argv[1]

  if not os.path.isfile(in_file): 
    usage("Bad file (%s)" % in_file)

  sc = SparkContext(appName="fasta-parser")

  print("Processing %s" % in_file)

  seqs = []
  i    = 0

  for record in SeqIO.parse(open(in_file, "rU"), "fasta") :
    i += 1
    seqs.append((i, str(record.seq)))

  rdd = sc.parallelize(seqs)
  dir = os.path.join(out_dir, os.path.basename(in_file))

  rdd.saveAsSequenceFile(dir)

# --------------------------------------------------
if __name__ == "__main__":
  main(sys.argv[1:])
