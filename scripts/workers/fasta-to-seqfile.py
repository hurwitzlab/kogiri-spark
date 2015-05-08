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

  in_dir  = argv[0] 
  out_dir = argv[1]

  if not os.path.isdir(in_dir): 
    usage("Bad dir (%s)" % in_dir)

  files = os.listdir(in_dir)

  if len(files) == 0:
    usage("Empty dir (%s)" % in_dir)

  sc = SparkContext(appName="fasta-parser")

  nfile = 0
  for in_file in files:
    nfile += 1
    print("%5d: %s" % (nfile, in_file))

    seqs = []
    i    = 0
    fh   = open(os.path.join(in_dir, in_file), "rU")

    for record in SeqIO.parse(fh, "fasta") :
      i += 1
      seqs.append((i, str(record.seq)))

    rdd = sc.parallelize(seqs)
    dir = os.path.join(out_dir, os.path.basename(in_file))

    rdd.saveAsSequenceFile(dir)

  print('Done processing %s file%s' % 
    (nfile, '' if (nfile == 1) else 's')
  )

# --------------------------------------------------
if __name__ == "__main__":
  main(sys.argv[1:])
