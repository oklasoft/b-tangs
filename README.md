# B-Tangs: Binning Trimmer of Artifacts in Next Gen Sequence

## Description

A simple brute force tool to remove possible PCR duplicates from next generation
sequencing datasets using Hadoop's map reduce. It is the simple solution of just
comparing all reads to each other & rejecting ones which match. Thus they go into
bins based on a key, which is made up on some amount of the sequence of the read.
From those bins we then select the best & keep only it for ones that match. This
was originally done years ago when we got some of our first really nasty looking
NGS data. I'm just now getting around to publishing it

## Prerequisites

Hadoop, ruby 1.9, [wukong](https://github.com/mrflip/wukong), some shell commands

## Installation

Again my installation passes the "works for me" test. There are no installation
directions or tools at this point as it is highly tuned to my installation. Probably
need to set the omrf dir full of ruby libraries in your ruby lib search path, make sure
the other *.rb scripts are in your shells $PATH and then go for it?

## Scripts

I don't rightly actually remember anymore what all these do. Just really use/call
clean_sample.rb directly & it will call the rest in the right way

* **clean_sample.rb**

  Good starting point driver script wrapping all the other steps. You give it
  some sequence files & some other options as listed via --help & it'll give you
  back then some results: cleaned files & stats
  
* **btangs.rb**

  The wrapper script around the Wukong bits. You can try calling this, but clean_sample.rb
  does a better job of getting everything setup for you
  
* **flat_fasta_joiner.rb**

  Due to hadoop streaming working over just single lines as its record, it was
  just easiest & faster to flatten the fasta/fastq file in some manner
  
* **joined_fastq_finisher.rb**

  Another wukong/hadoop script does some sort of finishing tasks
  
* **joined_qseq_finisher.rb**
  
  Another wukon/hadoop streaming script for finishing something related to qseq formatted files
  
* **qseq_joiner.rb**

  Something related to joining qseq

# License

Unless otherwise noted, everything is Copyright (c) 2010 Stuart Glenn, Oklahoma Medical
Research Foundation. (OMRF). Essentially under the BSD license, see LICENSE file for
full details. 
