#!/usr/bin/env ruby1.9

# 
# btangs.rb
# Created by Stuart Glenn on 2010-12-21
#
# == Synopsis
# Binning Trimmer of Artifacts in Next Gen Sequence
# Clean out possible PCR artifacts by searching for like sequence reads via some
# map reduce methods & a user defined key
#
# == Usage
#  btangs.rb --run=[local|hadoop] --input_format=FILE_FORMAT --range_start=KEY_START_POS --range_size=KEY_LENGTH --key_type=KEY_METHOD (--both_ends) (--include_rejects) INPUT OUTPUT
#
#
# == Options
#  --run=MODE                Run mode, either local or use hadoop  streaming
#  --input_format=FORMAT     Format of input file(s); joined_qseq, joined_fastq, qseq, fasta
#  --range_start=POS         Starting base position of the sequence to be used as input to the key method
#  --range_size=LENGTH       The lenght (or number of bases) from the sequence to use as the input to the key method
#  --key_type=METHOD         The method to use on the sequence (as limited via the range_start & range_size) to make key; sep_joined_pairs
#  --both_ends               Concate the range_start & range_size from both ends of the reads to make the input for the key
#  --include_rejects         Include the rejected sequence in the output
#
# ==Author
#  Stuart Glenn <Stuart-Glenn@omrf.org>
#
# ==Copyright
#  Copyright (c) 2010 Stuart Glenn, Oklahoma Medical Research Foundation. (OMRF)
#  All rights reserved.
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are met:
#  1. Redistributions of source code must retain the above copyright
#     notice, this list of conditions and the following disclaimer.
#  2. Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in the
#     documentation and/or other materials provided with the distribution.
#  3. All advertising materials mentioning features or use of this software
#     must display the following acknowledgement:
#     This product includes software developed by the OMRF
#  4. Neither the name of the Oklahoma Medical Research Foundation nor the
#     names of its contributors may be used to endorse or promote products
#     derived from this software without specific prior written permission.
#
#  THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ''AS IS'' AND ANY
#  EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#  DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

require 'omrf/btangs/cleaner'
include OMRF::Btangs::Cleaner

Wukong::Script.new(CleanerMapper,CleanerReducer).run