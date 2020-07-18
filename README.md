# Distributed Page Rank

Distributed Page Rank is an implementation of Google's Page Rank algorithm using the Apache Hadoop framework. Given that the World Wide Web currently has more than 5 billion nodes, a single machine cannot hope to scale its computation to data this large. Notably this issue of scale is prevalent when working with other graphs, such as social media networks, video sharing platforms among others. 


This implementation of the Page Rank algorithm leverages the Map Reduce framework to parallelize computation over a cluster of machines. Through this, the Page Rank algorithm can be scaled to handle Big Data, which is more often than not the domain that provides us with the most valuable insights. 

## Background

The [Page Rank](http://ilpubs.stanford.edu:8090/422/1/1999-66.pdf) algorithm was first introduced by Page et al as a tool for leveraging the hyperlink graph of the World Wide Web to rank the importance of web pages. The algorithm was applied as part of the information retrieval process in the google search engine.

Page Rank is an iterative algorithm which assigns each node in a graph a rank. The rank of each node at iteration i is defined to be:

![formula](https://render.githubusercontent.com/render/math?math=\Large%20r_i=d%2B(1-d)\sum_{j\in%20B(i)}%20\frac{1}{N(j)}%20r_{i-1}$)

Where ![formula](https://render.githubusercontent.com/render/math?math=B(i)) is defined to be the vertices which have an edge to node ![formula](https://render.githubusercontent.com/render/math?math=i$) and ![formula](https://render.githubusercontent.com/render/math?math=N(j)$) is the cardinality of the set of neighbors of node ![formula](https://render.githubusercontent.com/render/math?math=j$) such that ![formula](https://render.githubusercontent.com/render/math?math=j%20\in%20B(i)$). The intuition behind this algorithm follows from the idea that different types of edges carry different importance. Having an incoming edge from node with a high degree would transfer a high degree of credibility whereas an incoming edge from a node with a low degree should transfer a low degree of credibility. 


## Pre Processing and File Formats

Please ensure that the input graph is formated as an edge list according to the input file format. The output format is shown below and will be written to a new file in a specified directory.

```txt
#Input Format                           Output Format

1        2                              3        0.542
5        4                              1        0.123
2        3                              5        0.101
3        1                              2        0.089
4        9                              9        0.053
```

## Running Distributed Page Rank
Distributed Page Rank uses a command line interface to execute the Page Rank algorithm. It offers the capability of running sub operations of the algorithm manually or the entire operation as a whole until a convergence threshold is reached. The following commands are available to run individual operations of Page Rank.

```linux
#initialize input edge list to appropriate format
init <in_dir> <out_dir> <#machines>

#run one iteration of Page Rank
iter <in_dir> <out_dir> <#machines>

#find difference in page rank between current and previous iteration
diff <in_dir_curr> <in_dir_prev> <out_dir> <#machines>

#finish job and sort the vertices by decreasing order of Page Rank
finish <in_dir> <out_dir> <#machines>

#run all operations to convergence threshold of 0.001
composite <in_dir> <out_dir> <interim_dir1> <interm_dir2> <diff_dir> <#machines>
```
In order to start the program make sure to first package the files to a JAR and run it through Hadoop as follows:
```linux
ant jar

hadoop jar PageRank.jar src.driver.SocialRankDriver {arguments}
```

## Scaling On The Cloud

Since the Apache Hadoop program is intended to be ran on a cluster of machines, the program is well adapted to be executed on cloud services such as AWS EMR. That said, the code is able to be run on a single machine however this not recommended and should be used primarily for the purpose of testing.
