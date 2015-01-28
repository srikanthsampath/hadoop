# hadoop
Some Hadoop mapred programs

Steps:
=======
hadoop com.sun.tools.javac.Main hadoop/invertedindex/InvertedIndex.java 
jar cvf ii.jar hadoop/invertedindex/InvertedIndex*.class
hadoop fs -rmr output
hadoop jar ii.jar hadoop.invertedindex.InvertedIndex input output
hadoop fs -cat output/part-r-00000
