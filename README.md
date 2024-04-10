# pigcmd

....................................................................................................
link in description ---

# CMD in VM -- (copy and paste in URL) --

https://drive.google.com/file/d/1BlX31fu0R-vYbrcHzNykUXI8WXBqVMo3/view?usp=drive_link

.........................................................................................

# CMD in Ubuntu -- (copy and paste in URL) --

#install of pig 

link - 
https://downloads.apache.org/pig/pig-0.17.0/

( Video link - https://youtu.be/vhFELxiw77U?si=pgryMmv-sV-UrIRC )

...........................................................................

CMD - 

0. pwd 

2. mkdir pig_mudit

3. cd pig_mudit

4. touch input1.csv 
 
copy and paste in index1.csv -

id,name,age
1,John,25
2,Alice,30
3,Bob,28


5. touch  input2.csv 

copy and paste in index2.csv -

id,salary
1,50000
2,60000
3,55000

............................................................................

CMD - 

1. pig

2. fs -mkdir /Pig_Data  (in grunt) 

3. fs -put input1.csv /Pig_Data/

4. fs -put input2.csv /Pig_Data/

#sorted

5. student = LOAD 'hdfs://localhost:9000/Pig_Data/input1.csv'USING PigStorage(',') as ( id:int, name:chararray, age:int );

-- Sort data by age in ascending order

6. sorted_data = ORDER student BY age ASC;

-- Store sorted data


7. STORE sorted_data INTO ' hdfs://localhost:9000/pig_Output1/ ' USING PigStorage (',');

#output

8. hdfs dfs -cat 'hdfs://localhost:9000/pig_Output1/part-m-00000'    NOTE- (see the correct your directory) (URL- localhost:9870  / localhost:9000)

........................................................................................................

#group 

-- Load data
9. data = LOAD 'hdfs://localhost:9000/Pig_Data/input1.csv' USING PigStorage(',') AS (id:int, name:chararray, age:int);

-- Group data by age
10. grouped_data = GROUP data BY age;

-- Store grouped data
11. STORE grouped_data INTO 'hdfs://localhost:9000/grouped_output/' USING PigStorage(',');

#output

12. hdfs dfs -cat 'hdfs://localhost:9000/grouped_output/part-m-00000'    NOTE- (see the correct your directory) (URL- localhost:9870  / localhost:9000)


........................................................................................................

#join

-- Load data
13. data1 = LOAD 'hdfs://localhost:9000/Pig_Data/input1.csv' USING PigStorage(',') AS (id:int, name:chararray, age:int);
14. data2 = LOAD 'hdfs://localhost:9000/Pig_Data/input2.csv' USING PigStorage(',') AS (id:int, salary:int);

-- Join data1 and data2 on id
15. joined_data = JOIN data1 BY id, data2 BY id;

-- Store joined data
16. STORE joined_data INTO 'hdfs://localhost:9000/joined_output' USING PigStorage(',');

#output

17. hdfs dfs -cat 'hdfs://localhost:9000/joined_output/part-m-00000'    NOTE- (see the correct your directory) (URL- localhost:9870  / localhost:9000)


........................................................................................................

#project

-- Load data
18. data = LOAD 'hdfs://localhost:9000/Pig_Data/input1.csv' USING PigStorage(',') AS (id:int, name:chararray, age:int);

-- Project only id and name fields
19. projected_data = FOREACH data GENERATE id, name;

-- Store projected data
20. STORE projected_data INTO 'hdfs://localhost:9000/projected_output' USING PigStorage(',');

#output

21. hdfs dfs -cat 'hdfs://localhost:9000/projected_output/part-m-00000'    NOTE- (see the correct your directory) (URL- localhost:9870  / localhost:9000)






........................................................................................................

#filter

-- Load data
22. data = LOAD 'hdfs://localhost:9000/Pig_Data/input1.csv' USING PigStorage(',') AS (id:int, name:chararray, age:int);

-- Filter data where age is greater than 25
23. filtered_data = FILTER data BY age > 25;

-- Store filtered data
24. STORE filtered_data INTO 'hdfs://localhost:9000/filtered_output' USING PigStorage(',');


#output

25. hdfs dfs -cat 'hdfs://localhost:9000/filtered_output/part-m-00000'    NOTE- (see the correct your directory) (URL- localhost:9870  / localhost:9000)


......................................................................................


#wordcount

lines = LOAD 'hdfs://localhost:9000/Pig_Data/mm.txt' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) as word;
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE group,COUNT(words);
DUMP wordcount;


......................................................................................


#find a max temp for every year
records = LOAD 'hdfs://localhost:9000/Pig_Data/sample_temp.txt' AS (year:chararray, temperature:int, quality:int);

filtered_records = FILTER records BY temperature!=9999 AND (quality==0 OR quality==1 OR quality==4 OR quality==5 OR quality==9);

grouped_records = GROUP filtered_records BY year;

max_temp = FOREACH grouped_records GENERATE group, MAX(filtered_records.temperature);

DUMP max_temp;

STORE max_temp INTO 'hdfs://localhost:9000/max_temp_output' USING PigStorage(',');


HIVE_HOME/bin/schematool -dbType derby -initSchema
