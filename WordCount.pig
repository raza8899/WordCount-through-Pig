InputData = LOAD 'hdfs://localhost:8020/MyDir/Input-Big.txt' using PigStorage('\n') as (line:chararray);
Data= FOREACH InputData GENERATE FLATTEN(TOKENIZE(line)) as word;
FilteredData = FILTER Data by word MATCHES '\\w+';
Word_Group = GROUP FilteredData by word;
Word_Count = FOREACH Word_Group GENERATE group as word , COUNT(FilteredData) as count;
Ordered_Output= ORDER Word_Count BY count DESC;
STORE Ordered_Output INTO 'hdfs://localhost:8020/WordCount';



