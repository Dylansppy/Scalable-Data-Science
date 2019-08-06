lines = LOAD '$INPUT' AS (line:chararray);
words = FOREACH lines GENERATE flatten(TOKENIZE(line)) AS word;
word_groups = GROUP words BY word;
word_counts = FOREACH word_groups generate group AS word, COUNT(words) AS count;
STORE word_counts INTO '$OUTPUT';
