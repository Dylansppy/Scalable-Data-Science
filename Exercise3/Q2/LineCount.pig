lines = LOAD '$INPUT' AS (line:chararray);
rows = FOREACH lines GENERATE flatten(line) AS row;
row_groups = GROUP rows BY row;
row_counts = FOREACH row_groups generate group AS row, COUNT(rows) AS count;
STORE row_counts INTO '$OUTPUT';
