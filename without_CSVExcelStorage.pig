REGISTER piggybank.jar;
REGISTER udf_ankit.jar;

raw_data = LOAD 'veryverysmall/' 
USING PigStorage(',','-tagFile')
as (filename: chararray, date: chararray, dontcare1, dontcare2, dontcare3, dontcare4, dontcare5, adjClose: chararray);

------- Filter out the headers
filtered_data = FILTER raw_data BY date neq 'Date';

------- Get identifier
processed_data = FOREACH filtered_data generate ankit.GetYearMonthStock(filename, date) as identifier, date, adjClose;

C = GROUP processed_data BY identifier;
D = foreach C generate group, MIN(date), MAX(date);

------ Store the results
store D into 'out_folder';  -- write the results to a directory
