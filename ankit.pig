/* wc.pig */
REGISTER piggybank.jar;
REGISTER udf_ankit.jar;

A = LOAD 'veryverysmall/' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage
--(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') 
(',', 'YES_MULTILINE', 'NOCHANGE') 
as (date, dontcare1, dontcare2, dontcare3, dontcare4, dontcare5, adjClose);

/* Extract each token */
B = foreach A generate ankit.GetYearMonthStock(date) as bleh, date, adjClose;
--B = foreach A generate flatten(TOKENIZE((chararray)$0)) as word;  -- extract each token

C = group B by bleh;
--C = group B by bleh;

--D = foreach C generate COUNT(B), group;

/* Store the results */
store B into 'out_folder';  -- write the results to a directory name wc_out
