REGISTER piggybank.jar;
REGISTER udf_ankit.jar;

------- Load the raw data
raw_data = LOAD 'veryverysmall/' USING PigStorage(',','-tagFile')
as (filename: chararray, date: chararray, dontcare1, dontcare2, dontcare3, dontcare4, dontcare5, adjClose: chararray);

------- Filter out the headers
filtered_data = FILTER raw_data BY date neq 'Date';

------- Get identifier using UDF
processed_data = FOREACH filtered_data generate ankit.GetYearMonthStock(filename, date) as identifier, date, adjClose;

------- Group by the IDs, and find the first (MIN) and last (MAX) days of each month
C = GROUP processed_data BY identifier;
maxmin = FOREACH C generate group as identifier, MIN(processed_data.date) as mindate, MAX(processed_data.date) as maxdate;

------- Get min data
min_joiner = JOIN maxmin BY (identifier, mindate), processed_data BY (identifier, date);
min_joined_data = FOREACH min_joiner GENERATE $0 as identifier, $4 as date, $5 as adjClose;
------- Get max data
max_joiner = JOIN maxmin BY (identifier, maxdate), processed_data BY (identifier, date);
max_joined_data = FOREACH max_joiner GENERATE $0 as identifier, $4 as date, $5 as adjClose;
------- Combine the min and max dates
combined = COGROUP min_joined_data BY $0, max_joined_data BY $0;

------- Generate Xi
with_xi = FOREACH combined GENERATE FLATTEN(ankit.GetXi($1,$2));

------- Calculate x_bar
grouped_stocks = GROUP with_xi BY $0;
x_bar = FOREACH grouped_stocks {
		summation = SUM(with_xi.$2);
		count = COUNT(with_xi.$2);
		xbarVal = (count>0 ? (FLOAT)(summation/count) : 0);
		GENERATE group as stock_name, count as N, xbarVal as xbar;
	}

joint = JOIN grouped_stocks BY (group), x_bar by stock_name;
grouped_data = GROUP joint BY $0;

------- Get the volatility!
volatility = FOREACH grouped_data {
		--x_bar_this_stock = FILTER x_bar BY (stock_name eq group);
		GENERATE group as stock_name, ankit.CalculateVolatility(joint.$1, joint.N, joint.xbar) as vol;  ---Send the xi data, N, and xbar
	}

------- Filter out zero volatilies
volatility = FILTER volatility BY vol != 0.0;

------- Sort by volatility
sorted_vol = ORDER volatility BY vol;
top10 = LIMIT sorted_vol 10;
sorted_vol = ORDER volatility BY vol DESC;
bottom10 = LIMIT sorted_vol 10;

X = UNION top10, bottom10;
Y = GROUP X BY 1;
result = FOREACH Y GENERATE FLATTEN(X);

------ Store the results
store result into 'output_folder';
