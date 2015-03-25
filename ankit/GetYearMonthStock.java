package ankit;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class GetYearMonthStock extends EvalFunc<String> {

	@Override
	public String exec(Tuple tuple) throws IOException {
		String yearMonthStock = null, dateString = null, stockName = null;

		try {
			/* Get stock name and date string*/
			stockName = (String) tuple.get(0);
			dateString = (String) tuple.get(1);
			
			/* Remove .csv from the filename */
			stockName = stockName.substring(0, stockName.indexOf("."));

			/* Extract year-month from the date string */
			yearMonthStock = stockName + "-"
					+ dateString.substring(0, dateString.lastIndexOf('-'));

		} catch (Exception e) {
			System.out.println(stockName + " ==> " + dateString);
			e.printStackTrace();
		}
		return yearMonthStock;
	}
}
