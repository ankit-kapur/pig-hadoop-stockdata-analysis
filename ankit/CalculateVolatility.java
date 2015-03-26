package ankit;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

public class CalculateVolatility extends EvalFunc<Double> {

	@Override
	public Double exec(Tuple tuple) throws IOException {
		double volatility = 0.0, xBar = 0.0;
		DataBag xiData = null; 
		long N = 36;
		
		try {
			/* Get the xiData, N, and xBar */
			DataBag xiDataBag = (DataBag) tuple.get(0);
			DataBag NBag = (DataBag) tuple.get(1);
			DataBag xBarBag = (DataBag) tuple.get(2);
			
			for (Iterator<Tuple> iter = xiDataBag.iterator(); iter.hasNext();)
				xiData = (DataBag) iter.next().get(0);
			for (Iterator<Tuple> iter = NBag.iterator(); iter.hasNext();)
				N = (Long) iter.next().get(0);
			for (Iterator<Tuple> iter = xBarBag.iterator(); iter.hasNext();)
				xBar = (Float) iter.next().get(0);

			/* x value of first day */
			double summation = 0.0;
			for (Iterator<Tuple> iter = xiData.iterator(); iter.hasNext();) {
				Tuple tuple1 = iter.next();
				
				DataByteArray xiVal = (DataByteArray) tuple1.get(2);
				double xi = Double.parseDouble(new String(xiVal.get()));
				
				summation += Math.pow((xi - xBar), 2.0);
			}
			
			volatility = Math.sqrt(summation/(N-1));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new Double(volatility);
	}
}
