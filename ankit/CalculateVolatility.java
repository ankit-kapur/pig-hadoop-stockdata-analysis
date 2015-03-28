package ankit;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class CalculateVolatility extends EvalFunc<Double> {

	@Override
	public Double exec(Tuple tuple) throws IOException {
		double volatility = 0.0f, xBar = 0.0f;
		DataBag xiData = null; 
		long N = 36;
		
		try {
			/* Get the xiData, N, and xBar */
			xiData = (DataBag) tuple.get(0);
			N = (long) tuple.get(1);
			xBar = (Double) tuple.get(2);
			
			/* x value of first day */
			double summation = 0.0;
			for (Iterator<Tuple> iter = xiData.iterator(); iter.hasNext();) {
				Tuple tuple1 = iter.next();				
				double xi = (Double) tuple1.get(0);
				summation += Math.pow((xi - xBar), 2.0);
			}
			
			if (N-1 > 0)
				volatility = (Double) Math.sqrt(summation/(N-1));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return volatility;
	}
}
