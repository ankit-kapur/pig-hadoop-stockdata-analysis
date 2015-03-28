package ankit;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class GetXi extends EvalFunc<Double> {

	@Override
	public Double exec(Tuple tuple) throws IOException {
		Double xMin = 0.0, xMax = 0.0, xi = 0.0;
		
		try {
			/* Get stock name and date string*/
			xMin = (Double) tuple.get(0);
			xMax = (Double) tuple.get(1);
			
			/* Calculate xi */
			if (xMin < 0.0 || xMin > 0.0)
				xi = (xMax - xMin)/xMin;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return xi;
	}
}
