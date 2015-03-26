package ankit;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class GetXi extends EvalFunc<Tuple> {

	@Override
	public Tuple exec(Tuple tuple) throws IOException {
		double xMin = 0.0, xMax = 0.0, xi = 0.0;
		String id = "", date = "";
		Tuple outputTuple = TupleFactory.getInstance().newTuple(3);
		
		try {
			/* Get stock name and date string*/
			DataBag minDataObj = (DataBag) tuple.get(0);
			DataBag maxDataObj = (DataBag) tuple.get(1);

			/* x value of first day */
			for (Iterator<Tuple> iter = minDataObj.iterator(); iter.hasNext();) {
				Tuple tuple1 = iter.next();
				id = (String) tuple1.get(0);
				date = (String) tuple1.get(1);
				xMin = Double.parseDouble((String) tuple1.get(2));
			}
			
			/* x value of last day */
			for (Iterator<Tuple> iter = maxDataObj.iterator(); iter.hasNext();) {
				Tuple tuple1 = iter.next();
				xMax = Double.parseDouble((String) tuple1.get(2));
			}
			
			/* Calculate xi */
			xi = (xMax - xMin)/xMin;
			
			/* Create tuple */
			outputTuple.set(0, id);
			outputTuple.set(1, date);
			outputTuple.set(2, Double.toString(xi));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return outputTuple;
	}
}
