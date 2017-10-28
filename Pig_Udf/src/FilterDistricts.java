import java.io.IOException; 
import org.apache.pig.FilterFunc; 
import org.apache.pig.data.Tuple;
/**
 * @author Nihit Rai
 * Class for PIG UDF for Indian State analysis project for Acadgild -Project 1
 *
 */
public class FilterDistricts extends FilterFunc{

	@Override
	public Boolean exec(Tuple arg0) throws IOException {
		// TODO Auto-generated method stub
		Boolean returnFlag=false;
		try
		{
		int numArgs = arg0.size();
		if (numArgs==2)
		{
			//placing Project_Performance_IHHL_BPL at as argument 1
			int achieved = (int) arg0.get(0);
			
			//placing Project_Objectives_IHHL_BPL  at as argument 2
			int target = (int) arg0.get(1);
			
			//Calculating percentage
			double percentage = (achieved/target)*100;
			
			if(percentage>=80)
			{
				//Setting flag to true if the state has 80% or greater BPL achieved
				returnFlag=true;
			}
		}
		else
		{
			System.out.println("Invalid number of arguments. There should be two integer arguments");
		}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			
		}
		return returnFlag;
	}
}

