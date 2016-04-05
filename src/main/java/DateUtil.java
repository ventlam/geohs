import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by vent on 4/5/16.
 */
public class DateUtil {

    public static String getYesterdayDateString() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        return dateFormat.format(cal.getTime());
    }
}
