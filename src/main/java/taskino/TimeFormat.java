package taskino;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeFormat {

    public final static String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    public static class SimplerDateFormat extends SimpleDateFormat {

        private static final long serialVersionUID = 1L;

        public SimplerDateFormat(String format) {
            super(format);
        }

        public Date parseQuitely(String s) {
            try {
                return this.parse(s);
            } catch (ParseException e) {
                throw new IllegalArgumentException("date parse error:" + s);
            }
        }

    }

    public final static ThreadLocalObject<SimplerDateFormat> ISOFormatter =
                    new ThreadLocalObject<>(() -> new SimplerDateFormat(TimeFormat.ISO_FORMAT));

}
