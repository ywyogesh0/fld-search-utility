package com.fld.search.utility.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateConversionUtility {
    public static void main(String[] args) throws ParseException {
        long ts = 1571323208117L;
        String dateFormat = "yyyy-MM-dd HH:mm:ss.SSS";
        String formattedDate = "2019-10-17 15:40:08.117";

        System.out.println(DateConversionUtility.timestampToFormattedDate(ts, dateFormat));
        System.out.println(DateConversionUtility.FormattedDateToTimestamp(formattedDate, dateFormat));
    }

    public static String timestampToFormattedDate(long ts, String dateFormat) {
        Date date = new Date(ts);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        return simpleDateFormat.format(date);
    }

    public static long FormattedDateToTimestamp(String formattedDate, String dateFormat) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        return simpleDateFormat.parse(formattedDate).getTime();
    }
}
