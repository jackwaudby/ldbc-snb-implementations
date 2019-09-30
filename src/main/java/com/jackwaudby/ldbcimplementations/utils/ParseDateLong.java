package com.jackwaudby.ldbcimplementations.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * This script converts Date strings to long
 */
public class ParseDateLong {

    public static long birthdayStringToLong(String birthday) throws ParseException {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));                             // set timezone
        SimpleDateFormat birthdayFormat = new SimpleDateFormat("yyyy-MM-dd"); // birthday format
        Date birthdayDate = birthdayFormat.parse(birthday);                           // parse to Date
        return birthdayDate.getTime();                                                // get date in milliseconds
    }

    public static long creationDateStringToLong(String creationDate) throws ParseException {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));                                                    // set timezone
        SimpleDateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");    // creationDate format
        Date creationDateDate = creationDateFormat.parse(creationDate);                                      // parse to Date
        return creationDateDate.getTime();                                                                   // get date in milliseconds
    }


}
