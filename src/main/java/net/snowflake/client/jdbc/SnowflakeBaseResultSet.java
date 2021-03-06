/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.common.core.SFBinary;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SFTime;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import net.snowflake.common.core.SqlState;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.sql.Types;
import java.util.TimeZone;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Base class for query result set and metadata result set
 *
 * @author jhuang
 */
public class SnowflakeBaseResultSet implements ResultSet
{

  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeBaseResultSet.class);

  static final int[] powersOfTen =
  {
    1, 10, 100, 1000, 10000,
    100000, 1000000, 10000000, 100000000, 1000000000
  };

  protected Statement statement;

  protected boolean wasNull = false;

  protected Object[] nextRow = null;

  protected SnowflakeResultSetMetaData resultSetMetaData = null;

  protected int row = 0;

  protected boolean endOfResult = false;

  protected Map<String, Object> parameters = new HashMap<>();

  protected TimeZone timeZone;

  // Timezone used for TimestampNTZ
  protected TimeZone timeZoneUTC;

  // Formatters for different datatypes
  protected SnowflakeDateTimeFormat timestampNTZFormatter;
  protected SnowflakeDateTimeFormat timestampLTZFormatter;
  protected SnowflakeDateTimeFormat timestampTZFormatter;
  protected SnowflakeDateTimeFormat dateFormatter;
  protected SnowflakeDateTimeFormat timeFormatter;
  protected boolean honorClientTZForTimestampNTZ = true;
  protected SFBinaryFormat binaryFormatter;

  protected int fetchSize = 0;

  protected int fetchDirection = ResultSet.FETCH_FORWARD;

  protected long resultVersion = 0;

  @Override
  public boolean next() throws SQLException
  {
    logger.debug("public boolean next()");

    return false;
  }

  @Override
  public void close() throws SQLException
  {
    logger.debug("public void close()");

    // free the object so that they can be Garbage collected
    nextRow = null;
    statement = null;
    resultSetMetaData = null;
  }

  @Override
  public boolean wasNull() throws SQLException
  {
    logger.debug("public boolean wasNull() returning {}", wasNull);

    return wasNull;
  }

  @Override
  public String getString(int columnIndex) throws SQLException
  {
    logger.debug("public String getString(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);
    if (obj == null)
    {
      return null;
    }

    // print timestamp in string format
    int columnType = resultSetMetaData.getInternalColumnType(columnIndex);
    switch (columnType)
    {
      case Types.BOOLEAN:
        if (obj.toString().equals("1"))
        {
          return "TRUE";
        }
        else if (obj.toString().equals("0"))
        {
          return "FALSE";
        }
        break;

      case Types.TIMESTAMP:
      case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ:
      case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ:

        SFTimestamp sfTS = getSFTimestamp(columnIndex);
        int columnScale = resultSetMetaData.getScale(columnIndex);

        String timestampStr= null;

        // Derive the timestamp formatter to use
        SnowflakeDateTimeFormat formatter;
        if (columnType == Types.TIMESTAMP)
        {
          formatter = timestampNTZFormatter;
        }
        else if (columnType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ)
        {
          formatter = timestampLTZFormatter;
        }
        else // TZ
        {
          formatter = timestampTZFormatter;
        }

        if (formatter == null)
        {
          throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR
                  .getMessageCode(),
              "missing timestamp formatter");
        }

        Timestamp adjustedTimestamp =
            ResultUtil.adjustTimestamp(sfTS.getTimestamp());

        timestampStr = formatter.format(
            adjustedTimestamp, sfTS.getTimeZone(), columnScale);

        if (logger.isDebugEnabled())
          logger.debug("Converting timestamp to string from: {} to: {}",
              obj.toString(), timestampStr);

        return timestampStr;

      case Types.DATE:
        Date date = getDate(columnIndex, timeZoneUTC);

        if (dateFormatter == null)
        {
          throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
                                          ErrorCode.INTERNAL_ERROR
                                          .getMessageCode(),
                                          "missing date formatter");
        }

        String dateStr = null;

        // if date is on or before 1582-10-04, apply the difference
        // by (H-H/4-2) where H is the hundreds digit of the year according to:
        // http://en.wikipedia.org/wiki/Gregorian_calendar
        Date adjustedDate = ResultUtil.adjustDate(date);

        dateStr = dateFormatter.format(adjustedDate, timeZoneUTC);

        if (logger.isDebugEnabled())
        {
          String prevDateStr = dateFormatter.format(date, timeZoneUTC);

          logger.debug(
              "Adjust date from {} to {}",
              prevDateStr, dateStr);
        }

        if (logger.isDebugEnabled())
          logger.debug("Converting date to string from: {} to: {}",
                      obj.toString(), dateStr);
        return dateStr;

      case Types.TIME:
        SFTime sfTime = getSFTime(columnIndex);

        if (timeFormatter == null)
        {
          throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
                                          ErrorCode.INTERNAL_ERROR
                                          .getMessageCode(),
                                          "missing time formatter");
        }

        int scale = resultSetMetaData.getScale(columnIndex);
        String timeStr = null;


        timeStr = timeFormatter.format(sfTime, scale);

        if (logger.isDebugEnabled())
          logger.debug("Converting time to string from: {} to: {}",
              obj.toString(), timeStr);
        return timeStr;

      case Types.BINARY:
        if (binaryFormatter == null)
        {
          throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
                                          ErrorCode.INTERNAL_ERROR
                                          .getMessageCode(),
                                          "missing binary formatter");
        }

        if (binaryFormatter == SFBinaryFormat.HEX)
        {
          // Shortcut: the values are already passed with hex encoding, so just
          // return the string unchanged rather than constructing an SFBinary.
          return obj.toString();
        }

        SFBinary sfb = new SFBinary(getBytes(columnIndex));
        return binaryFormatter.format(sfb);

      default:
        break;
    }

    return obj.toString();
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException
  {
    logger.debug(
               "public boolean getBoolean(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return false;

    if (obj instanceof String)
    {
      if (obj.toString().equals("1"))
      {
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
    }
    else
    {
      return ((Boolean) obj).booleanValue();
    }
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException
  {
    logger.debug("public byte getByte(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public short getShort(int columnIndex) throws SQLException
  {
    logger.debug("public short getShort(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return 0;

    if (obj instanceof String)
    {
      return (Short.valueOf((String) obj)).shortValue();
    }
    else
    {
      return ((Number) obj).shortValue();
    }
  }

  @Override
  public int getInt(int columnIndex) throws SQLException
  {
    logger.debug("public int getInt(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return 0;

    if (obj instanceof String)
    {
      return (Integer.valueOf((String) obj)).intValue();
    }
    else
    {
      return ((Number) obj).intValue();
    }

  }

  @Override
  public long getLong(int columnIndex) throws SQLException
  {
    logger.debug("public long getLong(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return 0;

    try
    {
      if (obj instanceof String)
      {
        return (Long.valueOf((String) obj)).longValue();
      }
      else
      {
        return ((Number) obj).longValue();
      }
    }
    catch (NumberFormatException nfe)
    {
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
                                      ErrorCode.INTERNAL_ERROR.getMessageCode(),
                                      "Invalid long: " + (String) obj);
    }
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException
  {
    logger.debug("public float getFloat(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return 0;

    if (obj instanceof String)
    {
      return (Float.valueOf((String) obj)).floatValue();
    }
    else
    {
      return ((Number) obj).floatValue();
    }
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException
  {
    logger.debug("public double getDouble(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    // snow-11974: null for getDouble should return 0
    if (obj == null)
      return 0;

    if (obj instanceof String)
    {
      return (Double.valueOf((String) obj)).doubleValue();
    }
    else
    {
      return ((Number) obj).doubleValue();
    }
  }

  /**
   * @deprecated
   */
  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale)
          throws SQLException
  {
    logger.debug(
               "public BigDecimal getBigDecimal(int columnIndex, int scale)");

    BigDecimal value = null;

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return null;

    if (obj instanceof String)
    {
      value = new BigDecimal((String) obj);
    }
    else
    {
      value = new BigDecimal(obj.toString());
    }

    value = value.setScale(scale, RoundingMode.HALF_UP);

    return value;
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException
  {
    logger.debug("public byte[] getBytes(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return null;

    try
    {
      return SFBinary.fromHex(obj.toString()).getBytes();
    }
    catch (IllegalArgumentException ex)
    {
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "Invalid binary value: " + obj.toString());
    }
  }

  public Date getDate(int columnIndex, TimeZone tz) throws SQLException
  {
    if (tz == null)
    {
      tz = TimeZone.getDefault();
    }

    logger.debug("public Date getDate(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    try
    {
      if (obj == null)
      {
        return null;
      }

      long milliSecsSinceEpoch = Long.valueOf(obj.toString()) * 86400000;

      SFTimestamp tsInUTC = SFTimestamp.fromDate(new Date(milliSecsSinceEpoch),
          0, TimeZone.getTimeZone("UTC"));

      SFTimestamp tsInClientTZ = tsInUTC.moveToTimeZone(tz);

      if (logger.isDebugEnabled())
        logger.debug(
          "getDate: tz offset = {}",
              tsInClientTZ.getTimeZone().getOffset(tsInClientTZ.getTime()));

      // return the date adjusted to the JVM default time zone
      return new Date(tsInClientTZ.getTime());
    }
    catch (NumberFormatException ex)
    {
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "Invalid date value: " + obj.toString());
    }
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException
  {
    return getDate(columnIndex, TimeZone.getDefault());
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException
  {
    SFTime sfTime = getSFTime(columnIndex);
    if (sfTime == null)
    {
      return null;
    }

    return new Time(sfTime.getFractionalSeconds(3));
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException
  {
    return getTimestamp(columnIndex, TimeZone.getDefault());
  }

  public Timestamp getTimestamp(int columnIndex, TimeZone tz)
      throws SQLException
  {
    SFTimestamp sfTS = getSFTimestamp(columnIndex);

    if (sfTS == null)
    {
      return null;
    }

    Timestamp res = sfTS.getTimestamp();

    if (res == null)
    {
      return null;
    }

    // SNOW-14777: for timestamp_ntz, we should treat the time as in client time
    // zone so adjust the timestamp by subtracting the offset of the client
    // timezone
    if (honorClientTZForTimestampNTZ &&
        resultSetMetaData.getInternalColumnType(columnIndex) == Types.TIMESTAMP)
    {
      return sfTS.moveToTimeZone(tz).getTimestamp();
    }

    return res;
  }

  /**
   * Parse seconds since epoch with both seconds and fractional seconds after
   * decimal point (e.g 123.456 with a scale of 3) to a representation with
   * fractions normalized to an integer (e.g. 123456)
   * @param secondsSinceEpochStr
   * @param scale
   * @return a BigDecimal containing the number of fractional seconds since
   * epoch.
   */
  private BigDecimal parseSecondsSinceEpoch(String secondsSinceEpochStr,
                                                       int scale)
  {
      // seconds since epoch has both seconds and fractional seconds after decimal
      // point. Ex: 134567890.12345678
      // Note: can actually contain timezone in the lowest part
      // Example: obj is e.g. "123.456" (scale=3)
      //          Then, secondsSinceEpoch is 123.456
      BigDecimal secondsSinceEpoch = new BigDecimal(secondsSinceEpochStr);

      // Representation with fractions normalized to an integer
      // Note: can actually contain timezone in the lowest part
      // Example: fractionsSinceEpoch is 123456
      return secondsSinceEpoch.scaleByPowerOfTen(scale);
  }

  public SFTimestamp getSFTimestamp(int columnIndex) throws SQLException
  {
    logger.debug(
               "public Timestamp getTimestamp(int columnIndex)");

    Object obj = getObjectInternal(columnIndex);

    try
    {
      if (obj == null)
      {
        return null;
      }

      int scale = resultSetMetaData.getScale(columnIndex);

      BigDecimal fractionsSinceEpoch;

        // Derive the used timezone - if NULL, will be extracted from the number.
      TimeZone tz;
      switch (resultSetMetaData.getInternalColumnType(columnIndex))
      {
        case Types.TIMESTAMP:
          fractionsSinceEpoch = parseSecondsSinceEpoch(obj.toString(), scale);
          // Always in UTC
          tz = timeZoneUTC;
          break;
        case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ:
          String timestampStr = obj.toString();

          /**
           * For new result version, timestamp with timezone is formatted as
           * the seconds since epoch with fractional part in the decimal
           * followed by time zone index. E.g.: "123.456 1440". Here 123.456
           * is the number of seconds since epoch and 1440 is the timezone
           * index.
           */
          if (resultVersion > 0)
          {
            logger.trace(
                "Handle timestamp with timezone new encoding: {}",
                timestampStr);

            int indexForSeparator = timestampStr.indexOf(' ');
            String secondsSinceEpochStr =
                timestampStr.substring(0, indexForSeparator);
            String timezoneIndexStr =
                timestampStr.substring(indexForSeparator + 1);

            fractionsSinceEpoch = parseSecondsSinceEpoch(secondsSinceEpochStr,
                scale);

            tz = SFTimestamp.convertTimezoneIndexToTimeZone(Integer.parseInt(
                timezoneIndexStr));
          }
          else
          {
            logger.trace(
                "Handle timestamp with timezone old encoding: {}",
                timestampStr);

            fractionsSinceEpoch = parseSecondsSinceEpoch(timestampStr, scale);

            // Timezone needs to be derived from the binary value for old
            // result version
            tz = null;
          }
          break;
        default:
          // Timezone from the environment
          assert resultSetMetaData.getInternalColumnType(columnIndex)
              == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ;
          fractionsSinceEpoch = parseSecondsSinceEpoch(obj.toString(), scale);

          tz = timeZone;
          break;
      }

      // Construct a timestamp in the proper timezone
      return SFTimestamp.fromBinary(fractionsSinceEpoch, scale, tz);
    }
    catch (NumberFormatException ex)
    {
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "Invalid timestamp value: " + obj.toString());
    }
  }

  public SFTime getSFTime(int columnIndex) throws SQLException
  {
    Object obj = getObjectInternal(columnIndex);

    try
    {
      if (obj == null)
      {
        return null;
      }

      int scale = resultSetMetaData.getScale(columnIndex);
      long fractionsSinceMidnight =
          parseSecondsSinceEpoch(obj.toString(), scale).longValue();
      return SFTime.fromFractionalSeconds(fractionsSinceMidnight, scale);
    }
    catch (NumberFormatException ex)
    {
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "Invalid time value: " + obj.toString());
    }
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException
  {
    logger.debug(
               "public InputStream getAsciiStream(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  /**
   * @deprecated
   */
  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException
  {
    logger.debug(
               "public InputStream getUnicodeStream(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException
  {
    logger.debug(
               "public InputStream getBinaryStream(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getString(String columnLabel) throws SQLException
  {
    logger.debug(
               "public String getString(String columnLabel)");

    return getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException
  {
    logger.debug(
               "public boolean getBoolean(String columnLabel)");

    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException
  {
    logger.debug("public byte getByte(String columnLabel)");

    return getByte(findColumn(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException
  {
    logger.debug(
               "public short getShort(String columnLabel)");

    return getShort(findColumn(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException
  {
    logger.debug("public int getInt(String columnLabel)");

    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException
  {
    logger.debug("public long getLong(String columnLabel)");

    return getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException
  {
    logger.debug(
               "public float getFloat(String columnLabel)");

    return getFloat(findColumn(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException
  {
    logger.debug(
               "public double getDouble(String columnLabel)");

    return getDouble(findColumn(columnLabel));
  }

  /**
   * @deprecated
   */
  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale)
          throws SQLException
  {
    logger.debug(
               "public BigDecimal getBigDecimal(String columnLabel, "
               + "int scale)");

    return getBigDecimal(findColumn(columnLabel), scale);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException
  {
    logger.debug(
               "public byte[] getBytes(String columnLabel)");

    return getBytes(findColumn(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException
  {
    logger.debug("public Date getDate(String columnLabel)");

    return getDate(findColumn(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException
  {
    logger.debug("public Time getTime(String columnLabel)");

    return getTime(findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException
  {
    logger.debug(
               "public Timestamp getTimestamp(String columnLabel)");

    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException
  {
    logger.debug(
               "public InputStream getAsciiStream(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  /**
   * @deprecated
   */
  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException
  {
    logger.debug(
               "public InputStream getUnicodeStream(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException
  {
    logger.debug(
               "public InputStream getBinaryStream(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    logger.debug("public SQLWarning getWarnings()");

    return null;
  }

  @Override
  public void clearWarnings() throws SQLException
  {
    logger.debug("public void clearWarnings()");

    // do nothing since warnings are not tracked
  }

  @Override
  public String getCursorName() throws SQLException
  {
    logger.debug("public String getCursorName()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException
  {
    logger.debug("public ResultSetMetaData getMetaData()");

    return resultSetMetaData;
  }

  public Object getObjectInternal(int columnIndex) throws SQLException
  {
    logger.debug(
               "public Object getObjectInternal(int columnIndex)");

    if (nextRow == null)
    {
      throw new SQLException("No row found.");
    }

    if (columnIndex > nextRow.length)
    {
      throw new SQLException("Invalid column index: " + columnIndex);
    }

    wasNull = nextRow[columnIndex - 1] == null;

    logger.debug(
               "Returning column: " + columnIndex + ": "
               + nextRow[columnIndex - 1]);

    return nextRow[columnIndex - 1];
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException
  {
    logger.debug(
               "public Object getObject(int columnIndex)");

    int type = resultSetMetaData.getColumnType(columnIndex);

    Object internalObj = getObjectInternal(columnIndex);
    if (internalObj == null)
      return null;

    switch(type)
    {
      case Types.VARCHAR:
      case Types.CHAR:
        return getString(columnIndex);

      case Types.BINARY:
        return getBytes(columnIndex);

      case Types.INTEGER:
      case Types.SMALLINT:
        return Integer.valueOf(getInt(columnIndex));

      case Types.DECIMAL:
        return getBigDecimal(columnIndex);

      case Types.BIGINT:
        return getLong(columnIndex);

      case Types.DOUBLE:
        return Double.valueOf(getDouble(columnIndex));

      case Types.TIMESTAMP:
        return getTimestamp(columnIndex);

      case Types.DATE:
        return getDate(columnIndex);

      case Types.TIME:
        return getTime(columnIndex);

      case Types.BOOLEAN:
        return getBoolean(columnIndex);

      default:
        throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException
  {
    logger.debug(
               "public Object getObject(String columnLabel)");

    return getObject(findColumn(columnLabel));
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException
  {
    logger.debug(
               "public int findColumn(String columnLabel)");

    int columnIndex = resultSetMetaData.getColumnIndex(columnLabel);

    if (columnIndex == -1)
    {
      throw new SQLException("Column not found: " + columnLabel);
    }
    else
    {
      return ++columnIndex;
    }
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException
  {
    logger.debug(
               "public Reader getCharacterStream(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException
  {
    logger.debug(
               "public Reader getCharacterStream(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException
  {
    logger.debug(
               "public BigDecimal getBigDecimal(int columnIndex)");

    BigDecimal value = null;

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return null;

    if (obj instanceof String)
    {
      value = new BigDecimal((String) obj);
    }
    else
    {
      value = new BigDecimal(obj.toString());
    }

    return value;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException
  {
    logger.debug(
               "public BigDecimal getBigDecimal(String columnLabel)");

    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public boolean isBeforeFirst() throws SQLException
  {
    logger.debug("public boolean isBeforeFirst()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isAfterLast() throws SQLException
  {
    logger.debug("public boolean isAfterLast()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isFirst() throws SQLException
  {
    logger.debug("public boolean isFirst()");

    return row == 1;
  }

  @Override
  public boolean isLast() throws SQLException
  {
    logger.debug("public boolean isLast()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void beforeFirst() throws SQLException
  {
    logger.debug("public void beforeFirst()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void afterLast() throws SQLException
  {
    logger.debug("public void afterLast()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean first() throws SQLException
  {
    logger.debug("public boolean first()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean last() throws SQLException
  {
    logger.debug("public boolean last()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getRow() throws SQLException
  {
    logger.debug("public int getRow()");

    return row;
  }

  @Override
  public boolean absolute(int row) throws SQLException
  {
    logger.debug("public boolean absolute(int row)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean relative(int rows) throws SQLException
  {
    logger.debug("public boolean relative(int rows)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean previous() throws SQLException
  {
    logger.debug("public boolean previous()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException
  {
    logger.debug(
               "public void setFetchDirection(int direction)");

    if (direction != ResultSet.FETCH_FORWARD)
      throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getFetchDirection() throws SQLException
  {
    logger.debug("public int getFetchDirection()");

    return fetchDirection;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException
  {
    logger.debug("public void setFetchSize(int rows)");

    this.fetchSize = rows;
  }

  @Override
  public int getFetchSize() throws SQLException
  {
    logger.debug("public int getFetchSize()");

    return this.fetchSize;
  }

  @Override
  public int getType() throws SQLException
  {
    logger.debug("public int getType()");

    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException
  {
    logger.debug("public int getConcurrency()");

    throw new SQLFeatureNotSupportedException(
        "Feature not supported",
        ErrorCode.FEATURE_UNSUPPORTED.getSqlState(),
        ErrorCode.FEATURE_UNSUPPORTED.getMessageCode());
  }

  @Override
  public boolean rowUpdated() throws SQLException
  {
    logger.debug("public boolean rowUpdated()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowInserted() throws SQLException
  {
    logger.debug("public boolean rowInserted()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowDeleted() throws SQLException
  {
    logger.debug("public boolean rowDeleted()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException
  {
    logger.debug("public void updateNull(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException
  {
    logger.debug(
               "public void updateBoolean(int columnIndex, boolean x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException
  {
    logger.debug(
               "public void updateByte(int columnIndex, byte x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException
  {
    logger.debug(
               "public void updateShort(int columnIndex, short x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException
  {
    logger.debug(
               "public void updateInt(int columnIndex, int x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException
  {
    logger.debug(
               "public void updateLong(int columnIndex, long x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException
  {
    logger.debug(
               "public void updateFloat(int columnIndex, float x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException
  {
    logger.debug(
               "public void updateDouble(int columnIndex, double x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x)
          throws SQLException
  {
    logger.debug(
               "public void updateBigDecimal(int columnIndex, BigDecimal x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException
  {
    logger.debug(
               "public void updateString(int columnIndex, String x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException
  {
    logger.debug(
               "public void updateBytes(int columnIndex, byte[] x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException
  {
    logger.debug(
               "public void updateDate(int columnIndex, Date x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException
  {
    logger.debug(
               "public void updateTime(int columnIndex, Time x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException
  {
    logger.debug(
               "public void updateTimestamp(int columnIndex, Timestamp x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length)
          throws SQLException
  {
    logger.debug(
               "public void updateAsciiStream(int columnIndex, "
               + "InputStream x, int length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length)
          throws SQLException
  {
    logger.debug(
               "public void updateBinaryStream(int columnIndex, "
               + "InputStream x, int length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length)
          throws SQLException
  {
    logger.debug(
               "public void updateCharacterStream(int columnIndex, "
               + "Reader x, int length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength)
          throws SQLException
  {
    logger.debug(
               "public void updateObject(int columnIndex, Object x, "
               + "int scaleOrLength)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException
  {
    logger.debug(
               "public void updateObject(int columnIndex, Object x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException
  {
    logger.debug(
               "public void updateNull(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException
  {
    logger.debug(
               "public void updateBoolean(String columnLabel, boolean x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException
  {
    logger.debug(
               "public void updateByte(String columnLabel, byte x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException
  {
    logger.debug(
               "public void updateShort(String columnLabel, short x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException
  {
    logger.debug(
               "public void updateInt(String columnLabel, int x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException
  {
    logger.debug(
               "public void updateLong(String columnLabel, long x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException
  {
    logger.debug(
               "public void updateFloat(String columnLabel, float x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException
  {
    logger.debug(
               "public void updateDouble(String columnLabel, double x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x)
          throws SQLException
  {
    logger.debug(
               "public void updateBigDecimal(String columnLabel, "
               + "BigDecimal x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException
  {
    logger.debug(
               "public void updateString(String columnLabel, String x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException
  {
    logger.debug(
               "public void updateBytes(String columnLabel, byte[] x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException
  {
    logger.debug(
               "public void updateDate(String columnLabel, Date x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException
  {
    logger.debug(
               "public void updateTime(String columnLabel, Time x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x)
          throws SQLException
  {
    logger.debug(
               "public void updateTimestamp(String columnLabel, Timestamp x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
          throws SQLException
  {
    logger.debug(
               "public void updateAsciiStream(String columnLabel, "
               + "InputStream x, int length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
          throws SQLException
  {
    logger.debug(
               "public void updateBinaryStream(String columnLabel, "
               + "InputStream x, int length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader,
                                    int length) throws SQLException
  {
    logger.debug(
               "public void updateCharacterStream(String columnLabel, "
               + "Reader reader,int length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength)
          throws SQLException
  {
    logger.debug(
               "public void updateObject(String columnLabel, Object x, "
               + "int scaleOrLength)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException
  {
    logger.debug(
               "public void updateObject(String columnLabel, Object x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void insertRow() throws SQLException
  {
    logger.debug("public void insertRow()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRow() throws SQLException
  {
    logger.debug("public void updateRow()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void deleteRow() throws SQLException
  {
    logger.debug("public void deleteRow()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void refreshRow() throws SQLException
  {
    logger.debug("public void refreshRow()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancelRowUpdates() throws SQLException
  {
    logger.debug("public void cancelRowUpdates()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToInsertRow() throws SQLException
  {
    logger.debug("public void moveToInsertRow()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToCurrentRow() throws SQLException
  {
    logger.debug("public void moveToCurrentRow()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Statement getStatement() throws SQLException
  {
    logger.debug("public Statement getStatement()");

    return statement;
  }

  @Override
  public Object getObject(int columnIndex,
                          Map<String, Class<?>> map) throws SQLException
  {
    logger.debug(
               "public Object getObject(int columnIndex, Map<String, "
               + "Class<?>> map)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException
  {
    logger.debug("public Ref getRef(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException
  {
    logger.debug("public Blob getBlob(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException
  {
    logger.debug("public Clob getClob(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException
  {
    logger.debug("public Array getArray(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Object getObject(String columnLabel,
                          Map<String, Class<?>> map) throws SQLException
  {
    logger.debug(
               "public Object getObject(String columnLabel, "
               + "Map<String, Class<?>> map)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException
  {
    logger.debug("public Ref getRef(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException
  {
    logger.debug("public Blob getBlob(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException
  {
    logger.debug("public Clob getClob(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException
  {
    logger.debug(
               "public Array getArray(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException
  {
    logger.debug(
               "public Date getDate(int columnIndex, Calendar cal)");

    return getDate(columnIndex, cal.getTimeZone());
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException
  {
    logger.debug(
               "public Date getDate(String columnLabel, Calendar cal)");

    return getDate(findColumn(columnLabel), cal.getTimeZone());
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException
  {
    logger.debug(
               "public Time getTime(int columnIndex, Calendar cal)");

    return getTime(columnIndex);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException
  {
    logger.debug(
               "public Time getTime(String columnLabel, Calendar cal)");

    return getTime(columnLabel);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal)
          throws SQLException
  {
    logger.debug(
               "public Timestamp getTimestamp(int columnIndex, Calendar cal)");

    return getTimestamp(columnIndex, cal.getTimeZone());
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal)
          throws SQLException
  {
    logger.debug(
               "public Timestamp getTimestamp(String columnLabel, "
               + "Calendar cal)");

    return getTimestamp(findColumn(columnLabel), cal.getTimeZone());
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException
  {
    logger.debug("public URL getURL(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException
  {
    logger.debug("public URL getURL(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException
  {
    logger.debug(
               "public void updateRef(int columnIndex, Ref x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException
  {
    logger.debug(
               "public void updateRef(String columnLabel, Ref x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException
  {
    logger.debug(
               "public void updateBlob(int columnIndex, Blob x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException
  {
    logger.debug(
               "public void updateBlob(String columnLabel, Blob x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException
  {
    logger.debug(
               "public void updateClob(int columnIndex, Clob x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException
  {
    logger.debug(
               "public void updateClob(String columnLabel, Clob x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException
  {
    logger.debug(
               "public void updateArray(int columnIndex, Array x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException
  {
    logger.debug(
               "public void updateArray(String columnLabel, Array x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException
  {
    logger.debug("public RowId getRowId(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException
  {
    logger.debug(
               "public RowId getRowId(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException
  {
    logger.debug(
               "public void updateRowId(int columnIndex, RowId x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException
  {
    logger.debug(
               "public void updateRowId(String columnLabel, RowId x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability() throws SQLException
  {
    logger.debug("public int getHoldability()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isClosed() throws SQLException
  {
    logger.debug("public boolean isClosed()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException
  {
    logger.debug(
               "public void updateNString(int columnIndex, String nString)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(String columnLabel, String nString)
          throws SQLException
  {
    logger.debug(
               "public void updateNString(String columnLabel, String nString)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException
  {
    logger.debug(
               "public void updateNClob(int columnIndex, NClob nClob)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException
  {
    logger.debug(
               "public void updateNClob(String columnLabel, NClob nClob)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException
  {
    logger.debug("public NClob getNClob(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException
  {
    logger.debug(
               "public NClob getNClob(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException
  {
    logger.debug(
               "public SQLXML getSQLXML(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException
  {
    logger.debug(
               "public SQLXML getSQLXML(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject)
          throws SQLException
  {
    logger.debug(
               "public void updateSQLXML(int columnIndex, SQLXML xmlObject)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject)
          throws SQLException
  {
    logger.debug(
               "public void updateSQLXML(String columnLabel, SQLXML xmlObject)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(int columnIndex) throws SQLException
  {
    logger.debug(
               "public String getNString(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(String columnLabel) throws SQLException
  {
    logger.debug(
               "public String getNString(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException
  {
    logger.debug(
               "public Reader getNCharacterStream(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException
  {
    logger.debug(
                "public Reader getNCharacterStream(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateNCharacterStream(int columnIndex, "
               + "Reader x, long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader,
                                     long length) throws SQLException
  {
    logger.debug(
               "public void updateNCharacterStream(String columnLabel, "
               + "Reader reader,long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateAsciiStream(int columnIndex, "
               + "InputStream x, long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateBinaryStream(int columnIndex, "
               + "InputStream x, long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateCharacterStream(int columnIndex, Reader x, "
               + "long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateAsciiStream(String columnLabel, "
               + "InputStream x, long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateBinaryStream(String columnLabel, "
               + "InputStream x, long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader,
                                    long length) throws SQLException
  {
    logger.debug(
               "public void updateCharacterStream(String columnLabel, "
               + "Reader reader,long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateBlob(int columnIndex, InputStream "
               + "inputStream, long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream,
                         long length) throws SQLException
  {
    logger.debug(
               "public void updateBlob(String columnLabel, "
               + "InputStream inputStream,long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateClob(int columnIndex, Reader reader, "
               + "long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateClob(String columnLabel, Reader reader, "
               + "long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateNClob(int columnIndex, Reader reader, "
               + "long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateNClob(String columnLabel, Reader reader, "
               + "long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x)
          throws SQLException
  {
    logger.debug(
               "public void updateNCharacterStream(int columnIndex, Reader x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader)
          throws SQLException
  {
    logger.debug(
               "public void updateNCharacterStream(String columnLabel, "
               + "Reader reader)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x)
          throws SQLException
  {
    logger.debug(
               "public void updateAsciiStream(int columnIndex, InputStream x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x)
          throws SQLException
  {
    logger.debug(
               "public void updateBinaryStream(int columnIndex, InputStream x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x)
          throws SQLException
  {
    logger.debug(
               "public void updateCharacterStream(int columnIndex, Reader x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x)
          throws SQLException
  {
    logger.debug(
               "public void updateAsciiStream(String columnLabel, InputStream x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x)
          throws SQLException
  {
    logger.debug(
               "public void updateBinaryStream(String columnLabel, InputStream x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader)
          throws SQLException
  {
    logger.debug(
               "public void updateCharacterStream(String columnLabel, "
               + "Reader reader)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream)
          throws SQLException
  {
    logger.debug(
               "public void updateBlob(int columnIndex, InputStream inputStream)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream)
          throws SQLException
  {
    logger.debug(
               "public void updateBlob(String columnLabel, InputStream "
               + "inputStream)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException
  {
    logger.debug(
               "public void updateClob(int columnIndex, Reader reader)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException
  {
    logger.debug(
               "public void updateClob(String columnLabel, Reader reader)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException
  {
    logger.debug(
               "public void updateNClob(int columnIndex, Reader reader)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException
  {
    logger.debug(
               "public void updateNClob(String columnLabel, Reader reader)");

    throw new SQLFeatureNotSupportedException();
  }

  //@Override
  public <T> T getObject(int columnIndex,
                         Class<T> type) throws SQLException
  {
    logger.debug(
               "public <T> T getObject(int columnIndex,Class<T> type)");

    throw new SQLFeatureNotSupportedException();
  }

  //@Override
  public <T> T getObject(String columnLabel,
                         Class<T> type) throws SQLException
  {
    logger.debug(
               "public <T> T getObject(String columnLabel,Class<T> type)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(
          Class<T> iface) throws SQLException
  {
    logger.debug("public <T> T unwrap(Class<T> iface)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(
          Class<?> iface) throws SQLException
  {
    logger.debug(
               "public boolean isWrapperFor(Class<?> iface)");

    throw new SQLFeatureNotSupportedException();
  }
}
