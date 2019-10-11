package mojoprocs;



/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import hex.genmodel.easy.RowData;
import hex.genmodel.ModelMojoReader;
import hex.genmodel.MojoModel;
import hex.genmodel.MojoReaderBackend;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.prediction.*;
import ie.voltdb.h2outil.H2OMojoWrangler;

/**
 * VoltDB procedure to invoke a generated h20.ai class that uses a MOJO.
 * 
 * @author drolfe
 *
 */
public class IsFlightLate extends VoltProcedure {

  /**
   * Name of h20.ai class we're going to use.
   */
  private static final String modelZipFileName = "gbm_pojo_test.zip";

  EasyPredictModelWrapper modelWrapper = null;

  public static final SQLStmt seeIfCached = new SQLStmt("SELECT * FROM CACHED_RESULTS WHERE origin = ? "
      + "AND dest = ? AND CRSDepTime = ? AND year = ? AND month = ? AND dayOfMonth = ? "
      + "AND dayOfWeek = ? AND uniqueCarrier= ?;");

  public static final SQLStmt trackCacheUsage = new SQLStmt(
      "UPDATE CACHED_RESULTS set last_used = NOW, " + "usage_count = usage_count + 1 WHERE origin = ? "
          + "AND dest = ? AND CRSDepTime = ? AND year = ? AND month = ? AND dayOfMonth = ? "
          + "AND dayOfWeek = ? AND uniqueCarrier= ?;");

  public static final SQLStmt addCacheEntry = new SQLStmt(
      "INSERT INTO CACHED_RESULTS " + "(origin, dest,cRSDepTime,  year,  month,  dayOfMonth,"
          + "       dayOfWeek,  uniqueCarrier,  last_used,usage_count, delayed) " + " VALUES "
          + " (?,?,?,?,?,?,?,?,NOW,1,?);");

  /**
   * This VoltDB procedure uses an H20.AI function to guess whether a given
   * flight will be late. To make the example as simple as possible all values
   * are passed in as parameters. In a real world deployment we would obviously
   * pass in a Primary Key and the retrieve all the other fields. This example
   * uses a US Federal Aviation Administration dataset of commercial flight
   * data. We also cache the data and print stats if asked.
   * 
   * Note that all VoltDB procedure calls have a method like:
   * 
   * <br>
   * <code>
   * VoltTable[] run(params...);
   * </code>
   * 
   * @param origin
   *          Origin Airport
   * @param cRSDepTime
   *          Depature time
   * @param year
   *          year
   * @param month
   *          Month
   * @param dayOfMonth
   *          Day
   * @param dayOfWeek
   *          Day of week
   * @param uniqueCarrier
   *          Airline
   * @param dest
   *          Destination Airport
   * @param doStats
   *          - Dump nanosecond timings to System.out if == 1.
   * @return pmmlOut An array of VoltTable objects containing the results.
   * @throws VoltAbortException
   */
  public VoltTable[] run(String origin, String cRSDepTime, String year, String month, String dayOfMonth,
      String dayOfWeek, String uniqueCarrier, String dest, int doStats) throws VoltAbortException {

    long startNs = System.nanoTime();

    // We need to return an array of VoltTable[]. Normally we get
    // VoltTable's by issuing SQL queries. In this case we'll be inventing
    // one based on the results of h20.
    VoltTable[] h2oOut = null;

    // We keep track of various durations in nanoseconds.
    long cacheCheckNs = -1;
    long durationCreateModeNs = -1;
    long durationModelExecNs = -1;

    String result = null;

    // First thing: See if we have answered this question before...
    voltQueueSQL(seeIfCached, origin, dest, cRSDepTime, year, month, dayOfMonth, dayOfWeek, uniqueCarrier);
    VoltTable[] cacheResults = voltExecuteSQL();
    cacheCheckNs = System.nanoTime() - startNs;

    if (cacheResults[0].advanceRow()) {

      // We know the answer, so send that back...
      result = cacheResults[0].getString("DELAYED");
      h2oOut = createH2Oout(result);

      // Update cache so we know it's useful.
      voltQueueSQL(trackCacheUsage, origin, dest, cRSDepTime, year, month, dayOfMonth, dayOfWeek, uniqueCarrier);

    } else {

      // use h2o to do a prediction and then cache the result before returning
      // it.

      // H2o uses a very large Java object. We can't afford to instantiate it
      // each call...
      try {

        // keep track of how long it takes to instantiate the modelWrapper.
        // VoltDB
        // procedures normally run in tenths or hundreths of a milliseconds.
        // If it takes too long to instantiate the ML engine you need to do
        // define it as a variable and then instantiate it if and only if
        // it's null.
        startNs = System.nanoTime();

        if (modelWrapper == null) {
          synchronized (this) {
            if (modelWrapper == null) {

              // Note that the zip file needs to be in the same directory in the
              // JAR
              // file as the procedures we are creating...

              // URL mojoURL = IsFlightLate.class.getResource(modelZipFileName);
              MojoReaderBackend reader = H2OMojoWrangler.createInMemoryReaderBackendFromSetOfZipFiles(modelZipFileName);

              // = MojoReaderBackendFactory.createReaderBackend(mojoURL,
              // MojoReaderBackendFactory.CachingStrategy.MEMORY);
              MojoModel model = ModelMojoReader.readFrom(reader);
              modelWrapper = new EasyPredictModelWrapper(model);
            }
          }

        }

        durationCreateModeNs = System.nanoTime() - startNs;

        // Load our params into the data structure uses by H20...
        RowData row = new RowData();
        row.put("Year", year);
        row.put("Month", month);
        row.put("DayofMonth", dayOfMonth);
        row.put("DayOfWeek", dayOfWeek);
        row.put("CRSDepTime", cRSDepTime);
        row.put("UniqueCarrier", uniqueCarrier);
        row.put("Origin", origin);
        row.put("Dest", dest);

        // Run the modelWrapper. As before we track the time it takes.
        startNs = System.nanoTime();
        BinomialModelPrediction p = modelWrapper.predictBinomial(row);
        durationModelExecNs = System.nanoTime() - startNs;

        // Cache for future use...
        voltQueueSQL(addCacheEntry, origin, dest, cRSDepTime, year, month, dayOfMonth, dayOfWeek, uniqueCarrier,
            p.label);

        // We now need to load the results into a VoltTable.
        result = p.label;
        h2oOut = createH2Oout(result);

      } catch (Exception e) {

        System.err.println(e.getMessage());

        // VoltAbortException undoes all the DB changes made by a stored
        // procedure call. In thus case there aren't any, but if we added
        // logic to store or change state after the call we'd need this.
        throw new VoltAbortException(e);

      }
    }

    // Execute any SQL statements we have queued as a last step. In this
    // case it's a null-op.
    startNs = System.nanoTime();
    voltExecuteSQL(true);
    long updateDBNs = System.nanoTime() - startNs;

    // Note that we dump stats to System.out. We do *not* send them back as
    // part of a result. In VoltDB we implement High Availability by running the
    // same procedure in two or more places at once, so we can't have any copy
    // of
    // a procedure return different answers. It's very unlikely nanosecond
    // timings
    // will be the same in two places, even with the same inputs. The VoltDB
    // client
    // will get very upset if two copies of a procedure return different
    // answers,
    // so we dump the stats to Standard Output.
    if (doStats == 1) {
      printToSystemDotOut(origin, cRSDepTime, year, month, dayOfMonth, dayOfWeek, uniqueCarrier, dest, cacheCheckNs,
          durationCreateModeNs, durationModelExecNs, result, updateDBNs);
    }

    // Return the array we invented.
    return h2oOut;
  }

  /**
   * @param origin
   * @param cRSDepTime
   * @param year
   * @param month
   * @param dayOfMonth
   * @param dayOfWeek
   * @param uniqueCarrier
   * @param dest
   * @param cacheCheckNs
   * @param durationCreateModeNs
   * @param durationModelExecNs
   * @param result
   * @param updateDBNs
   */
  private void printToSystemDotOut(String origin, String cRSDepTime, String year, String month, String dayOfMonth,
      String dayOfWeek, String uniqueCarrier, String dest, long cacheCheckNs, long durationCreateModeNs,
      long durationModelExecNs, String result, long updateDBNs) {
    StringBuffer b = new StringBuffer(cRSDepTime);
    b.append(" ");
    b.append(year);
    b.append(" ");
    b.append(month);
    b.append(" ");
    b.append(dayOfMonth);
    b.append(" ");
    b.append(dayOfWeek);
    b.append(" ");
    b.append(uniqueCarrier);
    b.append(" ");
    b.append(origin);
    b.append(" ");
    b.append(dest);
    b.append(" Result=");
    b.append(result);
    b.append(" Cache Check/Model Create/Model Exec/Update DB Exec time=");
    b.append(cacheCheckNs);
    b.append("/");
    b.append(durationCreateModeNs);
    b.append("/");
    b.append(durationModelExecNs);
    b.append("/");
    b.append(updateDBNs);
    System.out.println(b.toString());
  }

  private VoltTable[] createH2Oout(String value) {

    // We now need to load the results into a VoltTable.
    VoltTable[] newH2oOut = new VoltTable[1];

    // Our table will have one row, a LABEL column.
    VoltTable.ColumnInfo[] cols = new VoltTable.ColumnInfo[1];
    Object[] vals = new Object[1];

    cols[0] = new VoltTable.ColumnInfo("LABEL", VoltType.STRING);
    vals[0] = value;

    // We are required to return an array every though we have only one
    // table. Create an try in position 0.
    newH2oOut[0] = new VoltTable(cols);

    // Add the row we invented.
    newH2oOut[0].addRow(vals);

    return newH2oOut;

  }

 
}