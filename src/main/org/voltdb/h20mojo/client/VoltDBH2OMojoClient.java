package org.voltdb.h20mojo.client;



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

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.voltutil.schemabuilder.VoltDBSchemaBuilder;

public class VoltDBH2OMojoClient {

  /**
   * DDL statements for the VoltDB implementation of TATP. Note that you just
   * run this using SQLCMD, but the make this implementation easier to re-create
   * we do it Programmatically.
   */
  final String[] ddlStatements = {
      "CREATE TABLE cached_results (origin varchar(20) NOT NULL, dest varchar(20) not null,CRSDepTime varchar(4) not null, "
          + " year  varchar(4) not null,  month  varchar(2) not null,  dayOfMonth   varchar(2) not null,  dayOfWeek   varchar(1) not null,\n"
          + "       uniqueCarrier   varchar(2) not null, delayed varchar(3) not null, last_used timestamp not null, usage_count bigint not null"
          + ", PRIMARY KEY (origin, dest, CRSDepTime, year, month, dayOfMonth, dayOfWeek, uniqueCarrier))"
          + " USING TTL 5 MINUTES ON COLUMN last_used;",

      "CREATE INDEX cached_results_ttl_idx on cached_results(last_used);",

      "PARTITION TABLE cached_results ON COLUMN origin;",

      "create view cache_effectiveness as select usage_count, count(*) hits from cached_results group by usage_count;"

  };

  /**
   * zip files containing resources we want to use. Note that they are located
   * in the same directory our stored procs are...
   */
  final String[] zipFiles = { "gbm_pojo_test.zip"
      // ,"StackedEnsemble_BestOfFamily_AutoML_20191007_160201.zip"
      // ,"deeplearning_5004dbaa_17ab_4a57_99c0_a62273b7f1be.zip"
      // ,"Automotive-ResearchDataSet-VIF-AEGIS.zip"
  };

  /**
   * Procedure statements for the VoltDB implementation of TATP. Note that you
   * just run this using SQLCMD, but the make this implementation easier to
   * re-create we do it Programmatically.
   */
  final String[] procStatements = {

      "CREATE PROCEDURE PARTITION ON TABLE cached_results COLUMN origin FROM CLASS mojoprocs.IsFlightLate;",

      "create procedure check_cache as select * from cache_effectiveness;"

  };
  
  final String[] otherClasses = {
      "ie.voltdb.h2outil.H2OMojoWrangler"
  };

  // We only create the DDL and procedures if a call to testProcName with
  // testParams fails....
  final String testProcName = "IsFlightLate";
  final Object[] testParams = { "SAN", "0730", "1987", "10", "14", "3", "PS", "SFO", 1 };

  Client client = null;

  Random r = new Random(42);

  private static Logger logger = LoggerFactory.getLogger(VoltDBH2OMojoClient.class);

  public VoltDBH2OMojoClient(String hostnames) {
    super();

    try {
      client = connectVoltDB(hostnames);

    } catch (Exception e) {
      logger.error(e.getMessage());
    }

  }

  public static void main(String[] args) {

    msg("Parameters:" + Arrays.toString(args));

    String hostnames = "localhost";
    int durationSeconds = 60;

    if (args.length > 0) {
      hostnames = args[0];
      durationSeconds = Integer.parseInt(args[1]);
    }

    try {

      VoltDBH2OMojoClient mc = new VoltDBH2OMojoClient(hostnames);
      mc.createSchemaIfNeeded();
      Random r = new Random();
      String[] origin = getOrigin();
      String[] dest = getDest();

      final long endTimeMs = System.currentTimeMillis() + (durationSeconds * 1000);

      int predictionCount = 0;

      while (System.currentTimeMillis() < endTimeMs) {

        String depTime = "0730";
        String year = "1987";
        String month = "10";
        String day = "14";
        String dayOfWeek = "3";
        String airline = "PS";

        String depHour = "" + r.nextInt(24);
        if (depHour.length() < 2) {
          depHour = "0" + depHour;
        }

        String depMin = "" + r.nextInt(6);
        if (depMin.length() < 2) {
          depMin = "0" + depMin;
        }

        depTime = depHour + depMin;

        @SuppressWarnings("unused")
        String prediction = mc.getPrediction(origin[r.nextInt(origin.length)], depTime, year, month, day, dayOfWeek,
            airline, dest[r.nextInt(dest.length)]);

        if (++predictionCount % 1000 == 0) {
          msg(predictionCount + " predictions done.");
        }

      }

      mc.checkCache();
      mc.disconnect();

    } catch (Exception e) {
      logger.error(e.getMessage());

    }

    msg("Finished");

  }

  private void checkCache() {

    if (client != null) {
      try {
        ClientResponse cr = client.callProcedure("check_cache");
        if (cr.getResults()[0].advanceRow()) {
          msg(cr.getResults()[0].toFormattedString());
        }
      } catch (IOException | ProcCallException e) {
        logger.error(e.getMessage());
      }
    }

  }

  private String getPrediction(String origin, String dest, String depTime, String year, String month, String day,
      String dayOfWeek, String airline) {

    String prediction = "";

    if (client != null) {
      try {
        ClientResponse cr = client.callProcedure("IsFlightLate", origin, dest, depTime, year, month, day, dayOfWeek,
            airline, 1);
        if (cr.getResults()[0].advanceRow()) {
          prediction = cr.getResults()[0].getString("LABEL");
        }
      } catch (IOException | ProcCallException e) {
        logger.error(e.getMessage());
      }
    }

    return prediction;

  }

  public void createSchemaIfNeeded() throws Exception {

    VoltDBSchemaBuilder b = new VoltDBSchemaBuilder(ddlStatements, procStatements, zipFiles, "mojoProcs.jar", client,
        "mojoprocs", testProcName, testParams,otherClasses);

    b.setMaxZipFileSize(50000);
    b.loadClassesAndDDLIfNeeded();

  }

  private void disconnect() {

    if (client != null) {
      try {

        client.drain();
        client.close();

      } catch (InterruptedException e) {
        logger.error(e.getMessage());
      } catch (NoConnectionsException e) {
        logger.error(e.getMessage());
      }

      client = null;
    }

  }

  private static final String[] getDest() {
    String[] sa = new String[134];

    sa[0] = "ABE";
    sa[1] = "ABQ";
    sa[2] = "ACY";
    sa[3] = "ALB";
    sa[4] = "AMA";
    sa[5] = "ANC";
    sa[6] = "ATL";
    sa[7] = "AUS";
    sa[8] = "AVL";
    sa[9] = "AVP";
    sa[10] = "BDL";
    sa[11] = "BGM";
    sa[12] = "BHM";
    sa[13] = "BNA";
    sa[14] = "BOI";
    sa[15] = "BOS";
    sa[16] = "BTV";
    sa[17] = "BUF";
    sa[18] = "BUR";
    sa[19] = "BWI";
    sa[20] = "CAE";
    sa[21] = "CAK";
    sa[22] = "CHA";
    sa[23] = "CHO";
    sa[24] = "CHS";
    sa[25] = "CLE";
    sa[26] = "CLT";
    sa[27] = "CMH";
    sa[28] = "COS";
    sa[29] = "CRP";
    sa[30] = "CVG";
    sa[31] = "DAL";
    sa[32] = "DAY";
    sa[33] = "DCA";
    sa[34] = "DEN";
    sa[35] = "DFW";
    sa[36] = "DSM";
    sa[37] = "DTW";
    sa[38] = "ELM";
    sa[39] = "ELP";
    sa[40] = "ERI";
    sa[41] = "EUG";
    sa[42] = "EWR";
    sa[43] = "EYW";
    sa[44] = "FAT";
    sa[45] = "FAY";
    sa[46] = "FLL";
    sa[47] = "FNT";
    sa[48] = "GEG";
    sa[49] = "GRR";
    sa[50] = "GSO";
    sa[51] = "GSP";
    sa[52] = "HNL";
    sa[53] = "HOU";
    sa[54] = "HPN";
    sa[55] = "HRL";
    sa[56] = "HTS";
    sa[57] = "IAD";
    sa[58] = "IAH";
    sa[59] = "ICT";
    sa[60] = "ILM";
    sa[61] = "IND";
    sa[62] = "ISP";
    sa[63] = "JAN";
    sa[64] = "JAX";
    sa[65] = "JFK";
    sa[66] = "KOA";
    sa[67] = "LAS";
    sa[68] = "LAX";
    sa[69] = "LBB";
    sa[70] = "LEX";
    sa[71] = "LGA";
    sa[72] = "LIH";
    sa[73] = "LIT";
    sa[74] = "LYH";
    sa[75] = "MAF";
    sa[76] = "MCI";
    sa[77] = "MCO";
    sa[78] = "MDT";
    sa[79] = "MDW";
    sa[80] = "MHT";
    sa[81] = "MIA";
    sa[82] = "MKE";
    sa[83] = "MRY";
    sa[84] = "MSP";
    sa[85] = "MSY";
    sa[86] = "MYR";
    sa[87] = "OAJ";
    sa[88] = "OAK";
    sa[89] = "OGG";
    sa[90] = "OKC";
    sa[91] = "OMA";
    sa[92] = "ONT";
    sa[93] = "ORD";
    sa[94] = "ORF";
    sa[95] = "ORH";
    sa[96] = "PBI";
    sa[97] = "PDX";
    sa[98] = "PHF";
    sa[99] = "PHL";
    sa[100] = "PHX";
    sa[101] = "PIT";
    sa[102] = "PNS";
    sa[103] = "PSP";
    sa[104] = "PVD";
    sa[105] = "PWM";
    sa[106] = "RDU";
    sa[107] = "RIC";
    sa[108] = "RNO";
    sa[109] = "ROA";
    sa[110] = "ROC";
    sa[111] = "RSW";
    sa[112] = "SAN";
    sa[113] = "SAT";
    sa[114] = "SBN";
    sa[115] = "SCK";
    sa[116] = "SDF";
    sa[117] = "SEA";
    sa[118] = "SFO";
    sa[119] = "SJC";
    sa[120] = "SJU";
    sa[121] = "SLC";
    sa[122] = "SMF";
    sa[123] = "SNA";
    sa[124] = "SRQ";
    sa[125] = "STL";
    sa[126] = "STT";
    sa[127] = "SWF";
    sa[128] = "SYR";
    sa[129] = "TOL";
    sa[130] = "TPA";
    sa[131] = "TUL";
    sa[132] = "TUS";
    sa[133] = "UCA";

    return sa;
  }

  private static final String[] getOrigin() {

    String[] sa = new String[132];

    sa[0] = "ABE";
    sa[1] = "ABQ";
    sa[2] = "ACY";
    sa[3] = "ALB";
    sa[4] = "AMA";
    sa[5] = "ANC";
    sa[6] = "ATL";
    sa[7] = "AUS";
    sa[8] = "AVP";
    sa[9] = "BDL";
    sa[10] = "BGM";
    sa[11] = "BHM";
    sa[12] = "BIL";
    sa[13] = "BNA";
    sa[14] = "BOI";
    sa[15] = "BOS";
    sa[16] = "BTV";
    sa[17] = "BUF";
    sa[18] = "BUR";
    sa[19] = "BWI";
    sa[20] = "CAE";
    sa[21] = "CHO";
    sa[22] = "CHS";
    sa[23] = "CLE";
    sa[24] = "CLT";
    sa[25] = "CMH";
    sa[26] = "COS";
    sa[27] = "CRP";
    sa[28] = "CRW";
    sa[29] = "CVG";
    sa[30] = "DAL";
    sa[31] = "DAY";
    sa[32] = "DCA";
    sa[33] = "DEN";
    sa[34] = "DFW";
    sa[35] = "DSM";
    sa[36] = "DTW";
    sa[37] = "EGE";
    sa[38] = "ELP";
    sa[39] = "ERI";
    sa[40] = "EWR";
    sa[41] = "EYW";
    sa[42] = "FLL";
    sa[43] = "GEG";
    sa[44] = "GNV";
    sa[45] = "GRR";
    sa[46] = "GSO";
    sa[47] = "HNL";
    sa[48] = "HOU";
    sa[49] = "HPN";
    sa[50] = "HRL";
    sa[51] = "IAD";
    sa[52] = "IAH";
    sa[53] = "ICT";
    sa[54] = "IND";
    sa[55] = "ISP";
    sa[56] = "JAN";
    sa[57] = "JAX";
    sa[58] = "JFK";
    sa[59] = "KOA";
    sa[60] = "LAN";
    sa[61] = "LAS";
    sa[62] = "LAX";
    sa[63] = "LBB";
    sa[64] = "LEX";
    sa[65] = "LGA";
    sa[66] = "LIH";
    sa[67] = "LIT";
    sa[68] = "LYH";
    sa[69] = "MAF";
    sa[70] = "MCI";
    sa[71] = "MCO";
    sa[72] = "MDT";
    sa[73] = "MDW";
    sa[74] = "MEM";
    sa[75] = "MFR";
    sa[76] = "MHT";
    sa[77] = "MIA";
    sa[78] = "MKE";
    sa[79] = "MLB";
    sa[80] = "MRY";
    sa[81] = "MSP";
    sa[82] = "MSY";
    sa[83] = "MYR";
    sa[84] = "OAK";
    sa[85] = "OGG";
    sa[86] = "OKC";
    sa[87] = "OMA";
    sa[88] = "ONT";
    sa[89] = "ORD";
    sa[90] = "ORF";
    sa[91] = "PBI";
    sa[92] = "PDX";
    sa[93] = "PHF";
    sa[94] = "PHL";
    sa[95] = "PHX";
    sa[96] = "PIT";
    sa[97] = "PSP";
    sa[98] = "PVD";
    sa[99] = "PWM";
    sa[100] = "RDU";
    sa[101] = "RIC";
    sa[102] = "RNO";
    sa[103] = "ROA";
    sa[104] = "ROC";
    sa[105] = "RSW";
    sa[106] = "SAN";
    sa[107] = "SAT";
    sa[108] = "SAV";
    sa[109] = "SBN";
    sa[110] = "SCK";
    sa[111] = "SDF";
    sa[112] = "SEA";
    sa[113] = "SFO";
    sa[114] = "SJC";
    sa[115] = "SJU";
    sa[116] = "SLC";
    sa[117] = "SMF";
    sa[118] = "SNA";
    sa[119] = "SRQ";
    sa[120] = "STL";
    sa[121] = "STT";
    sa[122] = "STX";
    sa[123] = "SWF";
    sa[124] = "SYR";
    sa[125] = "TLH";
    sa[126] = "TPA";
    sa[127] = "TRI";
    sa[128] = "TUL";
    sa[129] = "TUS";
    sa[130] = "TYS";
    sa[131] = "UCA";

    return sa;

  }

  private static Client connectVoltDB(String hostnames) throws Exception {
    Client newClient = null;
    ClientConfig config = null;

    try {
      msg("Logging into VoltDB");

      config = new ClientConfig(); // "admin", "idontknow");
      config.setMaxOutstandingTxns(200000);
      config.setMaxTransactionsPerSecond(5000000);
      config.setTopologyChangeAware(true);
      config.setReconnectOnConnectionLoss(true);
      config.setHeavyweight(true);

      newClient = ClientFactory.createClient(config);
      String[] hostnameArray = hostnames.split(",");

      for (int i = 0; i < hostnameArray.length; i++) {
        msg("Connect to " + hostnameArray[i] + "...");
        try {
          newClient.createConnection(hostnameArray[i]);
        } catch (Exception e) {
          msg(e.getMessage());
        }
      }

    } catch (Exception e) {
      logger.error(e.getMessage());
      throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
    }

    return newClient;

  }

  public static void msg(String message) {

    System.out.println(message);
    logger.info(message);
  }

 
}
