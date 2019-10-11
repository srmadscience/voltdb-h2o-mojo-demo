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
package org.voltdb.voltutil.schemabuilder;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

/**
 * Utility class to build a schema.
 * 
 */
public final class VoltDBSchemaBuilder {

  private static final String PROCEDURE = "Procedure ";
  private static final String WAS_NOT_FOUND = " was not found";

  private String jarFileName;

  private Logger logger = LoggerFactory.getLogger(VoltDBSchemaBuilder.class);

  Client voltClient;

  private String[] ddlStatements;

  private String[] procStatements;

  private String[] jarFiles;

  private String[] zipFiles;

  private String[] otherClasses;
  
  String procPackageName;

  String testProcName;

  Object[] testParams;

  // How big zip files have to be before we split and send by themselves.
  int maxZipFileSize = 10 * 1024 * 1024;

  /**
   * Utility class to build the schema.
   * 
   * @author srmadscience / VoltDB
   *
   */
  public VoltDBSchemaBuilder(String[] ddlStatements, String[] procStatements, String[] zipFiles, String jarFileName,
      Client voltClient, String procPackageName, String testProcName, Object[] testParams, String[] otherClasses ) {
    super();
    this.ddlStatements = ddlStatements;
    this.procStatements = procStatements;
    this.voltClient = voltClient;
    this.procPackageName = procPackageName;
    this.zipFiles = zipFiles;
    this.jarFileName = jarFileName;
    this.testProcName = testProcName;
    this.testParams = testParams;
    this.otherClasses = otherClasses;
    
    jarFiles = makeJarFiles(procStatements);
  }

  /**
   * See if we think Schema already exists...
   * 
   * @return true if the 'Get' procedure exists and takes one string as a
   *         parameter.
   */
  public boolean schemaExists() {

    boolean schemaExists = false;

    try {
      ClientResponse response = voltClient.callProcedure(testProcName, testParams);

      if (response.getStatus() == ClientResponse.SUCCESS) {
        // Database exists...
        schemaExists = true;
      } else {
        // If we'd connected to a copy of VoltDB without the schema and tried to
        // call Get
        // we'd have got a ProcCallException
        logger.error("Error while calling schemaExists(): " + response.getStatusString());
        schemaExists = false;
      }
    } catch (ProcCallException pce) {
      schemaExists = false;

      // Sanity check: Make sure we've got the *right* ProcCallException...
      if (!pce.getMessage().equals(PROCEDURE + testProcName + WAS_NOT_FOUND)) {
        logger.error("Got unexpected Exception while calling schemaExists()", pce);
      }

    } catch (Exception e) {
      logger.error("Error while creating classes.", e);
      schemaExists = false;
    }

    return schemaExists;
  }

  /**
   * Load classes and DDL required by YCSB.
   * 
   * @throws Exception
   */
  public synchronized boolean loadClassesAndDDLIfNeeded() throws Exception {

    if (schemaExists()) {
      return true;
    }

    File tempDir = Files.createTempDirectory("voltdbSchema").toFile();

    if (!tempDir.canWrite()) {
      throw new Exception(
          "Temp Directory (from Files.createTempDirectory()) '" + tempDir.getAbsolutePath() + "' is not writable");
    }

    //
    // Step 1: Send big zip files ahead.
    //
    if (zipFiles != null) {
      for (int i = 0; i < zipFiles.length; i++) {

        if (isBigZipFile(zipFiles[i])) {

          ArrayList<byte[]> zipParts = split(zipFiles[i]);

          for (int j = 0; j < zipParts.size(); j++) {

            String zipfilename = zipFiles[i] + "." + j;
            String zipJARfilename = zipfilename + ".jar";
            ByteArrayInputStream is = new ByteArrayInputStream(zipParts.get(j));

            logger.info("Creating JAR file for ZIP part " + j + " in " + tempDir + File.separator + zipJARfilename);

            Manifest manifest = new Manifest();
            manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
            JarOutputStream newJarFileOutputStream = new JarOutputStream(
                new FileOutputStream(tempDir + File.separator + zipJARfilename), manifest);

            add(procPackageName.replace(".", "/") + "/" + zipfilename, is, newJarFileOutputStream);

            newJarFileOutputStream.close();
            File file = new File(tempDir + File.separator + zipJARfilename);

            byte[] jarFileContents = new byte[(int) file.length()];
            FileInputStream fis = new FileInputStream(file);
            fis.read(jarFileContents);
            fis.close();
            logger.info("Calling @UpdateClasses to load JAR file containing zip file " + zipFiles[i] + " part " + j);

            ClientResponse cr;
            cr = voltClient.callProcedure("@UpdateClasses", jarFileContents, null);
            if (cr.getStatus() != ClientResponse.SUCCESS) {
              throw new Exception("Attempt to execute UpdateClasses failed:" + cr.getStatusString());
            }

          }

        }
      }
    }
    
    //
    // Step 2: Create tables etc
    //
    ClientResponse cr;

    for (int i = 0; i < ddlStatements.length; i++) {
      try {
        cr = voltClient.callProcedure("@AdHoc", ddlStatements[i]);
        if (cr.getStatus() != ClientResponse.SUCCESS) {
          throw new Exception("Attempt to execute '" + ddlStatements[i] + "' failed:" + cr.getStatusString());
        }
        logger.info(ddlStatements[i]);
      } catch (Exception e) {

        if (e.getMessage().indexOf("object name already exists") > -1) {
          // Someone else has done this...
          return false;
        }

        throw (e);
      }
    }

    //
    // Step 3: Load other zip files and classes into a JAR file and send.
    //
    logger.info("Creating JAR file in " + tempDir + File.separator + jarFileName);
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    JarOutputStream newJarFileOutputStream = new JarOutputStream(
        new FileOutputStream(tempDir + File.separator + jarFileName), manifest);

    
    
    for (int i = 0; i < otherClasses.length; i++) {
      InputStream is = getClass()
          .getResourceAsStream("/" + otherClasses[i].replace(".", "/") + ".class");
      logger.info("processing " + otherClasses[i].replace(".", "/"));
      add(otherClasses[i].replace(".", "/") + ".class", is, newJarFileOutputStream);
    }
    
    
   
    
    for (int i = 0; i < jarFiles.length; i++) {
      InputStream is = getClass()
          .getResourceAsStream("/" + procPackageName.replace(".", "/") + "/" + jarFiles[i] + ".class");
      logger.info("processing " + procPackageName.replace(".", "/") + "/" + jarFiles[i]);
      add(procPackageName.replace(".", "/") + "/" + jarFiles[i] + ".class", is, newJarFileOutputStream);
    }
    
    

    if (zipFiles != null) {
      for (int i = 0; i < zipFiles.length; i++) {
        if (!isBigZipFile(zipFiles[i])) {
          InputStream is = getClass().getResourceAsStream("/" + procPackageName.replace(".", "/") + "/" + zipFiles[i]);
          logger.info("processing " + procPackageName.replace(".", "/") + "/" + zipFiles[i]);
          add(procPackageName.replace(".", "/") + "/" + zipFiles[i], is, newJarFileOutputStream);
        }
      }
    }

    newJarFileOutputStream.close();
    File file = new File(tempDir + File.separator + jarFileName);

    byte[] jarFileContents = new byte[(int) file.length()];
    FileInputStream fis = new FileInputStream(file);
    fis.read(jarFileContents);
    fis.close();
    logger.info("Calling @UpdateClasses to load JAR file containing procedures");

    cr = voltClient.callProcedure("@UpdateClasses", jarFileContents, null);
    if (cr.getStatus() != ClientResponse.SUCCESS) {
      throw new Exception("Attempt to execute UpdateClasses failed:" + cr.getStatusString());
    }

    for (int i = 0; i < procStatements.length; i++) {
      logger.info(procStatements[i]);
      cr = voltClient.callProcedure("@AdHoc", procStatements[i]);
      if (cr.getStatus() != ClientResponse.SUCCESS) {
        throw new Exception("Attempt to execute '" + procStatements[i] + "' failed:" + cr.getStatusString());
      }
    }

    return schemaExists();

  }

  private ArrayList<byte[]> split(String zipfilename) throws Exception {

    ArrayList<byte[]> fileParts = new ArrayList<byte[]>();

    BufferedInputStream is = new BufferedInputStream(
        getClass().getResourceAsStream("/" + procPackageName.replace(".", "/") + "/" + zipfilename), maxZipFileSize);

    byte[] b = new byte[maxZipFileSize];

    int bytesRead = is.read(b);

    while (bytesRead > -1) {

      if (bytesRead < maxZipFileSize) {
        b = Arrays.copyOfRange(b, 0, bytesRead);
      }

      fileParts.add(b);
      b = new byte[maxZipFileSize];
      bytesRead = is.read(b);

    }

    is.close();

    return fileParts;
  }

  private boolean isBigZipFile(String zipfilename) throws Exception {

    boolean isBig = false;

    BufferedInputStream is = new BufferedInputStream(
        getClass().getResourceAsStream("/" + procPackageName.replace(".", "/") + "/" + zipfilename));

    byte[] b = new byte[maxZipFileSize];

    try {
      final int howManyRead = is.read(b, 0, maxZipFileSize);

      if (howManyRead >= maxZipFileSize) {
        isBig = true;
      }
    } catch (IOException e) {
      throw new Exception("Resource " + zipfilename + " not found in " + procPackageName );
    }

    is.close();

    return isBig;

  }

  /**
   * Method to take an array of "CREATE PROCEDURE" statements and return a list
   * of the class files they are talking about.
   * 
   * @param procStatements
   * @return a list of the class files they are talking about.
   */
  private String[] makeJarFiles(String[] procStatements) {

    ArrayList<String> jarFileAL = new ArrayList<String>();

    for (int i = 0; i < procStatements.length; i++) {

      String thisProcStringNoNewLines = procStatements[i].toUpperCase().replace(System.lineSeparator(), " ");

      if (thisProcStringNoNewLines.indexOf(" FROM CLASS ") > -1) {
        String[] thisProcStringAsWords = procStatements[i].replace(".", " ").split(" ");

        if (thisProcStringAsWords[thisProcStringAsWords.length - 1].endsWith(";")) {
          jarFileAL.add(thisProcStringAsWords[thisProcStringAsWords.length - 1].replace(";", ""));

        } else {
          logger.error("Parsing of '" + procStatements[i] + "' went wrong; can't find proc name");
        }

      }
    }

    String[] jarFileList = new String[jarFileAL.size()];
    jarFileList = jarFileAL.toArray(jarFileList);

    return jarFileList;
  }

  /**
   * Add an entry to our JAR file.
   * 
   * @param fileName
   * @param source
   * @param target
   * @throws IOException
   */
  private void add(String fileName, InputStream source, JarOutputStream target) throws IOException {
    BufferedInputStream in = null;
    try {

      JarEntry entry = new JarEntry(fileName.replace("\\", "/"));
      entry.setTime(System.currentTimeMillis());
      target.putNextEntry(entry);
      in = new BufferedInputStream(source);

      byte[] buffer = new byte[1024];
      while (true) {
        int count = in.read(buffer);
        if (count == -1) {
          break;
        }

        target.write(buffer, 0, count);
      }
      target.closeEntry();
    } finally {
      if (in != null) {
        in.close();
      }

    }
  }

  /**
   * @return the maxZipFileSize
   */
  public int getMaxZipFileSize() {
    return maxZipFileSize;
  }

  /**
   * @param maxZipFileSize
   *          the maxZipFileSize to set
   */
  public void setMaxZipFileSize(int maxZipFileSize) {
    this.maxZipFileSize = maxZipFileSize;
  }

}
