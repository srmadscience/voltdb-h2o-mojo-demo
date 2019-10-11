package ie.voltdb.h2outil;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Vector;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import hex.genmodel.InMemoryMojoReaderBackend;
import hex.genmodel.MojoReaderBackend;
import hex.genmodel.utils.IOUtils;
import mojoprocs.IsFlightLate;

public class H2OMojoWrangler {

  public static MojoReaderBackend createInMemoryReaderBackendFromSetOfZipFiles(String modelZipFileName)
      throws IOException {

    // What we unload content into
    HashMap<String, byte[]> content = new HashMap<>();

    // A small zip file will be by itself; A lare one will be split into
    // smaller ones, using the naming convention foo.zip.0, foo.zip.1 etc
    // We create streams for our zip files...
    Vector<InputStream> zipStreams = new Vector<InputStream>();

    // Either we have one zip file or many. Assume one and see
    // what happens...

    URL mojoURL = IsFlightLate.class.getResource(modelZipFileName);
    InputStream inputStream = null;

    try {
      // Some oddness here. When developing on a desktop
      // mojoURL will be 'null' if the zip is missing. If
      // being run from inside a JAR mojoURL will not be null
      // but will throw an IO exception of the file is bad.
      //
      // So the exception block below will catch either an
      // NPE or an IOException...
      inputStream = mojoURL.openStream();
    } catch (Exception e) {
      inputStream = null;
    }

    if (inputStream != null) {

      zipStreams.add(inputStream);

    } else {
      // Assume we got an io exception because we have many
      // zip files instead of 1...
      int zipCounter = 0;
      boolean keepGoing = true;

      while (keepGoing) {

        try {

          URL mojoPartURL = IsFlightLate.class.getResource(modelZipFileName + "." + zipCounter++);

          if (mojoPartURL != null) {

            InputStream zipInputStream = mojoPartURL.openStream();
            zipStreams.add(zipInputStream);

          } else {

            keepGoing = false;

          }
        } catch (IOException e2) {
          // Same oddness as above...
          // mojoPartURL will be non-null but unusable...
          keepGoing = false;
        }

      }
    }

    // Sanity check...
    if (zipStreams.size() == 0) {
      throw new IOException("Zip file " + modelZipFileName + " doesn't exist whole or in fragments...");
    }

    SequenceInputStream sequenceInputStream = new SequenceInputStream(zipStreams.elements());

    ZipInputStream zis = new ZipInputStream(sequenceInputStream);

    try {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {

        if (entry.getSize() > Integer.MAX_VALUE)
          throw new IOException("File too large: " + entry.getName());
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        IOUtils.copyStream(zis, os);
        content.put(entry.getName(), os.toByteArray());
      }
      zis.close();
    } finally {
      closeQuietly(zis);
    }
    return new InMemoryMojoReaderBackend(content);
  }

  private static void closeQuietly(Closeable c) {
    if (c != null)
      try {
        c.close();
      } catch (IOException e) {
        // intentionally ignore exception
      }
  }
  
}
