package com.elevenquest.test;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 * Hello world!
 *
 */
public class App 
{
    // https://www.postgresql.org/message-id/1171970019.3101.328.camel%40coppola.muc.ecircle.de
    // https://stackoverflow.com/questions/1347646/postgres-error-on-insert-error-invalid-byte-sequence-for-encoding-utf8-0x0/45810272
    static class RemoveNullCharacterReader extends FileReader
    {
        static int DEFAULT_BUFFER_SIZE = 65536;

        public RemoveNullCharacterReader(File file) throws FileNotFoundException {
            super(file);
        }

        public RemoveNullCharacterReader(String filepath) throws FileNotFoundException {
            super(filepath);
        }

        char[] orgbuffer = new char[DEFAULT_BUFFER_SIZE];
        static long sentbytes = 0;
        static long printcnt = 0;
        @Override
        public int read(char[] cbuf) throws IOException {
            int rtn = super.read(orgbuffer);
            if(rtn > 0) {
                String verifiedstr = new String(orgbuffer, 0, rtn).replaceAll("\u0000", "");
                char[] verifiedarr = verifiedstr.toCharArray();
                System.arraycopy(verifiedarr, 0, cbuf, 0, verifiedarr.length);
                sentbytes += verifiedarr.length;
                if(sentbytes > printcnt * 10000000) {
                    System.out.println("sent bytes:" + sentbytes);
                    printcnt++;
                }
                return verifiedarr.length;
            } else {
                return rtn;
            }
        }
    }
    public static void main( String[] args ) throws Exception
    {
        if(args.length!=4) {
            System.out.println("Please specify database URL, user, password and file on the command line.");
            System.out.println("Like this: jdbc:postgresql://localhost:5432/test test password file");
        } else {
            System.err.println("Loading driver");
            Class.forName("org.postgresql.Driver");
            System.err.println("Connecting to " + args[0]);
            Connection con = DriverManager.getConnection(args[0],args[1],args[2]);

            System.err.println("Copying text data rows from stdin");
            long startime = System.currentTimeMillis();
            CopyManager copyManager = new CopyManager((BaseConnection) con);
            FileReader fileReader = new RemoveNullCharacterReader(args[3]);
            copyManager.copyIn("COPY tb_part_dummy_tsv FROM STDIN", fileReader );
            long finishtime = System.currentTimeMillis();
            long elapsedtime = finishtime - startime;
            System.out.println("elapsed time:" + (elapsedtime / 1000));
            System.err.println("Done.");
            // con.commit();
        }
    }
}
