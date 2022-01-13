package com.elevenquest.athena.jdbctest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.HashMap;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello World!");
        App app = new App();
        app.printMetric();
        // app.testCase1_ConnectionOne();
        // app.testCase4_IncreaseConnectionAndMultiTransactionTo15Per5SecAndCloseAll();
        app.testCase5_Concurrently15ThreadWorksMultiTxForAlmost60Sec();
        Thread.sleep(300000);
        app.needExit = true;
    }

    public void testCase1_ConnectionOne() {
        Connection con = null;
        try {
            con = getConnectionFromDS();
            System.out.println("Success Connection.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (con != null)
                try { con.close(); } catch (Exception e) {e.printStackTrace();}
        }
    }

    public void testCase2_IncreaseConnectionsTo15Per5SecAndCloseAll() {
        java.util.ArrayList<Connection> cons = new java.util.ArrayList<Connection>();
        try {
            for (int cnt=0; cnt < 15; cnt++) {
                cons.add(getConnectionFromDS());
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (cons != null & cons.size() > 0) {
                for(Connection con : cons) {
                    try { con.close(); } catch(Exception e1) {e1.printStackTrace();}
                }
            }
        }
    }

    public void testCase3_IncreaseConnectionAndTransactionTo15Per5SecAndCloseAll() {
        java.util.ArrayList<Connection> cons = new java.util.ArrayList<Connection>();
        try {
            for (int cnt=0; cnt < 15; cnt++) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        PreparedStatement stmt = null;
                        ResultSet rs = null;
                        boolean isSuccessful = false;
                        try {
                            Connection con = getConnectionFromDS();
                            cons.add(con);
                            stmt = con.prepareStatement("select * from sampledb.tb_part_dummy limit 1000");
                            rs = stmt.executeQuery();
                            while(rs.next()) {
                                isSuccessful = true;
                            }
                            if (!isSuccessful)
                                System.out.println("Failure.");
                        } catch(Exception e) {
                            e.printStackTrace();
                        } finally {
                            if (stmt != null) {
                                try { stmt.close(); } catch(Exception e) { e.printStackTrace(); }
                            }
                            if (rs != null) {
                                try { rs.close(); } catch(Exception e) {e.printStackTrace();}
                            }
                        }
                    }
                }).start();
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (cons != null & cons.size() > 0) {
                for(Connection con : cons) {
                    try { con.close(); } catch(Exception e1) {e1.printStackTrace();}
                }
            }
        }
    }

    public void testCase4_IncreaseConnectionAndMultiTransactionTo15Per5SecAndCloseAll() {
        java.util.ArrayList<Connection> cons = new java.util.ArrayList<Connection>();
        try {
            for (int cnt=0; cnt < 15; cnt++) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Connection con = getConnectionFromDS();
                            cons.add(con);
                            for (int cnt = 0; cnt < 5; cnt++) {
                                PreparedStatement stmt = null;
                                ResultSet rs = null;
                                boolean isSuccessful = false;
                                try {
                                    stmt = con.prepareStatement("select * from sampledb.tb_part_dummy limit 1000");
                                    rs = stmt.executeQuery();
                                    while(rs.next()) {
                                        isSuccessful = true;
                                    }
                                    if (!isSuccessful)
                                        System.out.println("Failure.");
                                } catch(Exception e) {
                                    e.printStackTrace();
                                } finally {
                                    if (stmt != null) {
                                        try { stmt.close(); } catch(Exception e) { e.printStackTrace(); }
                                    }
                                    if (rs != null) {
                                        try { rs.close(); } catch(Exception e) {e.printStackTrace();}
                                    }
                                }
                            }
                        } catch (Exception oe) {
                            oe.printStackTrace();
                        }
                    }
                }).start();
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (cons != null & cons.size() > 0) {
                for(Connection con : cons) {
                    try { con.close(); } catch(Exception e1) {e1.printStackTrace();}
                }
            }
        }
    }

    public void testCase5_Concurrently15ThreadWorksMultiTxForAlmost60Sec() {
        try {
            for (int cnt=0; cnt < 15; cnt++) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        Connection con = null;
                        try {
                            con = getConnectionFromDS();
                            int repetitionCnt = (int)(Math.random() * 10) + 5;
                            for (int cnt = 0; cnt < repetitionCnt; cnt++) {
                                PreparedStatement stmt = null;
                                ResultSet rs = null;
                                boolean isSuccessful = false;
                                try {
                                    int limitSize = (int)(Math.random() * 10000);
                                    stmt = con.prepareStatement("select * from sampledb.tb_part_dummy limit " + limitSize);
                                    rs = stmt.executeQuery();
                                    while(rs.next()) {
                                        isSuccessful = true;
                                    }
                                    if (!isSuccessful)
                                        System.out.println("Failure.");
                                } catch(Exception e) {
                                    e.printStackTrace();
                                } finally {
                                    if (stmt != null) {
                                        try { stmt.close(); } catch(Exception e) { e.printStackTrace(); }
                                    }
                                    if (rs != null) {
                                        try { rs.close(); } catch(Exception e) {e.printStackTrace();}
                                    }
                                }
                            }
                        } catch (Exception oe) {
                            oe.printStackTrace();
                        } finally {
                            if (con != null) {
                                try { con.close(); } catch (Exception e) { e.printStackTrace(); }
                            }
                        }
                    }
                }).start();
                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    boolean needExit = false;
    HikariDataSource ds = null;

    public App() {
        ds = DataSourceConfiguration.getDataSource();
    }

    public Connection getConnectionFromDS() throws SQLException {
        return ds.getConnection();
    }

    public void printMetric() throws MalformedObjectNameException {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName poolName = new ObjectName("com.zaxxer.hikari:type=Pool (athenads)");
        final HikariPoolMXBean poolProxy = JMX.newMXBeanProxy(mBeanServer, poolName, HikariPoolMXBean.class);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    long startTime = System.currentTimeMillis();
                    HashMap<String, Integer> processmap = new HashMap<String, Integer>();
                    while (!needExit) {
                        Thread.sleep(1000);
                        BufferedReader isr = null;
                        int closewait = 0;
                        int est = 0;
                        int timewait = 0;
                        String mainprocess = "";
                        try {
                            Process netstats = new ProcessBuilder("/bin/sh", "-c", "netstat -anp | grep tcp6 | grep 443").start();
                            isr = new BufferedReader(new InputStreamReader(netstats.getInputStream()));
                            String aline = null;
                            while((aline = isr.readLine()) != null) {
                                StringTokenizer st = new StringTokenizer(aline);
                                String process = "";
                                while(st.hasMoreTokens())
                                    process = st.nextToken();
                                Integer curcount;
                                if ((curcount = processmap.get(process)) != null) {
                                    mainprocess = process;
                                    processmap.put(process, curcount+1);
                                } else {
                                    processmap.put(process, 1);
                                }
                                if (process.equals(mainprocess)) {
                                    if(aline.indexOf("CLOSE_WAIT") > 0)
                                    closewait++;
                                    if(aline.indexOf("ESTABLISHED") > 0)
                                        est++;
                                    if(aline.indexOf("TIME_WAIT") > 0)
                                        timewait++;
                                }
                            }
                        } finally {
                            if (isr != null) try {isr.close();}catch(Exception e) {e.printStackTrace();}
                        }
                        long elapsedTime = (System.currentTimeMillis() - startTime);
                        System.out.println(MessageFormat.format("Elapsed Time: {0}, Active Count: {1}, Idle Count: {2}, ThreadAwating Count: {3}, Total Count: {4}, HTTP CLOSE_WAIT: {5}, HTTP EST: {6}, HTTP TIME_WAIT: {7} ",
                            elapsedTime, 
                            poolProxy.getActiveConnections(), poolProxy.getIdleConnections(), 
                            poolProxy.getThreadsAwaitingConnection(), poolProxy.getTotalConnections(),
                            closewait, est, timewait
                            ));
                    }
            } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public Connection getConnection() throws SQLException, ClassNotFoundException {
        Properties info = new Properties();
        info.put("AwsCredentialsProviderClass", "com.simba.athena.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        info.put("S3OutputLocation", "s3://my-athena-resultbucket/test/");
        Class.forName("com.simba.athena.jdbc.Driver");
        return DriverManager.getConnection("jdbc:awsathena://AwsRegion=us-east-1;", info);
    }

}
