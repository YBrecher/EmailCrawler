package edu.touro.mco364;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

     private static final Set<String> linksToVisit = Collections.synchronizedSet(new HashSet<>());
     private static final Set<String> linksVisited = Collections.synchronizedSet(new HashSet<>());
     private static final Set<String> emailSet = Collections.synchronizedSet(new HashSet<>());

    public static void main(String[] args) throws IOException, InterruptedException, ConcurrentModificationException {

        ExecutorService executor = Executors.newFixedThreadPool(500);

        final int EMAIL_MAX_COUNT = 10000;

        executor.execute(new Scraper("https://www.touro.edu/")); //scrape first url before loop to build up link set for a smoother run
        Thread.sleep(1000);

        while (emailSet.size() <= EMAIL_MAX_COUNT) {

            synchronized (linksToVisit) {

                if (linksToVisit.iterator().hasNext()) {
                    String link = linksToVisit.iterator().next();

                    while (!isValidProtocol(link)) { // Check for valid protocol before continuing loop
                        linksToVisit.remove(link);
                        link = linksToVisit.iterator().next();
                    }

                    linksToVisit.remove(link);
                    linksVisited.add(link);

                    executor.execute(new Scraper(link));
                    Thread.sleep(50);

                    System.out.println(emailSet.size() + "      " + linksVisited.size() + "     " + link);

                }
            }
        }

        executor.shutdownNow();


        synchronized (emailSet) {
            for (String element : emailSet) {
                System.out.println(element);
            }

            insertToDB(emailSet);
        }
    }

    static class Scraper extends Thread{

        String hyperLink;
        Scraper(String hyperLink)
        {
            this.hyperLink = hyperLink;
        }

        @Override
        public void run() {

            Document doc = null;
            try {
                doc = Jsoup.connect(hyperLink).ignoreContentType(true).ignoreHttpErrors(true).get();
            } catch (IOException e) {
                e.printStackTrace();
            }

            List<String> urls = doc.select("a[href]").eachAttr("abs:href");
            Set<String> extractedLinks = new HashSet<>(urls);
            extractedLinks.removeAll(linksVisited);
            linksToVisit.addAll(extractedLinks);

            Pattern p = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+");
            Matcher matcher = p.matcher(doc.html());
            while (matcher.find()) {
                emailSet.add(matcher.group());
            }
        }


    }

    private static boolean isValidProtocol (String link) throws MalformedURLException {
        URL url = new URL(link);
        String protocol = url.getProtocol();
        return protocol.equals("http") | protocol.equals("https");
    }

        private static void insertToDB (Set<String> list) {
            String connectionUrl = "Enter URL Here";

            try (Connection con = DriverManager.getConnection(connectionUrl);
                 Statement stmt = con.createStatement()) {

                for (String element : list) {
                    String insertQuery = String.format("Insert Into Emails Values ('%s')", element);
                    stmt.executeUpdate(insertQuery);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
