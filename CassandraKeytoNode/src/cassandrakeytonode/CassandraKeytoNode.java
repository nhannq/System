/*----------------------------------------------------------------
 *  Author:        Nhan Nguyen
 *  Written:       02/03/2013
 *  Last updated:  05/20/2014
 *
 * CasssandraKeytoNode:
 *  - Calculate initial tokens for a node in Cassandra cluster
 *  - Get the token range of keys from Cassandra
 *   
 *  
 *----------------------------------------------------------------*/

package cassandrakeytonode;

import com.datastax.driver.core.*;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CassandraKeytoNode {

    /**
     * @param args the command line arguments
     */
    private Cluster cluster;
    private Session session;

    public void connect(String node) {
        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
    }

    public void getHashValues() {
        FileWriter fw = null;
        try {
            session = cluster.connect("mykeyspace"); //connect to Cassandra server
            
            Properties properties = new Properties();            
            try {
                properties.load(new FileInputStream("cassandrakeytonode.properties"));
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
//            System.out.println(properties.getProperty("nbKeys", "20000"));
            int nbKeys = Integer.parseInt(properties.getProperty("nbKeys", "20000"));
            int nbNodes = Integer.parseInt(properties.getProperty("nbNodes", "6"));
            System.out.println(nbNodes);
            for (int i = 0; i < nbKeys; i++) {
                String query = "insert into t2 (k, other) VALUES (" + i + ", 1);"; //put nbKeys keys to Cassandra
                session.execute(query);
            }
            
            List<Long> initialTokens = new ArrayList<Long>();
            for (int i = 0; i< nbNodes; i++) {
            	initialTokens.add((long)((Math.pow(2,64)/nbNodes)*i - Math.pow(2,63)));
            }
            for (int i = 0; i < nbNodes; i++) {
            	System.out.println(initialTokens.get(i));
            }
            
            ResultSet rS = session.execute("select token(k), k from t2;");
            List<List<Integer>> nodes = new ArrayList<List<Integer>>();
            for (int i = 0; i < nbNodes; i++) {
            	nodes.add(new ArrayList<Integer>());
            }

            List<Row> lSt = rS.all();
            
            //get keys from Cassandra and check which token range they belong to 
            for (int i = 0; i < nbKeys; i++) {
                Row a = lSt.get(i);
    //            System.out.print(a.getInt("k") + " : ");
    //            System.out.println(a.getLong("token(k)"));
                Long hashedValue = a.getLong("token(k)");
                for (int j = 0; j < nbNodes-1; j++) {
                	if ((hashedValue > initialTokens.get(j)) && (hashedValue <= initialTokens.get(j+1))) {
                        nodes.get(j+1).add(a.getInt("k"));
                        break;
                    }
                }
                if ((hashedValue > initialTokens.get(nbNodes-1)) || (hashedValue <= initialTokens.get(0))) {
                    nodes.get(0).add(a.getInt("k"));
                }                
    //        a.getVarint(a.getInt(0));
    //        a.getInt("token(k)");
            }
            
            System.out.println(nodes.get(0).size());
            System.out.println(nodes.get(1).size());
            System.out.println(nodes.get(2).size());
            fw = new FileWriter("ids611.txt");
            System.out.println("Node 0-1");
            for (int i = 0; i < 199; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(199));
            fw.close();
            System.out.println("Node 0-2");
            fw = new FileWriter("ids612.txt");
            for (int i = 200; i < 399; i++) {
               fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(399));
            fw.close();
            System.out.println("Node 0-3");
            fw = new FileWriter("ids613.txt");
            for (int i = 400; i < 599; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(599));
            fw.close();
            System.out.println("Node 0-4");
            fw = new FileWriter("ids614.txt");
            for (int i = 600; i < 799; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(799));
            fw.close();
            System.out.println("Node 0-5");
            fw = new FileWriter("ids615.txt");
            for (int i = 800; i < 999; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(999));
            fw.close();
            fw = new FileWriter("ids616.txt");
            System.out.println("Node 0-6");
            for (int i = 1000; i < 1199; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(1199));
            fw.close();
            System.out.println("Node 0-7");
            fw = new FileWriter("ids617.txt");
            for (int i = 1200; i < 1399; i++) {
               fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(1399));
            fw.close();
            System.out.println("Node 0-8");
            fw = new FileWriter("ids618.txt");
            for (int i = 1400; i < 1599; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(1599));
            fw.close();
            System.out.println("Node 0-9");
            fw = new FileWriter("ids619.txt");
            for (int i = 1600; i < 1799; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(1799));
            fw.close();
            System.out.println("Node 0-10");
            fw = new FileWriter("ids6110.txt");
            for (int i = 1800; i < 1999; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(1999));
            fw.close();
            System.out.println("Node 0-11");
            fw = new FileWriter("ids6111.txt");
            for (int i = 2000; i < 2199; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(2199));
            fw.close();            
            
            System.out.println("Node 1-1");
            fw = new FileWriter("ids621.txt");
            for (int i = 0; i < 199; i++) {
                fw.write(nodes.get(1).get(i) + ",");
            }
            fw.write(nodes.get(1).get(199));
            fw.close();
            System.out.println("Node 1-2");
            fw = new FileWriter("ids622.txt");
            for (int i = 200; i < 399; i++) {
                fw.write(nodes.get(1).get(i) + ",");
            }
            fw.write(nodes.get(1).get(399));
            fw.close();
            System.out.println("Node 1-3");
            fw = new FileWriter("ids623.txt");
            for (int i = 400; i < 599; i++) {
                fw.write(nodes.get(1).get(i) + ",");
            }
            fw.write(nodes.get(1).get(599));
            fw.close();
            System.out.println("Node 1-4");
            fw = new FileWriter("ids624.txt");
            for (int i = 600; i < 799; i++) {
                fw.write(nodes.get(1).get(i) + ",");
            }
            fw.write(nodes.get(1).get(899));
            fw.close();
            System.out.println("Node 1-5");
            fw = new FileWriter("ids625.txt");
            for (int i = 800; i < 999; i++) {
                fw.write(nodes.get(1).get(i) + ",");
            }
            fw.write(nodes.get(1).get(999));
            fw.close();
            
            System.out.println("Node 2-1");
            fw = new FileWriter("ids631.txt");
            for (int i = 0; i < 199; i++) {
                fw.write(nodes.get(2).get(i) + ",");
            }
            fw.write(nodes.get(2).get(199));
            fw.close();
            System.out.println("Node 2-2");
            fw = new FileWriter("ids632.txt");
            for (int i = 200; i < 399; i++) {
                fw.write(nodes.get(2).get(i) + ",");
            }
            fw.write(nodes.get(2).get(399));
            fw.close();
            System.out.println("Node 2-3");
            fw = new FileWriter("ids633.txt");
            for (int i = 400; i < 599; i++) {
                fw.write(nodes.get(2).get(i) + ",");
            }
            fw.write(nodes.get(2).get(599));
            fw.close();
            System.out.println("Node 2-4");
            fw = new FileWriter("ids634.txt");
            for (int i = 600; i < 799; i++) {
                fw.write(nodes.get(2).get(i) + ",");
            }
            fw.write(nodes.get(2).get(899));
            fw.close();
            System.out.println("Node 2-5");
            fw = new FileWriter("ids635.txt");
            for (int i = 800; i < 999; i++) {
                fw.write(nodes.get(2).get(i) + ",");
            }
            fw.write(nodes.get(2).get(999));
            fw.close();
            
            System.out.println("Node 3-1");
            fw = new FileWriter("ids641.txt");
            for (int i = 0; i < 199; i++) {
                fw.write(nodes.get(3).get(i) + ",");
            }
            fw.write(nodes.get(3).get(199));
            fw.close();
            System.out.println("Node 3-2");
            fw = new FileWriter("ids642.txt");
            for (int i = 200; i < 399; i++) {
                fw.write(nodes.get(3).get(i) + ",");
            }
            fw.write(nodes.get(3).get(399));
            fw.close();
            System.out.println("Node 3-3");
            fw = new FileWriter("ids643.txt");
            for (int i = 400; i < 599; i++) {
                fw.write(nodes.get(3).get(i) + ",");
            }
            fw.write(nodes.get(3).get(599));
            fw.close();
            System.out.println("Node 3-4");
            fw = new FileWriter("ids644.txt");
            for (int i = 600; i < 799; i++) {
                fw.write(nodes.get(3).get(i) + ",");
            }
            fw.write(nodes.get(3).get(899));
            fw.close();
            System.out.println("Node 3-5");
            fw = new FileWriter("ids645.txt");
            for (int i = 800; i < 999; i++) {
                fw.write(nodes.get(3).get(i) + ",");
            }
            fw.write(nodes.get(3).get(999));
            fw.close();
            
            System.out.println("Node 4-1");
            fw = new FileWriter("ids651.txt");
            for (int i = 0; i < 199; i++) {
                fw.write(nodes.get(4).get(i) + ",");
            }
            fw.write(nodes.get(4).get(199));
            fw.close();
            System.out.println("Node 4-2");
            fw = new FileWriter("ids652.txt");
            for (int i = 200; i < 399; i++) {
                fw.write(nodes.get(4).get(i) + ",");
            }
            fw.write(nodes.get(4).get(399));
            fw.close();
            System.out.println("Node 4-3");
            fw = new FileWriter("ids653.txt");
            for (int i = 400; i < 599; i++) {
                fw.write(nodes.get(4).get(i) + ",");
            }
            fw.write(nodes.get(4).get(599));
            fw.close();
            System.out.println("Node 4-4");
            fw = new FileWriter("ids654.txt");
            for (int i = 600; i < 799; i++) {
                fw.write(nodes.get(4).get(i) + ",");
            }
            fw.write(nodes.get(4).get(899));
            fw.close();
            System.out.println("Node 4-5");
            fw = new FileWriter("ids655.txt");
            for (int i = 800; i < 999; i++) {
                fw.write(nodes.get(4).get(i) + ",");
            }
            fw.write(nodes.get(4).get(999));
            fw.close();
            
            System.out.println("Node 5-1");
            fw = new FileWriter("ids661.txt");
            for (int i = 0; i < 199; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(199));
            fw.close();
            System.out.println("Node 5-2");
            fw = new FileWriter("ids662.txt");
            for (int i = 200; i < 399; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(399));
            fw.close();
            System.out.println("Node 5-3");
            fw = new FileWriter("ids663.txt");
            for (int i = 400; i < 599; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(599));
            fw.close();
            System.out.println("Node 5-4");
            fw = new FileWriter("ids664.txt");
            for (int i = 600; i < 799; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(899));
            fw.close();
            System.out.println("Node 5-5");
            fw = new FileWriter("ids665.txt");
            for (int i = 800; i < 999; i++) {
                fw.write(nodes.get(0).get(i) + ",");
            }
            fw.write(nodes.get(0).get(999));
            fw.close();
            System.out.println("Done");
        } catch (IOException ex) {
            Logger.getLogger(CassandraKeytoNode.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        } finally {
            try {
                fw.close();
            } catch (IOException ex) {
                Logger.getLogger(CassandraKeytoNode.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static void main(String[] args) {
        // TODO code application logic here
        CassandraKeytoNode cassandraKey2Node = new CassandraKeytoNode();
        cassandraKey2Node.connect("localhost");
        cassandraKey2Node.getHashValues();
    }
}
