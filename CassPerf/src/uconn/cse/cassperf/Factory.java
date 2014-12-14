/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf;

/**
 *
 * @author nhannguyen
 */
public class Factory {

    public static int THRIFT_PORT = 9696;
    public int nodeId = 1;

    public Factory() {
        nodeId = 1;
    }

    private static class FactoryHolder {

        public static final Factory INSTANCE = new Factory();
    }

    public static Factory getInstance() {
        return FactoryHolder.INSTANCE;
    }
}
