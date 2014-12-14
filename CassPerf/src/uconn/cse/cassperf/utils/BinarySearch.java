/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.utils;

/**
 *
 * @author nhannguyen
 */
public class BinarySearch {

    public BinarySearch() {
    }
    /*
     public int search(double[] a, double key) {
     int lo = 0;
     int mid = 0;
     int hi = a.length - 1;
     int min = Integer.MAX_VALUE;
     while (lo <= hi) {
     // Key is in a[lo..hi] or not present.
     mid = lo + (hi - lo) / 2;
     if (key < a[mid]) {
     hi = mid - 1;
     } else if (key > a[mid]) {
     lo = mid + 1;
     } else {
     return mid;
     }
     }
     if (lo > a.length - 1) {
     lo = a.length - 1;
     }
     if (hi > a.length - 1) {
     hi = a.length - 1;
     }
     if (mid > a.length - 1) {
     mid = a.length - 1;
     }
     if (lo < 0) {
     lo = 0;
     }
     if (hi < 0) {
     hi = 0;
     }
     if (mid < 0) {
     mid = 0;
     }

     double n1 = Math.abs(key - a[lo]);
     double n2 = Math.abs(key - a[mid]);
     double n3 = Math.abs(key - a[hi]);
     if (n1 >= n2) {
     if (n1 >= n3) {
     if (n2 >= n3) {
     return hi;
     } else {
     return mid;
     }
     } else {
     return mid;
     }
     } else {
     if (n1 >= n3) {
     return hi;
     } else {
     return lo;
     }
     }

     }
     */

    public int search(double[] a, int[] idx, double key) {
        int lo = 0;
        int mid = 0;
        int hi = a.length - 1;
        int min = Integer.MAX_VALUE;
        while (lo <= hi) {
            // Key is in a[lo..hi] or not present.
            mid = lo + (hi - lo) / 2;
            if (key < a[idx[mid]]) {
                hi = mid - 1;
            } else if (key > a[idx[mid]]) {
                lo = mid + 1;
            } else {
                return mid;
            }
        }
        if (lo > a.length - 1) {
            lo = a.length - 1;
        }
        if (hi > a.length - 1) {
            hi = a.length - 1;
        }
        if (mid > a.length - 1) {
            mid = a.length - 1;
        }
        if (lo < 0) {
            lo = 0;
        }
        if (hi < 0) {
            hi = 0;
        }
        if (mid < 0) {
            mid = 0;
        }

        double n1 = Math.abs(key - a[idx[lo]]);
        double n2 = Math.abs(key - a[idx[mid]]);
        double n3 = Math.abs(key - a[idx[hi]]);
        if (n1 >= n2) {
            if (n1 >= n3) {
                if (n2 >= n3) {
                    return hi;
                } else {
                    return mid;
                }
            } else {
                return mid;
            }
        } else {
            if (n1 >= n3) {
                return hi;
            } else {
                return lo;
            }
        }

    }

    final public BinarySearch getInstance() {
        return new BinarySearch();
    }
}
