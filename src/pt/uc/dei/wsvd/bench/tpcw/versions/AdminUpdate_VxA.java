package pt.uc.dei.wsvd.bench.tpcw.versions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import pt.uc.dei.wsvd.bench.Database;

/**
 * WS - Vulnerability Detection Tools Benchmark
 * TPC - C Benchmark Services
 * #WebServiceOperation
 *
 *
 * @author nmsa@dei.uc.pt
 */
public class AdminUpdate_VxA {

    public void adminUpdate(int i_id, double cost, String image, String thumbnail) {
        Connection con = Database.pickConnection();
        try {
            // First UPDATE
            String sqlUpdate1 = "UPDATE tpcw_item SET i_cost = ?, i_image = ?, i_thumbnail = ?, i_pub_date = sysdate WHERE i_id = ?";
            try (PreparedStatement ps = con.prepareStatement(sqlUpdate1)) {
                ps.setDouble(1, cost);
                ps.setString(2, image);
                ps.setString(3, thumbnail);
                ps.setInt(4, i_id);
                ps.executeUpdate();
            }

            // SELECT
            String sqlSelect = "SELECT ol_i_id "
                    + " FROM tpcw_orders, tpcw_order_line "
                    + "WHERE tpcw_orders.o_id = tpcw_order_line.ol_o_id "
                    + "  AND NOT (tpcw_order_line.ol_i_id = ?) "
                    + "  AND tpcw_orders.o_c_id IN (SELECT o_c_id "
                    + "                      FROM tpcw_orders, tpcw_order_line "
                    + "                      WHERE tpcw_orders.o_id = tpcw_order_line.ol_o_id "
                    + "                      AND tpcw_orders.o_id > (SELECT MAX(o_id) - 10000 FROM tpcw_orders)"
                    + "                      AND tpcw_order_line.ol_i_id = ?) "
                    + "  AND ROWNUM < 5 "
                    + "GROUP BY ol_i_id "
                    + "ORDER BY SUM(ol_qty) DESC ";

            int[] related_items = new int[5];
            int counter = 0;
            int last = 0;

            try (PreparedStatement ps = con.prepareStatement(sqlSelect)) {
                ps.setInt(1, i_id);
                ps.setInt(2, i_id);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        last = rs.getInt(1);
                        related_items[counter] = last;
                        counter++;
                    }
                }
            }

            // This is the case for the situation where there are not 5 related books.
            for (int i = counter; i < 5; i++) {
                last++;
                related_items[i] = last;
            }

            // Second UPDATE
            String sqlUpdate2 = "UPDATE tpcw_item SET i_related1 = ?, i_related2 = ?, i_related3 = ?, i_related4 = ?, i_related5 = ? WHERE i_id = ?";
            try (PreparedStatement ps = con.prepareStatement(sqlUpdate2)) {
                ps.setInt(1, related_items[0]);
                ps.setInt(2, related_items[1]);
                ps.setInt(3, related_items[2]);
                ps.setInt(4, related_items[3]);
                ps.setInt(5, related_items[4]);
                ps.setInt(6, i_id);
                ps.executeUpdate();
            }

            con.commit();
        } catch (java.lang.Exception ex) {
            //ex.printStackTrace();
            try {
                con.rollback();
            } catch (Exception e) {
            }
        } finally {
            Database.relaseConnection(con);
        }
    }
}
