package pt.uc.dei.wsvd.bench.tpcw.versions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import pt.uc.dei.wsvd.bench.Database;
import pt.uc.dei.wsvd.bench.tpcw.object.ShortBook;

/**
 * WS - Vulnerability Detection Tools Benchmark
 * TPC - C Benchmark Services
 * #WebServiceOperation
 *
 *
 * @author nmsa@dei.uc.pt
 */
public class GetBestSellers_VxA {

    public List<ShortBook> getBestSellers(String subject) {
        List<ShortBook> books = new ArrayList<ShortBook>();
        Connection con = Database.pickConnection();
        try {
            String sql = "SELECT i_id, i_title, a_fname, a_lname "
                    + "FROM tpcw_item, tpcw_author, tpcw_order_line "
                    + "WHERE tpcw_item.i_id = tpcw_order_line.ol_i_id "
                    + "AND tpcw_item.i_a_id = tpcw_author.a_id "
                    + "AND tpcw_order_line.ol_o_id > (SELECT MAX(o_id)-3333 FROM tpcw_orders) "
                    + "AND tpcw_item.i_subject = ? "
                    + "AND ROWNUM <= 50 "
                    + "GROUP BY i_id, i_title, a_fname, a_lname "
                    + "ORDER BY SUM(ol_qty) DESC";

            try (PreparedStatement ps = con.prepareStatement(sql)) {
                ps.setString(1, subject);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        books.add(new ShortBook(rs));
                    }
                }
                con.commit();
            }
        } catch (java.lang.Exception ex) {
            //ex.printStackTrace();
            try{
                con.rollback();
            } catch (Exception e) {
            }
        } finally {
            Database.relaseConnection(con);
        }
        return books;
    }
}
