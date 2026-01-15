package pt.uc.dei.wsvd.bench.tpcw.versions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import pt.uc.dei.wsvd.bench.Database;

/**
 * WS - Vulnerability Detection Tools Benchmark
 * TPC - C Benchmark Services
 * #WebServiceOperation
 *
 *
 * @author nmsa@dei.uc.pt
 */
public class GetPassword_VxA {

    public String getPassword(String C_UNAME) {
        String passwd = null;
        Connection con = Database.pickConnection();
        try {
            String sql = "SELECT c_passwd FROM tpcw_customer WHERE c_uname = ?";
            try (PreparedStatement ps = con.prepareStatement(sql)) {
                ps.setString(1, C_UNAME);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        passwd = rs.getString("c_passwd");
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
        return passwd;
    }
}
