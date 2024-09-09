package internacao.persistence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Conexao {

	public Connection getConnection() {
		
		// Desenvolvimento		
		//String url = "jdbc:postgresql://10.10.73.14:6433/hm0615_hspm"; // servidor antigo
		
		String url = "jdbc:postgresql://pg019dbsrv.prodam:5432/hm0615_hspm";
		String usuario = "hm0615_hspm";
		String senha = "pwd_hm0615_hspm";
		
		// Nova Base Produção //09/09/2024
		//String url = "jdbc:postgresql://pg023dbsrv.prodam:5432/hm0615_hspm";
		//String usuario = "hspm_acesso";
		//String senha = "pwd_hspm_acesso";
		
		// Homologação
		//String url = "jdbc:postgresql://pg017dbsrv.prodam:5432/hm0615_hspm";
		//String usuario = "hspm_acesso";
		//String senha = "pwd_hspm_acesso";

		
		// Old Produção
		//String url = "jdbc:postgresql://10.10.68.39:6432/hm0615_hspm";
		//String usuario = "hspm_acesso";
		//String senha = "pwd_hspm_acesso";

		Connection result = null;
		try {
			Class.forName("org.postgresql.Driver");
			result = DriverManager.getConnection(url, usuario, senha);
			return result;
		} catch (SQLException e) {
			System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}