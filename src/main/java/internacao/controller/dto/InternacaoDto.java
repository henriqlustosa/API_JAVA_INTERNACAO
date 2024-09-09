package  internacao.controller.dto;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import internacao.model.Extrato;
import  internacao.model.Internacao;
import  internacao.persistence.Conexao;

public class InternacaoDto {

	public static List<Internacao> Internacoes(int tipo, String dt_inicio, String dt_fim) {
		
		
		List<Internacao> internacoes = new ArrayList<Internacao>();
		PreparedStatement preparedStatement;

				//  WHERE dt_internacao::date between '2019-08-21' AND '2019-08-30' ORDER BY dt_internacao;
		
		//String sqlString = "SELECT * FROM agh.v_internacao "; 
		
		String sqlString = "SELECT DISTINCT  I.cd_prontuario, I.nm_paciente, I.in_sexo, I.nr_idade, I.nr_quarto, I.nr_leito, nm_ala, nm_clinica, "+
				"nm_unidade_funcional, nm_acomodacao, st_leito, dt_internacao, dt_entrada_setor, nm_especialidade, " +
				"nm_medico, dt_ultimo_evento, nm_origem, sg_cid, tx_observacao, nr_convenio, nr_plano, nm_convenio_plano, "+
				"nr_crm_profissional, nm_carater_internacao, nm_origem_internacao, nr_procedimento, dt_alta_medica, dt_saida_paciente, "+
				"dc_tipo_alta_medica, p.nm_vinculo , p.nm_orgao "+
				                "FROM agh.v_internacao as I inner join agh.v_paciente as P on I.cd_prontuario = P.cd_prontuario ";
		
		if(tipo==1) {
			
			sqlString += "WHERE dt_saida_paciente::date between '"+ dt_inicio +"' AND '"+dt_fim+"' ORDER BY dt_saida_paciente";
		}else if (tipo==2) {
			sqlString +=  "WHERE dt_internacao::date between '"+ dt_inicio +"' AND '"+dt_fim+"' ORDER BY dt_internacao";
		}
		
		//String sqlString = "SELECT * FROM agh.v_internacao " +
		//					" WHERE dt_internacao::date = '"+ data +"'";
		
							//" WHERE dt_internacao::date between '2019-08-21' AND '2019-08-21' ORDER BY dt_internacao"; // where date_part('year', dt_alta_medica) = 2019 AND date_part('month', dt_alta_medica) = 7;";

		try {

			Connection conn = new Conexao().getConnection();
			preparedStatement = conn.prepareStatement(sqlString);

			ResultSet resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {

				Internacao internacao = new Internacao();

				internacao.setCd_prontuario(resultSet.getLong("cd_prontuario"));
				internacao.setNm_paciente(resultSet.getString("nm_paciente"));
				internacao.setIn_sexo(resultSet.getString("in_sexo"));
				internacao.setNr_idade(resultSet.getInt("nr_idade"));
				internacao.setNr_quarto(resultSet.getString("nr_quarto"));
				internacao.setNr_leito(resultSet.getString("nr_leito"));
				internacao.setNm_ala(resultSet.getString("nm_ala"));
				internacao.setNm_clinica(resultSet.getString("nm_clinica"));
				internacao.setNm_unidade_funcional(resultSet.getString("nm_unidade_funcional"));
				internacao.setNm_acomodacao(resultSet.getString("nm_acomodacao"));
				internacao.setSt_leito(resultSet.getString("st_leito"));

				DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm");
				String dt_internacao = dateFormat.format(resultSet.getTimestamp("dt_internacao"));

				internacao.setDt_internacao(dt_internacao);

				String dt_entrada_setor = dateFormat.format(resultSet.getTimestamp("dt_entrada_setor"));
				internacao.setDt_entrada_setor(dt_entrada_setor);
				internacao.setNm_especialidade(resultSet.getString("nm_especialidade"));
				internacao.setNm_medico(resultSet.getString("nm_medico"));

				String dt_ultimo_evento = dateFormat.format(resultSet.getTimestamp("dt_ultimo_evento"));
				internacao.setDt_ultimo_evento(dt_ultimo_evento);
				internacao.setNm_origem(resultSet.getString("nm_origem"));
				internacao.setSg_cid(resultSet.getString("sg_cid"));
				internacao.setTx_observacao(resultSet.getString("tx_observacao"));
				internacao.setNr_convenio(resultSet.getInt("nr_convenio"));
				internacao.setNr_plano(resultSet.getInt("nr_plano"));
				internacao.setNm_convenio_plano(resultSet.getString("nm_convenio_plano"));
				internacao.setNr_crm_profissional(resultSet.getString("nr_crm_profissional"));
				internacao.setNm_carater_internacao(resultSet.getString("nm_carater_internacao"));
				internacao.setNm_origem_internacao(resultSet.getString("nm_origem_internacao"));
				internacao.setNr_procedimento(resultSet.getString("nr_procedimento"));

				String dt_alta_medica = resultSet.getString("dt_alta_medica");

				if (dt_alta_medica == null) {
					internacao.setDt_alta_medica(dt_alta_medica);
				} else {
					dt_alta_medica = dateFormat.format(resultSet.getTimestamp("dt_alta_medica"));
					internacao.setDt_alta_medica(dt_alta_medica);
				}

				String dt_saida_paciente = resultSet.getString("dt_saida_paciente");
				if (dt_saida_paciente == null) {
					internacao.setDt_saida_paciente(dt_saida_paciente);
				} else {
					dt_saida_paciente = dateFormat.format(resultSet.getTimestamp("dt_saida_paciente"));
					internacao.setDt_saida_paciente(dt_saida_paciente);
				}
				internacao.setDc_tipo_alta_medica(resultSet.getString("dc_tipo_alta_medica"));
				
				internacao.setNm_vinculo(resultSet.getString("nm_vinculo"));
				internacao.setNm_orgao(resultSet.getString("nm_orgao"));
									
				internacoes.add(internacao);

			}
		} catch (SQLException e) {
			System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage() );
		} catch (Exception e) {
			e.printStackTrace();
		}

		return internacoes;
	}

	public static  List<Extrato> ExtratoInternacaoPaciente(Long _prontuario) {
		List<Extrato> extratos = new ArrayList<Extrato>();
		PreparedStatement preparedStatement;
		
		String sqlString = "SELECT * FROM agh.v_extrato_internacao WHERE cd_prontuario = " + _prontuario ;
		
		try {

			Connection conn = new Conexao().getConnection();
			preparedStatement = conn.prepareStatement(sqlString);

			ResultSet resultSet = preparedStatement.executeQuery();
			
				while (resultSet.next()) {
					Extrato extrato = new Extrato();

					extrato.setCd_prontuario(resultSet.getLong("cd_prontuario"));
					extrato.setNm_paciente(resultSet.getString("nm_paciente"));
					
					DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm");
					String dt_lancamento = dateFormat.format(resultSet.getTimestamp("dt_lancamento"));
					extrato.setDt_lancamento(dt_lancamento);
					extrato.setNm_movimento(resultSet.getString("nm_movimento"));
					extrato.setNm_especialidade(resultSet.getString("nm_especialidade"));
					extrato.setNr_leito(resultSet.getString("nr_leito"));
					extrato.setNr_quarto(resultSet.getString("nr_quarto"));
					extrato.setNm_unidade_funcional(resultSet.getString("nm_unidade_funcional"));
					
					extratos.add(extrato);
				}
		}catch (SQLException e) {
			System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return extratos;
	}
	
	public static List<Internacao> GetPaciente(Long prontuarioId)
	{
		List<Internacao> internacoes = new ArrayList<Internacao>();
		PreparedStatement preparedStatement;

		
		String sqlString = "SELECT I.nr_seq, I.cd_prontuario, I.nm_paciente, I.in_sexo, I.nr_idade, I.nr_quarto, I.nr_leito, nm_ala, nm_clinica, "+
				"nm_unidade_funcional, nm_acomodacao, st_leito, dt_internacao, dt_entrada_setor, nm_especialidade, " +
				"nm_medico, dt_ultimo_evento, nm_origem, sg_cid, tx_observacao, nr_convenio, nr_plano, nm_convenio_plano, "+
				"nr_crm_profissional, nm_carater_internacao, nm_origem_internacao, nr_procedimento, dt_alta_medica, dt_saida_paciente, "+
				"dc_tipo_alta_medica, p.nm_vinculo , p.nm_orgao "+
				                "FROM agh.v_internacao as I inner join agh.v_paciente as P on I.cd_prontuario = P.cd_prontuario ";
		
			sqlString +=  "WHERE I.cd_prontuario = "+ prontuarioId ;
			sqlString += " AND dt_alta_medica is not null";
			sqlString += " ORDER BY dt_internacao";
	
		try {

			Connection conn = new Conexao().getConnection();
			preparedStatement = conn.prepareStatement(sqlString);

			ResultSet resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {

				Internacao internacao = new Internacao();
				
				internacao.setNr_seq(resultSet.getLong("nr_seq"));
				internacao.setCd_prontuario(resultSet.getLong("cd_prontuario"));
				internacao.setNm_paciente(resultSet.getString("nm_paciente"));
				internacao.setIn_sexo(resultSet.getString("in_sexo"));
				internacao.setNr_idade(resultSet.getInt("nr_idade"));
				internacao.setNr_quarto(resultSet.getString("nr_quarto"));
				internacao.setNr_leito(resultSet.getString("nr_leito"));
				internacao.setNm_ala(resultSet.getString("nm_ala"));
				internacao.setNm_clinica(resultSet.getString("nm_clinica"));
				internacao.setNm_unidade_funcional(resultSet.getString("nm_unidade_funcional"));
				internacao.setNm_acomodacao(resultSet.getString("nm_acomodacao"));
				internacao.setSt_leito(resultSet.getString("st_leito"));

				DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm");
				String dt_internacao = dateFormat.format(resultSet.getTimestamp("dt_internacao"));

				internacao.setDt_internacao(dt_internacao);

				String dt_entrada_setor = dateFormat.format(resultSet.getTimestamp("dt_entrada_setor"));
				internacao.setDt_entrada_setor(dt_entrada_setor);
				internacao.setNm_especialidade(resultSet.getString("nm_especialidade"));
				internacao.setNm_medico(resultSet.getString("nm_medico"));

				String dt_ultimo_evento = dateFormat.format(resultSet.getTimestamp("dt_ultimo_evento"));
				internacao.setDt_ultimo_evento(dt_ultimo_evento);
				internacao.setNm_origem(resultSet.getString("nm_origem"));
				internacao.setSg_cid(resultSet.getString("sg_cid"));
				internacao.setTx_observacao(resultSet.getString("tx_observacao"));
				internacao.setNr_convenio(resultSet.getInt("nr_convenio"));
				internacao.setNr_plano(resultSet.getInt("nr_plano"));
				internacao.setNm_convenio_plano(resultSet.getString("nm_convenio_plano"));
				internacao.setNr_crm_profissional(resultSet.getString("nr_crm_profissional"));
				internacao.setNm_carater_internacao(resultSet.getString("nm_carater_internacao"));
				internacao.setNm_origem_internacao(resultSet.getString("nm_origem_internacao"));
				internacao.setNr_procedimento(resultSet.getString("nr_procedimento"));

				String dt_alta_medica = resultSet.getString("dt_alta_medica");

				if (dt_alta_medica == null) {
					internacao.setDt_alta_medica(dt_alta_medica);
				} else {
					dt_alta_medica = dateFormat.format(resultSet.getTimestamp("dt_alta_medica"));
					internacao.setDt_alta_medica(dt_alta_medica);
				}

				String dt_saida_paciente = resultSet.getString("dt_saida_paciente");
				if (dt_saida_paciente == null) {
					internacao.setDt_saida_paciente(dt_saida_paciente);
				} else {
					dt_saida_paciente = dateFormat.format(resultSet.getTimestamp("dt_saida_paciente"));
					internacao.setDt_saida_paciente(dt_saida_paciente);
				}
				internacao.setDc_tipo_alta_medica(resultSet.getString("dc_tipo_alta_medica"));
				
				internacao.setNm_vinculo(resultSet.getString("nm_vinculo"));
				internacao.setNm_orgao(resultSet.getString("nm_orgao"));
									
				internacoes.add(internacao);

			}
		} catch (SQLException e) {
			System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage() );
		} catch (Exception e) {
			e.printStackTrace();
		}

		return internacoes;
	
	}
}
