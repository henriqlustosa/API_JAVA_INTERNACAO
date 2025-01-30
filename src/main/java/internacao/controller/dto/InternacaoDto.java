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
		
		String sqlString = "";
		
		if(tipo==1) {
			
			sqlString = "SELECT \r\n"
					+ "       sub.nr_seq,\r\n"
					+ "       sub.cd_prontuario,\r\n"
					+ "       sub.nm_paciente,\r\n"
					+ "       sub.in_sexo,\r\n"
					+ "       sub.nr_idade,\r\n"
					+ "       sub.nr_quarto,\r\n"
					+ "       sub.nr_leito,\r\n"
					+ "       sub.nm_ala,\r\n"
					+ "       sub.nm_clinica,\r\n"
					+ "       sub.nm_unidade_funcional,\r\n"
					+ "       sub.nm_acomodacao,\r\n"
					+ "       sub.st_leito,\r\n"
					+ "       sub.dt_internacao,\r\n"
					+ "       sub.dt_entrada_setor,\r\n"
					+ "       sub.nm_especialidade,\r\n"
					+ "       sub.nm_medico,\r\n"
					+ "       sub.dt_ultimo_evento,\r\n"
					+ "       sub.nm_origem,\r\n"
					+ "       sub.sg_cid,\r\n"
					+ "       sub.tx_observacao,\r\n"
					+ "       sub.nr_convenio,\r\n"
					+ "       sub.nr_plano,\r\n"
					+ "       sub.nm_convenio_plano,\r\n"
					+ "       sub.nr_crm_profissional,\r\n"
					+ "       sub.nm_carater_internacao,\r\n"
					+ "       sub.nm_origem_internacao,\r\n"
					+ "       sub.nr_procedimento,\r\n"
					+ "       sub.dt_alta_medica,\r\n"
					+ "       sub.dt_saida_paciente,\r\n"
					+ "       sub.dc_tipo_alta_medica,\r\n"
					+ "       sub.nm_vinculo,\r\n"
					+ "       sub.nm_orgao,\r\n"
					+ "       sub.dt_movimentacao,\r\n"
					+ "       sub.dthr_alta_medica,\r\n"
					+ "       sub.informado_por\r\n"
					+ "FROM (\r\n"
					+ "    SELECT \r\n"
					+ "           I.nr_seq,\r\n"
					+ "           I.cd_prontuario,\r\n"
					+ "           I.nm_paciente,\r\n"
					+ "           I.in_sexo,\r\n"
					+ "           I.nr_idade,\r\n"
					+ "           I.nr_quarto,\r\n"
					+ "           I.nr_leito,\r\n"
					+ "           I.nm_ala,\r\n"
					+ "           I.nm_clinica,\r\n"
					+ "           I.nm_unidade_funcional,\r\n"
					+ "           I.nm_acomodacao,\r\n"
					+ "           I.st_leito,\r\n"
					+ "           I.dt_internacao,\r\n"
					+ "           I.dt_entrada_setor,\r\n"
					+ "           I.nm_especialidade,\r\n"
					+ "           I.nm_medico,\r\n"
					+ "           I.dt_ultimo_evento,\r\n"
					+ "           I.nm_origem,\r\n"
					+ "           I.sg_cid,\r\n"
					+ "           I.tx_observacao,\r\n"
					+ "           I.nr_convenio,\r\n"
					+ "           I.nr_plano,\r\n"
					+ "           I.nm_convenio_plano,\r\n"
					+ "           I.nr_crm_profissional,\r\n"
					+ "           I.nm_carater_internacao,\r\n"
					+ "           I.nm_origem_internacao,\r\n"
					+ "           I.nr_procedimento,\r\n"
					+ "           I.dt_alta_medica,\r\n"
					+ "           I.dt_saida_paciente,\r\n"
					+ "           I.dc_tipo_alta_medica,\r\n"
					+ "           P.nm_vinculo,\r\n"
					+ "           P.nm_orgao,\r\n"
					+ "           T.dt_movimentacao,\r\n"
					+ "           T.dthr_alta_medica,\r\n"
					+ " 		  T.informado_por,\r\n"
					+ "           -- This window function ranks rows by dt_movimentacao (descending).\r\n"
					+ "           ROW_NUMBER() OVER (PARTITION BY I.nr_seq ORDER BY T.dt_movimentacao DESC) AS rn\r\n"
					+ "    FROM agh.v_internacao      AS I\r\n"
					+ "    JOIN agh.v_paciente        AS P ON I.cd_prontuario = P.cd_prontuario\r\n"
					+ "    JOIN agh.v_movimentacao_internacao          AS T ON T.int_seq = I.nr_seq\r\n"
					+ "    WHERE  I.dt_saida_paciente::date BETWEEN '"+ dt_inicio +"' AND '"+dt_fim+"'\r\n"
					+ ") AS sub\r\n"
					+ "WHERE sub.rn = 1  -- Keep only the row with the largest T.dt_movimentacao\r\n"
					+ "ORDER BY sub.dt_saida_paciente;\r\n"
					+ "";
		}else if (tipo==2) {
			sqlString =  "SELECT \r\n"
					+ "       sub.nr_seq,\r\n"
					+ "       sub.cd_prontuario,\r\n"
					+ "       sub.nm_paciente,\r\n"
					+ "       sub.in_sexo,\r\n"
					+ "       sub.nr_idade,\r\n"
					+ "       sub.nr_quarto,\r\n"
					+ "       sub.nr_leito,\r\n"
					+ "       sub.nm_ala,\r\n"
					+ "       sub.nm_clinica,\r\n"
					+ "       sub.nm_unidade_funcional,\r\n"
					+ "       sub.nm_acomodacao,\r\n"
					+ "       sub.st_leito,\r\n"
					+ "       sub.dt_internacao,\r\n"
					+ "       sub.dt_entrada_setor,\r\n"
					+ "       sub.nm_especialidade,\r\n"
					+ "       sub.nm_medico,\r\n"
					+ "       sub.dt_ultimo_evento,\r\n"
					+ "       sub.nm_origem,\r\n"
					+ "       sub.sg_cid,\r\n"
					+ "       sub.tx_observacao,\r\n"
					+ "       sub.nr_convenio,\r\n"
					+ "       sub.nr_plano,\r\n"
					+ "       sub.nm_convenio_plano,\r\n"
					+ "       sub.nr_crm_profissional,\r\n"
					+ "       sub.nm_carater_internacao,\r\n"
					+ "       sub.nm_origem_internacao,\r\n"
					+ "       sub.nr_procedimento,\r\n"
					+ "       sub.dt_alta_medica,\r\n"
					+ "       sub.dt_saida_paciente,\r\n"
					+ "       sub.dc_tipo_alta_medica,\r\n"
					+ "       sub.nm_vinculo,\r\n"
					+ "       sub.nm_orgao,\r\n"
					+ "       sub.dt_movimentacao,\r\n"
					+ "       sub.dthr_alta_medica,\r\n"
					+ "       sub.informado_por\r\n"
					+ "FROM (\r\n"
					+ "    SELECT \r\n"
					+ "           I.nr_seq,\r\n"
					+ "           I.cd_prontuario,\r\n"
					+ "           I.nm_paciente,\r\n"
					+ "           I.in_sexo,\r\n"
					+ "           I.nr_idade,\r\n"
					+ "           I.nr_quarto,\r\n"
					+ "           I.nr_leito,\r\n"
					+ "           I.nm_ala,\r\n"
					+ "           I.nm_clinica,\r\n"
					+ "           I.nm_unidade_funcional,\r\n"
					+ "           I.nm_acomodacao,\r\n"
					+ "           I.st_leito,\r\n"
					+ "           I.dt_internacao,\r\n"
					+ "           I.dt_entrada_setor,\r\n"
					+ "           I.nm_especialidade,\r\n"
					+ "           I.nm_medico,\r\n"
					+ "           I.dt_ultimo_evento,\r\n"
					+ "           I.nm_origem,\r\n"
					+ "           I.sg_cid,\r\n"
					+ "           I.tx_observacao,\r\n"
					+ "           I.nr_convenio,\r\n"
					+ "           I.nr_plano,\r\n"
					+ "           I.nm_convenio_plano,\r\n"
					+ "           I.nr_crm_profissional,\r\n"
					+ "           I.nm_carater_internacao,\r\n"
					+ "           I.nm_origem_internacao,\r\n"
					+ "           I.nr_procedimento,\r\n"
					+ "           I.dt_alta_medica,\r\n"
					+ "           I.dt_saida_paciente,\r\n"
					+ "           I.dc_tipo_alta_medica,\r\n"
					+ "           P.nm_vinculo,\r\n"
					+ "           P.nm_orgao,\r\n"
					+ "           T.dt_movimentacao,\r\n"
					+ "           T.dthr_alta_medica,\r\n"
					+ "           T.informado_por,\r\n"
					+ "           -- This window function ranks rows by dt_movimentacao (descending).\r\n"
					+ "           ROW_NUMBER() OVER (PARTITION BY I.nr_seq ORDER BY T.dt_movimentacao DESC) AS rn\r\n"
					+ "    FROM agh.v_internacao      AS I\r\n"
					+ "    JOIN agh.v_paciente        AS P ON I.cd_prontuario = P.cd_prontuario\r\n"
					+ "    JOIN agh.v_movimentacao_internacao          AS T ON T.int_seq = I.nr_seq\r\n"
					+ "    WHERE  I.dt_internacao::date BETWEEN '"+ dt_inicio +"' AND '"+dt_fim + "'\r\n"
					+ ") AS sub\r\n"
					+ "WHERE sub.rn = 1  -- Keep only the row with the largest T.dt_movimentacao\r\n"
					+ "ORDER BY sub.dt_internacao;\r\n"
					+ "";
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
				
				String dt_movimentacao = resultSet.getString("dt_movimentacao");
				if (dt_movimentacao == null) {
					internacao.setDt_movimentacao(dt_movimentacao);
				} else {
					dt_movimentacao = dateFormat.format(resultSet.getTimestamp("dt_movimentacao"));
					internacao.setDt_movimentacao(dt_movimentacao);
				}
				String dthr_alta_medica =resultSet.getString("dthr_alta_medica");
				if (dthr_alta_medica == null) {
					internacao.setDthr_alta_medica(dthr_alta_medica);
				} else {
					dthr_alta_medica = dateFormat.format(resultSet.getTimestamp("dthr_alta_medica"));
					internacao.setDthr_alta_medica(dthr_alta_medica);
				}
				internacao.setInformado_por(resultSet.getString("informado_por"));				
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
