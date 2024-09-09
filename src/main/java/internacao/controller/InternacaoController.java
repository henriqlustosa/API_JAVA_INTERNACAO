package  internacao.controller;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import internacao.controller.dto.InternacaoDto;
import internacao.model.Internacao;

@RestController
@RequestMapping("/hspmsgh-api/internacoes")
public class InternacaoController {

	@CrossOrigin(origins="*")
	@GetMapping()
	public ResponseEntity<List<Internacao>> getInternacoesAlta(int tipo ,String dt_inicio, String dt_fim){
			// tipo = 1 - consulta por data da alta
			// tipo = 2 - consulta por data da internacao
				
			List<Internacao> internacoes = InternacaoDto.Internacoes(tipo, dt_inicio, dt_fim);
			
			if(internacoes == null) {
				return ResponseEntity.notFound().build();
			}
			return ResponseEntity.ok(internacoes);
	}
	
	@GetMapping("/paciente/{prontuarioId}")
	public ResponseEntity<List<Internacao>> getInternacoesPaciente(@PathVariable Long prontuarioId){
		
		List<Internacao> internacoes = InternacaoDto.GetPaciente(prontuarioId);
		
		
		if(internacoes == null) {
			return ResponseEntity.notFound().build();
		}
		return ResponseEntity.ok(internacoes);
	}
		
}