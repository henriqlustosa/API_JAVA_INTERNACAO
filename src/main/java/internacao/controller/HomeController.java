package internacao.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class HomeController {
	@RequestMapping(value = "/hspmsgh-api")
	public String index() {
        return "index.html";
    }

}
