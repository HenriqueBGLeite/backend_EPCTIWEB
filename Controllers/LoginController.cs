using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using EPCTIWebApi.Model;
using System.Web.Http.Cors;
using Microsoft.AspNetCore.Authorization;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using Microsoft.IdentityModel.Tokens;
using System.Text;
using Microsoft.Extensions.Configuration;

namespace EPCTIWebApi.Controllers
{
    [AllowAnonymous]
    [Route("api/[Controller]/[Action]/")]
    public class LoginController : Controller
    {
        private readonly IConfiguration _configuration;

        public LoginController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [HttpPost]
        public Usuario ValidaUsuario([FromBody] Usuario usuario)
        {
            var usuarioAutenticado = new Usuario();

            usuarioAutenticado = usuarioAutenticado.validaUsuario(usuario);

            if (usuarioAutenticado.Erro == "N" && usuarioAutenticado.Warning == "N")
            {
                //GERAÇÃO DO TOKEN
                var claims = new[]
                {
                    //Declarar dados que precisa no token
                    new Claim ("filial", usuarioAutenticado.Filial.ToString()), //Verificar como faz quando o valor for INT
                    new Claim ("codigo", usuarioAutenticado.Codigo.ToString()), //Verificar como faz quando o valor for INT
                    new Claim ("nome", usuarioAutenticado.Nome),
                    new Claim ("acessoSistema", usuarioAutenticado.AcessoSistema),
                    new Claim ("base", usuarioAutenticado.Base)
                };

                //Recebe uma instancia da classe SymmetricSecurityKey
                //Armazenando a chave de criptografia usada na criação do token
                var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_configuration["SecurityKey"]));

                //Recebe um objeto de tipo SigninCredentials contendo a chave de criptografia e o algoritimo de segurança empregados na geração de assinaturas digitais para tokens
                var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

                //Gera o Token com os dados previamente declarados
                var token = new JwtSecurityToken(
                    issuer: "EPOCA",
                    audience: "EPOCA",
                    claims: claims,
                    expires: DateTime.Now.AddHours(12),
                    signingCredentials: creds
                    );

                usuarioAutenticado.Token = new JwtSecurityTokenHandler().WriteToken(token);

                return usuarioAutenticado;
            }
            else
            {
                return usuarioAutenticado;
            }
        }
    }
}