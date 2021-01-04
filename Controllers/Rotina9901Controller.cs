using System;
using System.Net;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using EPCTIWebApi.Model;

namespace EPCTIWebApi.Controllers
{
    [Route("api/[Controller]/[Action]/")]
    [Authorize()]
    public class Rotina9901Controller : Controller
    {
        [Route("{matricula}")]
        public JsonResult BuscaFiliais(int matricula)
        {
            return Json(new Menu().BuscaFiliais(matricula));
        }

        public JsonResult BuscaFornecedores()
        {
            return Json(new Rotina9901().BuscaFornecedores());
        }

        [HttpPost]
        public JsonResult BuscaProdutos([FromBody] Rotina9901 parametros)
        {
            try
            {
                return Json(new Rotina9901().BuscaProdutos(parametros));
            }
            catch (Exception ex)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                return Json(ex.Message);
            }
        }

        [Route("{CodFilial}/{Produto}")]
        public JsonResult BuscaDadosProduto(int CodFilial, Int64 Produto)
        {
            try
            {
                return Json(new Rotina9901Edita().BuscaDadosProduto(CodFilial, Produto));
            }
            catch (Exception ex)
            {
                return Json(ex.Message);
            }
        }

        public JsonResult BuscaListaDataProduto()
        {
            try
            {
                return Json(new ListaDataProduto().BuscaListaDataProduto());
            }
            catch (Exception ex)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;

                return Json(ex.Message);
            }
        }
        
        [Route("{codFilial}/{tipoEndereco}/{tipoEstrutura}")]
        public JsonResult ListaEnderecos(int codFilial, int tipoEndereco, int tipoEstrutura)
        {
            try
            {
                return Json(new Rotina9901Edita().ListaEnderecos(codFilial, tipoEndereco, tipoEstrutura));
            }
            catch (Exception ex)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                return Json(ex.Message);
            }
        }

        [Route("{codFilial}")]
        public JsonResult ListaEnderecosLoja(int codFilial)
        {
            try
            {
                return Json(new Rotina9901Edita().ListaEnderecosLoja(codFilial));
            }
            catch (Exception ex)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                return Json(ex.Message);
            }
        }

        [HttpPost]
        public string GravarAlteracoesCadastro([FromBody] Rotina9901Edita dados)
        {
            try
            {
                Boolean gravou = new Rotina9901Edita().GravarAlteracoesCadastro(dados);

                if (gravou)
                {
                    return "Registro alterado com sucesso.";
                }
                else
                {
                    return "Ocorreu um erro ao salvar o registro. Tente novamente mais tarde.";
                }
            }
            catch (Exception ex)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                return ex.Message;
            }
        }

    }
}