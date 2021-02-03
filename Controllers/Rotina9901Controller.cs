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
        
        [HttpPost]
        public JsonResult ListaEnderecos([FromBody] DTOPesquisaEndereco dados)
        {
            try
            {
                return Json(new Rotina9901Edita().ListaEnderecos(dados));
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

        [Route("{codFilial}")]
        public JsonResult ListaEnderecosLivres(int codFilial)
        {
            try
            {
                return Json(new Rotina9901Edita().ListaEnderecosLivres(codFilial));
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
                return new Rotina9901Edita().GravarAlteracoesCadastro(dados);
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
        }

    }
}