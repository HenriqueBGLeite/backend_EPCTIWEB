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
    public class Rotina9950Controller : Controller
    {
        [Route("{matricula}")]
        public JsonResult BuscaFiliais(int matricula)
        {
            return Json(new Menu().BuscaFiliais(matricula));
        }

        public JsonResult BuscaRotas()
        {
            return Json(new Rotina9950().BuscaRotas());
        }

        [Route("{filial}")]
        public JsonResult BoxFilial(int filial)
        {
            return Json(new Rotina9950().BoxFilial(filial));
        }

        [HttpPost]
        public JsonResult BuscaDadosCargaPedidos([FromBody] Rotina9950 parametros)
        {
            try
            {
                return Json(new Rotina9950().BuscaDadosCargaPedidos(parametros));
            }
            catch (Exception ex)
            {
                HttpContext.Response.StatusCode = (int) HttpStatusCode.InternalServerError;
                return Json(ex.Message);
            }
        }

        [HttpPost]
        public JsonResult GerarOsWms([FromBody] List<Rotina9950Gerar> dados)
        {
            try
            {
                return Json(new Rotina9950Gerar().GerarOs(dados));
            } 
            catch (Exception ex)
            {
                return Json(ex.Message);
            }
        }

        [HttpPost]
        public JsonResult GerarVolumesWMS([FromBody] Rotina9950Impressao dados)
        {
            try
            {
                return Json(new Rotina9950Impressao().GerarVolumesWMS(dados));
            }
            catch (Exception ex)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                return Json(ex.Message);
            }
        }

        [HttpPost]
        public JsonResult EstornarWms([FromBody] List<Rotina9950Estornar> dados)
        {
            try
            {
                return Json(new Rotina9950Estornar().EstornarOs(dados));
            }
            catch (Exception ex)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                return Json(ex.Message);
            }
        }

        [HttpPost]
        public JsonResult BuscaEstorno([FromBody] ParamEstorno dados)
        {
            return Json(new Rotina9950Estornar().BuscaEstorno(dados));
        }

        [HttpPost]
        public JsonResult Corte([FromBody] Rotina9950Corte dados)
        {
            try
            {
                return Json(new Rotina9950Corte().Corte(dados));
            }
            catch (Exception ex)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                return Json(ex.Message);
            }
        }

        [Route("{valorBusca}/{tipoBusca}")]
        public JsonResult BuscaCortes(int valorBusca, string tipoBusca)
        {
            return Json(new Rotina9950Corte().BuscaCortes(valorBusca, tipoBusca));
        }

        [Route("{NumCar}")]
        public Boolean FinalizaOsCarregamento(int numcar)
        {
            return new Rotina9950().LiberarCarregamento(numcar);
        }
    }
}
