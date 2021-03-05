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
    public class Rotina9909Controller : Controller
    {
        [Route("{matricula}")]
        public JsonResult BuscaFiliais(int matricula)
        {
            return Json(new Menu().BuscaFiliais(matricula));
        }

        [Route("{CodFilial}/{Produto}")]
        public Rotina9909 BuscaDadosProdutoTransferencia(int codFilial, int produto)
        {
            return new Rotina9909().BuscaDadosProdutoTransferencia(codFilial, produto);
        }

        [Route("{Enderecos}/{CodProd}")]
        public List<int> ValidaEnderecos(string enderecos, string codprod)
        {
            return new Rotina9909().ValidaEnderecos(enderecos, codprod);
        }

        [HttpPost]
        public RetornoTransf TransfereEnderecos([FromBody] List<TransferenciaEnderecos> lista)
        {
            return new Rotina9909().TransfereEnderecos(lista);
        }
    }
}
