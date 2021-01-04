using System;
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
    public class MenuController : Controller
    {
        [Route("{matricula}")]
        public JsonResult BuscarRotinas(int matricula)
        {
            return Json(new Menu().BuscarRotinas(matricula));
        }
    }
}
