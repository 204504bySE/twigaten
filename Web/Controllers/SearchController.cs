using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Twigaten.Web.Controllers
{
    [Route("/search")]
    [ApiController]
    public class SearchController : ControllerBase
    {
        [HttpPost]
        public async Task<ActionResult> Media(IFormFile File)
        {

        }
    }

}