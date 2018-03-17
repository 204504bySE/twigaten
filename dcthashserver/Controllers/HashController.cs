using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace aspcoretest.Controllers
{
    [Route("hash")]
    public class HashController : Controller
    {
        [HttpPost("dct")]
        public IActionResult DCT(IFormFile File)
        {
            return Content(twidown.PictHash.DCTHash(File.OpenReadStream()).ToString(), "text/plain");
        }

        [HttpPost("dctcrop")]
        public IActionResult DCTCrop(IFormFile File)
        {
            return Content(twidown.PictHash.DCTHash(File.OpenReadStream(), true).ToString(), "text/plain");
        }


        [HttpGet("index")]
        public IActionResult Index()
        {
            return Content("ぬるぽ", "text/plain");
        }
    }
}