using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Twigaten.DctHashServer.Controllers
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

        /// <summary>
        /// GCをやらせるひどいAPI
        /// </summary>
        /// <returns></returns>
        [HttpHead("dct")]
        public IActionResult GCNow()
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Default, true, false);
            GC.WaitForPendingFinalizers();
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce; //これは毎回必要
            GC.Collect();
            return Ok();
        }

        [HttpGet("index")]
        public IActionResult Index()
        {
            return Content("ぬるぽ", "text/plain");
        }
    }
}