using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace Twigaten.Web.Pages
{
    public class ErrorModel : PageModel
    {
        public void OnGet()
        {
            //FallbackToPageで飛んできたらちゃんと404にする
            if(HttpContext.Response.StatusCode == StatusCodes.Status200OK) { HttpContext.Response.StatusCode = StatusCodes.Status404NotFound; }
        }
    }
}