using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace Twigaten.Web
{
    public class AuthFirstModel : PageModel
    {
        public async Task<ActionResult> OnGetAsync()
        {
            var ParamTask = new Parameters.LoginParameters().InitValidate(HttpContext);
            if (HttpContext.Session.TryGetValue(nameof(Controllers.AuthController.FirstProcess), out var Bytes)
                && 1 <= Bytes.Length && Bytes[0] == 0)
            {
                await ParamTask.ConfigureAwait(false);
                return Page();
            }
            else 
            {
                await ParamTask.ConfigureAwait(false);
                return LocalRedirect("/"); 
            }
        }
    }
}