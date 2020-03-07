using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace Twigaten.Web.Pages
{
    public class AboutModel : PageModel
    {
        public async Task OnGetAsync()
        {
            await new Parameters.LoginParameters().InitValidate(HttpContext).ConfigureAwait(false);
        }
    }
}